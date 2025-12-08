import json
import time
import math
import random
import os
import numpy as np
import glob
from mininet.log import info, error

def load_and_merge_traces(trace_files):
    '''
    Loads multiple JSON trace files.
    Namespaces all logical IDs to ensure uniqueness across files.
    Example: '0.0' in trace file 1 becomes 'T1_0.0'.
    '''
    merged_events = []
    
    # We will flatten the config into a simple list of unique logical processes
    # format: [ ('T0_0.0', resource_cost), ('T0_0.1', resource_cost), ... ]
    all_logical_processes = []

    for trace_idx, filepath in enumerate(trace_files):
        try:
            with open(filepath, 'r') as f:
                data = json.load(f)
        except Exception as e:
            error(f'Failed to load {filepath}: {e}\n')
            continue
            
        # 1. Extract Config (The first element)
        # Structure: {'0': [['0.0', 1], ...], '1': [...]}
        process_config = None
        events = []
        
        if len(data) > 0 and isinstance(data[0], dict) and '0' in data[0]:
             process_config = data[0]
             events = data[1:]
        elif 'sender' in data[0]:
            # No config map found, assuming just events. 
            # We must infer processes from events later or error out.
            # For this specific format, we expect the config map.
            error(f'Error: {filepath} missing config map at index 0.\n')
            continue

        # 2. Namespace the Config and collect processes
        # trace_prefix = f'T{trace_idx}_' 
        trace_prefix = f'{trace_idx}-' # Using dash separator
        
        # Sort groups to ensure deterministic loading order
        for group_id in sorted(process_config.keys(), key=lambda x: int(x)):
            group_list = process_config[group_id]
            for proc_entry in group_list:
                # proc_entry: ['0.0', 1]
                original_name = proc_entry[0]
                cost = proc_entry[1]
                
                namespaced_name = trace_prefix + str(original_name)
                all_logical_processes.append(namespaced_name)

        # 3. Namespace the Events
        for event in events:
            # Modify the event in place (or copy if safer)
            if 'sender' in event:
                event['sender'] = trace_prefix + str(event['sender'])
            
            if 'receiver' in event:
                new_receivers = []
                for r in event['receiver']:
                    new_receivers.append(trace_prefix + str(r))
                event['receiver'] = new_receivers
                
            merged_events.append(event)

    # 4. Sort all events globally by time
    merged_events.sort(key=lambda x: x.get('time', 0.0))
    
    info(f'*** Loaded {len(all_logical_processes)} unique processes from {len(trace_files)} traces. ***\n')
    return all_logical_processes, merged_events

def map_processes_to_hosts(net, all_logical_processes, percent_usage, procs_per_host):
    '''
    Maps namespaced logical processes (e.g., 'T0-1.0') to Physical Mininet Nodes.
    
    Uses STRIDED assignment to ensure processes from the same group are spread
    across different physical hosts, enabling network traffic between them.
    
    For example, with 8 hosts and processes ['0-1.0', '0-1.1', ..., '0-1.7']:
    - Old (consecutive): all 8 map to host 0 -> all intra-group traffic skipped
    - New (strided): 1.0->h0, 1.1->h1, ..., 1.7->h7 -> traffic flows across network
    '''
    # 1. Determine Physical Resources
    all_hosts = net.hosts
    total_physical = len(all_hosts)
    
    # Calculate how many physical hosts we are ALLOWED to use
    active_count = int(math.ceil(total_physical * percent_usage))
    if active_count == 0: active_count = 1
    
    physical_pool = all_hosts[:active_count]
    info(f'*** Resource Allocation: Using {active_count}/{total_physical} physical hosts. ***\n')

    # 2. Group processes by their logical group (e.g., '0-1.0' -> group '0-1')
    #    Processes in the same group communicate frequently, so we spread them out
    from collections import defaultdict
    groups = defaultdict(list)
    for proc_name in all_logical_processes:
        # Extract group: '0-1.0' -> '0-1', '2-3.5' -> '2-3'
        parts = proc_name.rsplit('.', 1)
        group_id = parts[0] if len(parts) > 1 else proc_name
        groups[group_id].append(proc_name)
    
    # 3. Perform STRIDED Mapping
    #    Assign each process in a group to a different host (round-robin across hosts)
    mapping = {}
    host_usage = [0] * len(physical_pool)  # Track how many procs per host
    
    for group_id in sorted(groups.keys()):
        group_procs = groups[group_id]
        for i, proc_name in enumerate(group_procs):
            # Stride across physical hosts for each process in the group
            phys_idx = i % len(physical_pool)
            assigned_host = physical_pool[phys_idx]
            mapping[proc_name] = assigned_host
            host_usage[phys_idx] += 1
    
    # Log distribution stats
    max_usage = max(host_usage) if host_usage else 0
    min_usage = min(host_usage) if host_usage else 0
    info(f'*** Process Distribution: {len(groups)} groups, {min_usage}-{max_usage} procs/host ***\n')

    return mapping

def run_multi_trace_experiment(net, trace_file_paths, percentage=1.0, procs_per_host=8, 
                                num_server_ports=32, time_scale=1.0, max_events=30000,
                                congestion_control='cubic'):
    '''
    Run a multi-trace experiment on the network.
    
    Args:
        net: Mininet network object
        trace_file_paths: List of trace JSON files to replay
        percentage: Fraction of physical hosts to use (0.0 to 1.0)
        procs_per_host: Max logical processes per physical host
        num_server_ports: Number of iperf3 server ports per host (for concurrency)
        time_scale: Timing scale factor (0.0 = no delays/fastest, 1.0 = real-time accurate replay)
        max_events: Maximum number of events to process (None = all events)
        congestion_control: TCP congestion control algorithm ('cubic', 'reno', 'dctcp', 'bbr')
    '''
    
    # 0. Setup Logging
    log_dir = '/tmp/mininet_metrics'
    os.system(f'rm -rf {log_dir}')
    os.system(f'mkdir -p {log_dir}')
    
    # 1. Load Traces
    all_logical_procs, events = load_and_merge_traces(trace_file_paths)
    
    # Limit events if specified
    if max_events is not None and max_events < len(events):
        info(f'*** Limiting to first {max_events} events (out of {len(events)}) ***\n')
        events = events[:max_events]
    
    host_map = map_processes_to_hosts(net, all_logical_procs, percentage, procs_per_host)

    # 2. Configure TCP congestion control on all hosts
    info(f'*** Setting TCP congestion control to: {congestion_control} ***\n')
    for h in net.hosts:
        # Set congestion control algorithm
        h.cmd(f'sysctl -w net.ipv4.tcp_congestion_control={congestion_control} 2>/dev/null')
        # Enable ECN if using DCTCP (required for DCTCP to work properly)
        if congestion_control == 'dctcp':
            h.cmd('sysctl -w net.ipv4.tcp_ecn=1')

    # 3. Start MULTIPLE iperf3 Servers per host (to handle concurrent connections)
    BASE_PORT = 5201
    info(f'*** Starting {num_server_ports} iperf3 Servers per host... ***\n')
    for h in net.hosts:
        for port_offset in range(num_server_ports):
            port = BASE_PORT + port_offset
            h.cmd(f'iperf3 -s -p {port} -D --logfile /dev/null')
    
    time.sleep(2)  # Wait for daemons to spin up

    # 3. Replay Loop
    info(f'*** Replaying {len(events)} events (time_scale={time_scale})... ***\n')
    start_wall_time = time.time()
    if events: first_event_time = events[0].get('time', 0.0)
    
    # Track port usage per destination host for load balancing
    host_port_counter = {}
    flows_started = 0
    flows_skipped = 0
    last_progress_time = start_wall_time

    for i, event in enumerate(events):
        # --- Timing (only if time_scale > 0) ---
        if time_scale > 0:
            target_delay = (event.get('time', 0.0) - first_event_time) * time_scale
            current_delay = time.time() - start_wall_time
            if target_delay > current_delay:
                time.sleep(target_delay - current_delay)

        # --- Setup ---
        sender_name = event.get('sender')
        receivers = event.get('receiver', [])
        size_bytes = int(event.get('size', 1024))
        if size_bytes < 1: size_bytes = 1024

        if sender_name not in host_map:
            flows_skipped += 1
            continue
        phys_sender = host_map[sender_name]

        for rx_name in receivers:
            if rx_name not in host_map:
                flows_skipped += 1
                continue
            phys_rx = host_map[rx_name]
            if phys_sender == phys_rx:
                flows_skipped += 1
                continue
            
            # --- Load-balance across server ports ---
            rx_host_name = phys_rx.name
            if rx_host_name not in host_port_counter:
                host_port_counter[rx_host_name] = 0
            port_offset = host_port_counter[rx_host_name] % num_server_ports
            port = BASE_PORT + port_offset
            host_port_counter[rx_host_name] += 1
            
            # --- Execute command ---
            log_file = f'{log_dir}/{i}_{sender_name}_to_{rx_name}.json'
            cmd = (f'iperf3 -c {phys_rx.IP()} -p {port} '
                   f'-n {size_bytes} -J '
                   f'> {log_file} 2>&1 &')
            
            phys_sender.cmd(cmd)
            flows_started += 1
        
        # Progress indicator every 1000 events OR every 5 seconds
        now = time.time()
        if (i + 1) % 1000 == 0 or (now - last_progress_time) > 5:
            elapsed = now - start_wall_time
            rate = (i + 1) / elapsed if elapsed > 0 else 0
            eta = (len(events) - i - 1) / rate if rate > 0 else 0
            info(f'*** Progress: {i+1}/{len(events)} ({100*(i+1)/len(events):.1f}%) - {rate:.0f} evt/s - ETA: {eta:.0f}s ***\n')
            last_progress_time = now

    elapsed_total = time.time() - start_wall_time
    info(f'*** Replay finished in {elapsed_total:.1f}s. Started {flows_started} flows, skipped {flows_skipped}. ***\n')
    
    # Wait time proportional to flows started (minimum 10s, max 60s)
    wait_time = min(60, max(10, flows_started // 1000))
    info(f'*** Waiting {wait_time}s for flows to complete... ***\n')
    time.sleep(wait_time)
    
    # 4. Cleanup
    for h in net.hosts:
        h.cmd('killall -9 iperf3')
    
    # 5. Analyze
    analyze_iperf_results(log_dir)

def analyze_iperf_results(log_dir):
    info('\n' + '='*40 + '\n')
    info('   IPERF3 EXPERIMENT RESULTS   \n')
    info('='*40 + '\n')
    
    fcts = []
    total_bytes = 0
    empty_files = 0
    server_busy = 0
    connection_refused = 0
    incomplete_json = 0
    json_parse_errors = 0
    iperf_errors = []
    sample_contents = []  # Store sample file contents for debugging
    
    # Robust Globbing
    log_files = glob.glob(f'{log_dir}/*.json')
    info(f'Found {len(log_files)} log files to analyze.\n')
    
    for log_f in log_files:
        try:
            with open(log_f, 'r') as f:
                content = f.read().strip()
                if not content:
                    empty_files += 1
                    continue
                
                # Try to parse JSON
                try:
                    data = json.loads(content)
                except json.JSONDecodeError:
                    json_parse_errors += 1
                    if len(sample_contents) < 3:
                        # Save first 200 chars of non-JSON content for debugging
                        sample_contents.append(content[:200])
                    continue
                
                # iperf3 JSON structure is standard:
                # data['end']['sum_sent']['seconds'] is the duration
                # data['end']['sum_sent']['bytes'] is the total bytes
                
                if 'error' in data:
                    error_msg = data['error'].lower()
                    if 'busy' in error_msg:
                        server_busy += 1
                    elif 'refused' in error_msg or 'connect' in error_msg:
                        connection_refused += 1
                    else:
                        if len(iperf_errors) < 5:
                            iperf_errors.append(data['error'])
                    continue
                
                if 'end' not in data or 'sum_sent' not in data.get('end', {}):
                    incomplete_json += 1
                    if len(sample_contents) < 3:
                        # Show what keys are present
                        sample_contents.append(f"Keys: {list(data.keys())}")
                    continue
                    
                duration = data['end']['sum_sent']['seconds']
                b_sent = data['end']['sum_sent']['bytes']
                
                fcts.append(duration)
                total_bytes += b_sent
        except Exception as e:
            if len(sample_contents) < 3:
                sample_contents.append(f"Exception: {str(e)}")
    
    # Calculate totals
    total_errors = (empty_files + server_busy + connection_refused + 
                    incomplete_json + json_parse_errors + len(iperf_errors))
    
    # Report error breakdown
    info(f'Log files analyzed: {len(log_files)}\n')
    info(f'  - Successful:        {len(fcts)}\n')
    info(f'  - Empty files:       {empty_files}\n')
    info(f'  - JSON parse errors: {json_parse_errors}\n')
    info(f'  - Incomplete JSON:   {incomplete_json}\n')
    info(f'  - Server busy:       {server_busy}\n')
    info(f'  - Connection refused: {connection_refused}\n')
    info(f'  - iperf3 errors:     {len(iperf_errors)}\n')
    
    if iperf_errors:
        info(f'  Sample iperf3 errors: {iperf_errors[:3]}\n')
    if sample_contents:
        info(f'  Sample problematic content:\n')
        for i, sample in enumerate(sample_contents[:3]):
            info(f'    [{i+1}]: {sample[:150]}...\n')
            
    if not fcts:
        info('\nNo successful flows found.\n')
        info('='*40 + '\n')
        return

    fcts = np.array(fcts)
    
    info(f'\nSuccessful Flows:  {len(fcts)}\n')
    info(f'Avg FCT:           {np.mean(fcts)*1000:.2f} ms\n')
    info(f'P50 FCT:           {np.percentile(fcts, 50)*1000:.2f} ms\n')
    info(f'P99 FCT:           {np.percentile(fcts, 99)*1000:.2f} ms\n')
    info(f'Total Vol:         {total_bytes / 1e6:.2f} MB ({total_bytes / 1e9:.2f} GB)\n')
    info('='*40 + '\n')