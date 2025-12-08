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
    '''
    # 1. Determine Physical Resources
    all_hosts = net.hosts
    total_physical = len(all_hosts)
    
    # Calculate how many physical hosts we are ALLOWED to use
    active_count = int(math.ceil(total_physical * percent_usage))
    if active_count == 0: active_count = 1
    
    physical_pool = all_hosts[:active_count]
    info(f'*** Resource Allocation: Using {active_count}/{total_physical} physical hosts. ***\n')

    # 2. Perform Mapping (Bin Packing / Round Robin)
    mapping = {}
    
    phys_idx = 0
    procs_on_current_phys = 0
    
    for proc_name in all_logical_processes:
        # Assign current logical process to current physical host
        assigned_host = physical_pool[phys_idx]
        mapping[proc_name] = assigned_host
        
        procs_on_current_phys += 1
        
        # Check if this physical host is full
        if procs_on_current_phys >= procs_per_host:
            # Move to next physical host
            phys_idx += 1
            procs_on_current_phys = 0
            
            # Wrap around if we run out of physical hosts
            if phys_idx >= len(physical_pool):
                phys_idx = 0

    return mapping

def run_multi_trace_experiment(net, trace_file_paths, percentage=1.0, procs_per_host=8):
    
    # 0. Setup Logging
    log_dir = '/tmp/mininet_metrics'
    os.system(f'rm -rf {log_dir}')
    os.system(f'mkdir -p {log_dir}')
    
    # 1. Load Traces
    all_logical_procs, events = load_and_merge_traces(trace_file_paths)
    host_map = map_processes_to_hosts(net, all_logical_procs, percentage, procs_per_host)

    # 2. Start iperf3 Servers (Daemon Mode)
    info('*** Starting iperf3 Servers... ***\n')
    for h in net.hosts:
        # -s: Server
        # -p 5201: Port (Default)
        # -D: Run as Daemon (background process automatically)
        # --logfile: Redirect server errors to /dev/null to keep screen clean
        h.cmd('iperf3 -s -p 5201 -D --logfile /dev/null')
    
    time.sleep(1) # Quick wait for daemons to spin up

    # 3. Replay Loop
    info(f'*** Replaying {len(events)} events using iperf3... ***\n')
    start_wall_time = time.time()
    if events: first_event_time = events[0].get('time', 0.0)

    for i, event in enumerate(events):
        # --- Timing ---
        target_delay = event.get('time', 0.0) - first_event_time
        current_delay = time.time() - start_wall_time
        if target_delay > current_delay:
            time.sleep(target_delay - current_delay)

        # --- Setup ---
        sender_name = event.get('sender')
        receivers = event.get('receiver', [])
        size_bytes = int(event.get('size', 1024)) # Default 1KB
        if size_bytes < 1: size_bytes = 1024

        if sender_name not in host_map: continue
        phys_sender = host_map[sender_name]

        for rx_name in receivers:
            if rx_name not in host_map: continue
            phys_rx = host_map[rx_name]
            if phys_sender == phys_rx: continue
            
            # --- The Robust Command ---
            # Unique log file per flow (CRITICAL for concurrency)
            log_file = f'{log_dir}/{i}_{sender_name}_to_{rx_name}.json'
            
            # iperf3 flags:
            # -c: Client (connect to host)
            # -n: Number of bytes to transmit (matches your trace)
            # -J: JSON output (Clean data!)
            # -p: Port
            cmd = (f'iperf3 -c {phys_rx.IP()} -p 5201 '
                   f'-n {size_bytes} -J '
                   f'> {log_file} 2>&1 &')
            
            phys_sender.cmd(cmd)

    info('*** Replay finished. Waiting 10s for stragglers... ***\n')
    time.sleep(10)
    
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
    errors = 0
    
    # Robust Globbing
    log_files = glob.glob(f'{log_dir}/*.json')
    
    for log_f in log_files:
        try:
            with open(log_f, 'r') as f:
                data = json.load(f)
                
                # iperf3 JSON structure is standard:
                # data['end']['sum_sent']['seconds'] is the duration
                # data['end']['sum_sent']['bytes'] is the total bytes
                
                if 'error' in data:
                    errors += 1
                    continue
                    
                duration = data['end']['sum_sent']['seconds']
                b_sent = data['end']['sum_sent']['bytes']
                
                fcts.append(duration)
                total_bytes += b_sent
        except Exception:
            # File might be empty if iperf crashed or was killed
            errors += 1
            
    if not fcts:
        info('No successful flows found.\n')
        return

    fcts = np.array(fcts)
    
    info(f'Total Flows:       {len(fcts)}\n')
    info(f'Errors/Empty:      {errors}\n')
    info(f'Avg FCT:           {np.mean(fcts)*1000:.2f} ms\n')
    info(f'P99 FCT:           {np.percentile(fcts, 99)*1000:.2f} ms\n')
    info(f'Total Vol:         {total_bytes / 1e6:.2f} MB\n')
    info('='*40 + '\n')