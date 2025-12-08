import json
import time
import math
import random
import os
import numpy as np
import glob
import matplotlib.pyplot as plt
from mininet.log import info, error

CC_ALG='cubic'
MAX_EVENTS=None

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
                                num_server_ports=32, time_scale=1.0, max_events=MAX_EVENTS,
                                congestion_control=CC_ALG):
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

def get_flow_type(filename):
    '''
    Determine if a flow is distributed inference (intra-group) or agent-agent (inter-group).
    Filename format: {idx}_{sender}_to_{receiver}.json
    Example: 123_0-1.0_to_0-1.5.json -> intra-group (same '0-1' prefix)
             456_0-1.0_to_0-2.3.json -> inter-group (different prefixes)
    '''
    try:
        # Extract sender and receiver from filename
        basename = os.path.basename(filename).replace('.json', '')
        parts = basename.split('_to_')
        if len(parts) != 2:
            return 'unknown'
        
        # Remove the event index prefix from sender
        sender_parts = parts[0].split('_', 1)
        sender = sender_parts[1] if len(sender_parts) > 1 else parts[0]
        receiver = parts[1]
        
        # Extract group: '0-1.0' -> '0-1'
        sender_group = sender.rsplit('.', 1)[0] if '.' in sender else sender
        receiver_group = receiver.rsplit('.', 1)[0] if '.' in receiver else receiver
        
        if sender_group == receiver_group:
            return 'distributed_inference'  # Intra-group (n.x to n.y)
        else:
            return 'agent_agent'  # Inter-group (n.x to m.y)
    except:
        return 'unknown'

def analyze_iperf_results(log_dir):
    info('\n' + '='*40 + '\n')
    info('   IPERF3 EXPERIMENT RESULTS   \n')
    info('='*40 + '\n')
    
    # Aggregate metrics
    fcts = []
    flow_sizes = []
    total_bytes = 0
    
    # Per-flow-type metrics
    dist_inf_fcts = []  # Distributed inference (intra-group)
    dist_inf_sizes = []
    dist_inf_bytes = 0
    
    agent_fcts = []  # Agent-to-agent (inter-group)
    agent_sizes = []
    agent_bytes = 0
    
    # Error tracking
    empty_files = 0
    server_busy = 0
    connection_refused = 0
    incomplete_json = 0
    json_parse_errors = 0
    iperf_errors = []
    sample_contents = []
    
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
                        sample_contents.append(content[:200])
                    continue
                
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
                        sample_contents.append(f"Keys: {list(data.keys())}")
                    continue
                    
                duration = data['end']['sum_sent']['seconds']
                b_sent = data['end']['sum_sent']['bytes']
                
                # Aggregate metrics
                fcts.append(duration)
                flow_sizes.append(b_sent)
                total_bytes += b_sent
                
                # Categorize by flow type
                flow_type = get_flow_type(log_f)
                if flow_type == 'distributed_inference':
                    dist_inf_fcts.append(duration)
                    dist_inf_sizes.append(b_sent)
                    dist_inf_bytes += b_sent
                elif flow_type == 'agent_agent':
                    agent_fcts.append(duration)
                    agent_sizes.append(b_sent)
                    agent_bytes += b_sent
                    
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
    flow_sizes = np.array(flow_sizes)
    
    # --- AGGREGATE METRICS ---
    avg_flow_size = np.mean(flow_sizes)
    avg_fct = np.mean(fcts)
    avg_throughput = (avg_flow_size / avg_fct) * 8 / 1e6  # Convert to Mbps
    
    info(f'\n--- AGGREGATE (All Flows) ---\n')
    info(f'Successful Flows:  {len(fcts)}\n')
    info(f'Avg Flow Size:     {avg_flow_size / 1024:.2f} KB\n')
    info(f'Avg FCT:           {avg_fct*1000:.2f} ms\n')
    info(f'P50 FCT:           {np.percentile(fcts, 50)*1000:.2f} ms\n')
    info(f'P99 FCT:           {np.percentile(fcts, 99)*1000:.2f} ms\n')
    info(f'Avg Throughput:    {avg_throughput:.2f} Mbps\n')
    info(f'Total Vol:         {total_bytes / 1e6:.2f} MB ({total_bytes / 1e9:.2f} GB)\n')
    
    # --- DISTRIBUTED INFERENCE METRICS (Intra-group: n.x -> n.y) ---
    if dist_inf_fcts:
        dist_inf_fcts = np.array(dist_inf_fcts)
        dist_inf_sizes = np.array(dist_inf_sizes)
        dist_avg_size = np.mean(dist_inf_sizes)
        dist_avg_fct = np.mean(dist_inf_fcts)
        dist_avg_throughput = (dist_avg_size / dist_avg_fct) * 8 / 1e6  # Convert to Mbps
        
        info(f'\n--- DISTRIBUTED INFERENCE (Intra-group: n.x -> n.y) ---\n')
        info(f'Successful Flows:  {len(dist_inf_fcts)}\n')
        info(f'Avg Flow Size:     {dist_avg_size / 1024:.2f} KB\n')
        info(f'Avg FCT:           {dist_avg_fct*1000:.2f} ms\n')
        info(f'P50 FCT:           {np.percentile(dist_inf_fcts, 50)*1000:.2f} ms\n')
        info(f'P99 FCT:           {np.percentile(dist_inf_fcts, 99)*1000:.2f} ms\n')
        info(f'Avg Throughput:    {dist_avg_throughput:.2f} Mbps\n')
        info(f'Total Vol:         {dist_inf_bytes / 1e6:.2f} MB ({dist_inf_bytes / 1e9:.2f} GB)\n')
    else:
        info(f'\n--- DISTRIBUTED INFERENCE (Intra-group) ---\n')
        info(f'No distributed inference flows found.\n')
    
    # --- AGENT-TO-AGENT METRICS (Inter-group: n.x -> m.y) ---
    if agent_fcts:
        agent_fcts = np.array(agent_fcts)
        agent_sizes = np.array(agent_sizes)
        agent_avg_size = np.mean(agent_sizes)
        agent_avg_fct = np.mean(agent_fcts)
        agent_avg_throughput = (agent_avg_size / agent_avg_fct) * 8 / 1e6  # Convert to Mbps
        
        info(f'\n--- AGENT-TO-AGENT (Inter-group: n.x -> m.y) ---\n')
        info(f'Successful Flows:  {len(agent_fcts)}\n')
        info(f'Avg Flow Size:     {agent_avg_size / 1024:.2f} KB\n')
        info(f'Avg FCT:           {agent_avg_fct*1000:.2f} ms\n')
        info(f'P50 FCT:           {np.percentile(agent_fcts, 50)*1000:.2f} ms\n')
        info(f'P99 FCT:           {np.percentile(agent_fcts, 99)*1000:.2f} ms\n')
        info(f'Avg Throughput:    {agent_avg_throughput:.2f} Mbps\n')
        info(f'Total Vol:         {agent_bytes / 1e6:.2f} MB ({agent_bytes / 1e9:.2f} GB)\n')
    else:
        info(f'\n--- AGENT-TO-AGENT (Inter-group) ---\n')
        info(f'No agent-to-agent flows found.\n')
    
    info('='*40 + '\n')
    
    # Plot distributions for all flow types
    plot_distributions(flow_sizes, fcts, label='All Flows')
    
    if len(dist_inf_fcts) > 0:
        plot_distributions(dist_inf_sizes, dist_inf_fcts, label='Distributed Inference')
    
    if len(agent_fcts) > 0:
        plot_distributions(agent_sizes, agent_fcts, label='Agent-to-Agent')


def plot_distributions(flow_sizes, fcts, output_dir=f"plots/{CC_ALG}", label='All Flows'):
    '''
    Plot two graphs:
    1. Distribution of flow sizes (data per event)
    2. Distribution of Flow Completion Times (FCT)
    
    Args:
        flow_sizes: Array of flow sizes in bytes
        fcts: Array of flow completion times in seconds
        output_dir: Directory to save plots
        label: Label for the flow type (e.g., 'All Flows', 'Distributed Inference', 'Agent-to-Agent')
    '''
    if len(flow_sizes) == 0 or len(fcts) == 0:
        info(f'*** No data to plot for {label} ***\n')
        return
    
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Create filename-safe version of label
    label_safe = label.lower().replace(' ', '_').replace('-', '_')
    
    # Convert to more readable units
    flow_sizes_kb = flow_sizes / 1024  # Convert bytes to KB
    fcts_ms = fcts * 1000  # Convert seconds to ms
    
    # Create figure with two subplots
    fig, axes = plt.subplots(1, 2, figsize=(14, 5))
    fig.suptitle(f'{label} ({len(fcts)} flows)', fontsize=16, fontweight='bold')
    
    # --- Plot 1: Flow Size Distribution ---
    ax1 = axes[0]
    # Limit x-axis to 99.5th percentile to fit data better (exclude extreme outliers)
    size_limit = np.percentile(flow_sizes_kb, 99.5)
    filtered_sizes = flow_sizes_kb[flow_sizes_kb <= size_limit]
    ax1.hist(filtered_sizes, bins=50, color='steelblue', edgecolor='black', alpha=0.7)
    ax1.set_xlabel('Flow Size (KB)', fontsize=12)
    ax1.set_ylabel('Count', fontsize=12)
    ax1.set_title('Distribution of Flow Sizes', fontsize=14)
    ax1.axvline(np.mean(flow_sizes_kb), color='red', linestyle='--', linewidth=2, 
                label=f'Mean: {np.mean(flow_sizes_kb):.1f} KB')
    ax1.axvline(np.median(flow_sizes_kb), color='orange', linestyle='--', linewidth=2,
                label=f'Median: {np.median(flow_sizes_kb):.1f} KB')
    ax1.set_xlim(0, size_limit * 1.05)  # Add 5% padding
    ax1.legend()
    ax1.grid(True, alpha=0.3)
    
    # --- Plot 2: FCT Distribution ---
    ax2 = axes[1]
    # Limit x-axis to 99th percentile to fit data better
    fct_limit = np.percentile(fcts_ms, 99)
    filtered_fcts = fcts_ms[fcts_ms <= fct_limit]
    ax2.hist(filtered_fcts, bins=50, color='forestgreen', edgecolor='black', alpha=0.7)
    ax2.set_xlabel('Flow Completion Time (ms)', fontsize=12)
    ax2.set_ylabel('Count', fontsize=12)
    ax2.set_title('Distribution of FCT (up to P99)', fontsize=14)
    ax2.axvline(np.mean(fcts_ms), color='red', linestyle='--', linewidth=2,
                label=f'Mean: {np.mean(fcts_ms):.1f} ms')
    ax2.axvline(np.median(fcts_ms), color='orange', linestyle='--', linewidth=2,
                label=f'P50: {np.median(fcts_ms):.1f} ms')
    ax2.axvline(np.percentile(fcts_ms, 99), color='purple', linestyle='--', linewidth=2,
                label=f'P99: {np.percentile(fcts_ms, 99):.1f} ms')
    ax2.set_xlim(0, fct_limit * 1.1)  # Add 10% padding
    ax2.legend()
    ax2.grid(True, alpha=0.3)
    
    # Adjust layout and save
    plt.tight_layout()
    
    # Save to output directory with label in filename
    plot_path = f'{output_dir}/distributions_{label_safe}.png'
    plt.savefig(plot_path, dpi=150, bbox_inches='tight')
    info(f'*** Saved {label} distribution plots to: {plot_path} ***\n')
    plt.close(fig)
    
    # Also create a CDF plot for FCT (more informative for network analysis)
    fig2, ax3 = plt.subplots(figsize=(8, 5))
    sorted_fcts = np.sort(fcts_ms)
    cdf = np.arange(1, len(sorted_fcts) + 1) / len(sorted_fcts)
    ax3.plot(sorted_fcts, cdf, color='steelblue', linewidth=2)
    ax3.set_xlabel('Flow Completion Time (ms)', fontsize=12)
    ax3.set_ylabel('CDF', fontsize=12)
    ax3.set_title(f'CDF of Flow Completion Times - {label}', fontsize=14, fontweight='bold')
    ax3.axhline(0.5, color='orange', linestyle='--', alpha=0.7, label='P50')
    ax3.axhline(0.99, color='purple', linestyle='--', alpha=0.7, label='P99')
    ax3.axvline(np.median(fcts_ms), color='orange', linestyle=':', alpha=0.7)
    ax3.axvline(np.percentile(fcts_ms, 99), color='purple', linestyle=':', alpha=0.7)
    ax3.legend()
    ax3.grid(True, alpha=0.3)
    # Limit x-axis to P99.5 for better visualization
    ax3.set_xlim(0, np.percentile(fcts_ms, 99.5) * 1.05)
    ax3.set_ylim(0, 1.02)
    
    cdf_path = f'{output_dir}/fct_cdf_{label_safe}.png'
    plt.savefig(cdf_path, dpi=150, bbox_inches='tight')
    info(f'*** Saved FCT CDF plot to: {cdf_path} ***\n')
    
    plt.close('all')