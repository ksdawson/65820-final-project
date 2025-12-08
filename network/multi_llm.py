import json
import time
import math
import random
import os
import numpy as np
import glob
from mininet.log import info, error

def load_and_merge_traces(trace_files):
    """
    Loads multiple JSON trace files.
    Namespaces all logical IDs to ensure uniqueness across files.
    Example: '0.0' in trace file 1 becomes 'T1_0.0'.
    """
    merged_events = []
    
    # We will flatten the config into a simple list of unique logical processes
    # format: [ ('T0_0.0', resource_cost), ('T0_0.1', resource_cost), ... ]
    all_logical_processes = []

    for trace_idx, filepath in enumerate(trace_files):
        try:
            with open(filepath, 'r') as f:
                data = json.load(f)
        except Exception as e:
            error(f"Failed to load {filepath}: {e}\n")
            continue
            
        # 1. Extract Config (The first element)
        # Structure: {"0": [["0.0", 1], ...], "1": [...]}
        process_config = None
        events = []
        
        if len(data) > 0 and isinstance(data[0], dict) and "0" in data[0]:
             process_config = data[0]
             events = data[1:]
        elif "sender" in data[0]:
            # No config map found, assuming just events. 
            # We must infer processes from events later or error out.
            # For this specific format, we expect the config map.
            error(f"Error: {filepath} missing config map at index 0.\n")
            continue

        # 2. Namespace the Config and collect processes
        # trace_prefix = f"T{trace_idx}_" 
        trace_prefix = f"{trace_idx}-" # Using dash separator
        
        # Sort groups to ensure deterministic loading order
        for group_id in sorted(process_config.keys(), key=lambda x: int(x)):
            group_list = process_config[group_id]
            for proc_entry in group_list:
                # proc_entry: ["0.0", 1]
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
    
    info(f"*** Loaded {len(all_logical_processes)} unique processes from {len(trace_files)} traces. ***\n")
    return all_logical_processes, merged_events

def map_processes_to_hosts(net, all_logical_processes, percent_usage, procs_per_host):
    """
    Maps namespaced logical processes (e.g., "T0-1.0") to Physical Mininet Nodes.
    """
    # 1. Determine Physical Resources
    all_hosts = net.hosts
    total_physical = len(all_hosts)
    
    # Calculate how many physical hosts we are ALLOWED to use
    active_count = int(math.ceil(total_physical * percent_usage))
    if active_count == 0: active_count = 1
    
    physical_pool = all_hosts[:active_count]
    info(f"*** Resource Allocation: Using {active_count}/{total_physical} physical hosts. ***\n")

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

# def run_multi_trace_experiment(net, trace_file_paths, percentage=1.0, procs_per_host=8):
#     # 1. Load Data (Namespaced)
#     all_logical_procs, events = load_and_merge_traces(trace_file_paths)
#     if not all_logical_procs:
#         return

#     # 2. Start Servers
#     info("*** Starting Traffic Listeners... ***\n")
#     for h in net.hosts:
#         h.cmd('python3 traffic_tool.py -m server -p 8000 &')
#     time.sleep(1) 

#     # 3. Compute Mapping
#     host_map = map_processes_to_hosts(net, all_logical_procs, percentage, procs_per_host)

#     # 4. Replay Loop
#     info(f"*** Starting Replay of {len(events)} events... ***\n")
    
#     start_wall_time = time.time()
#     first_event_time = events[0].get('time', 0.0)

#     for i, event in enumerate(events):
#         # Timing
#         event_time = event.get('time', 0.0)
#         target_delay = event_time - first_event_time
#         current_delay = time.time() - start_wall_time
        
#         if target_delay > current_delay:
#             time.sleep(target_delay - current_delay)
            
#         # Execution
#         sender_name = event.get('sender')     
#         receivers = event.get('receiver', []) 
#         size_bytes = int(event.get('size', 0))

#         if sender_name not in host_map:
#             continue
            
#         phys_sender = host_map[sender_name]
        
#         for rx_name in receivers:
#             if rx_name not in host_map:
#                 continue
#             phys_rx = host_map[rx_name]
            
#             # Optimization: Skip loopback (optional)
#             if phys_sender == phys_rx: continue
                
#             cmd = (f'python3 traffic_tool.py -m client '
#                    f'-t {phys_rx.IP()} -p 8000 -b {size_bytes} &')
#             phys_sender.cmd(cmd)
            
#     # Cleanup
#     time.sleep(2)
#     info("*** Replay Complete. Killing servers. ***\n")
#     for h in net.hosts:
#         h.cmd('killall -9 python3')

def run_multi_trace_experiment(net, trace_file_paths, percentage=1.0, procs_per_host=8, 
                               random_sample_n=None, shuffle_mapping=True):
    
    # 0. Setup Logging Directory
    log_dir = "/tmp/mininet_metrics"
    os.system(f"rm -rf {log_dir}")
    os.system(f"mkdir -p {log_dir}")
    
    # 1. Load Data (Namespaced)
    all_logical_procs, events = load_and_merge_traces(trace_file_paths)
    if not all_logical_procs:
        return

    # 2. Start Servers
    info("*** Starting Traffic Listeners... ***\n")
    for h in net.hosts:
        h.cmd('python3 traffic_tool.py -m server -p 8000 &')
    time.sleep(1)

    # 3. Compute Mapping
    host_map = map_processes_to_hosts(net, all_logical_procs, percentage, procs_per_host)

    # 4. Replay Loop
    info(f"*** Starting Replay... Logging to {log_dir} ***\n")
    
    start_wall_time = time.time()
    if events:
        first_event_time = events[0].get('time', 0.0)

    for i, event in enumerate(events):
        # Timing
        event_time = event.get('time', 0.0)
        target_delay = event_time - first_event_time
        current_delay = time.time() - start_wall_time
        
        if target_delay > current_delay:
            time.sleep(target_delay - current_delay)
            
        # Execution
        sender_name = event.get('sender')
        receivers = event.get('receiver', [])
        size_bytes = int(event.get('size', 0))

        if sender_name not in host_map: continue
        phys_sender = host_map[sender_name]
        
        for rx_name in receivers:
            if rx_name not in host_map: continue
            phys_rx = host_map[rx_name]
            if phys_sender == phys_rx: continue
            
            # --- CHANGED: REDIRECT OUTPUT TO LOG FILE ---
            # We append (>>) to a log file named after the sender host
            log_file = f"{log_dir}/{phys_sender.name}.log"
            
            cmd = (f'python3 traffic_tool.py -m client '
                   f'-t {phys_rx.IP()} -p 8000 -b {size_bytes} '
                   f'>> {log_file} 2>&1 &') # Redirect stdout and stderr
            
            phys_sender.cmd(cmd)

    # 5. Wait for stragglers
    info("*** Replay finished. Waiting 5s for pending flows... ***\n")
    time.sleep(5)
    
    # 6. Cleanup
    for h in net.hosts:
        h.cmd('killall -9 python3')

    # 7. ANALYZE METRICS
    analyze_results(log_dir)

def analyze_results(log_dir):
    info("\n" + "="*40 + "\n")
    info("   EXPERIMENT RESULTS   \n")
    info("="*40 + "\n")
    
    fcts = []
    throughputs = []
    errors = 0
    total_flows = 0
    
    log_files = glob.glob(f"{log_dir}/*.log")
    
    for log_f in log_files:
        with open(log_f, 'r') as f:
            for line in f:
                try:
                    data = json.loads(line)
                    if data.get('event') == 'flow_complete':
                        fcts.append(data['duration_sec'])
                        throughputs.append(data['throughput_mbps'])
                        total_flows += 1
                    elif data.get('event') == 'error':
                        errors += 1
                except:
                    pass # Ignore non-json lines
                    
    if total_flows == 0:
        info("No successful flows recorded.\n")
        return

    # Calculate Statistics
    fcts = np.array(fcts)
    
    info(f"Total Flows:       {total_flows}\n")
    info(f"Failed/Error:      {errors}\n")
    info("-" * 20 + "\n")
    
    # Flow Completion Time (Lower is Better)
    info(f"Avg FCT:           {np.mean(fcts)*1000:.2f} ms\n")
    info(f"P50 FCT (Median):  {np.percentile(fcts, 50)*1000:.2f} ms\n")
    info(f"P99 FCT (Tail):    {np.percentile(fcts, 99)*1000:.2f} ms\n")
    info(f"Max FCT:           {np.max(fcts)*1000:.2f} ms\n")
    
    info("-" * 20 + "\n")
    # Throughput (Higher is Better)
    info(f"Avg Throughput:    {np.mean(throughputs):.2f} Mbps\n")
    info("="*40 + "\n")