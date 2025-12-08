import json
import time
import math
import sys
import random
import os
import re
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

def parse_iperf_fct(output):
    """
    Parses iperf output to find the duration (Flow Completion Time).
    Example Output line: 
    '[  3]  0.0- 0.0 sec  178 KBytes  31.4 Mbits/sec'
    We want the second timestamp '0.0 sec' (or whatever the end time is).
    """
    # Regex: Look for '0.0- X.X sec'
    # Group 1 matches the end time (duration)
    match = re.search(r'0\.0-\s*(\d+\.?\d*)\s+sec', output)
    if match:
        return float(match.group(1))
    return None

def run_multi_trace_experiment(net, trace_file_paths, percentage=1.0, procs_per_host=8):
    # 1. Load and Merge Traces
    all_logical_procs, events = load_and_merge_traces(trace_file_paths)
    host_map = map_processes_to_hosts(net, all_logical_procs, percentage, procs_per_host)
    
    # 2. Start iperf Servers (iperf2 handles multiple clients natively)
    info('\n*** Starting iperf servers on all hosts... ***\n')
    for h in net.hosts:
        # -s: Server
        # -p: Port 5001 (Default)
        # & runs it in background
        h.cmd('iperf -s -p 5001 &')
    
    # Wait for servers to spin up
    time.sleep(1)

    # 3. Replay Trace
    info(f'*** Replaying {len(events)} flows... ***\n')
    
    start_wall_time = time.time()
    if events: 
        first_event_time = events[0].get('time', 0.0)
    
    # We store active flow handles here: (src_node, dst_node, popen_object, expected_bytes)
    active_flows = []

    for i, event in enumerate(events):
        # --- Timing ---
        target_delay = event.get('time', 0.0) - first_event_time
        current_delay = time.time() - start_wall_time
        
        if target_delay > current_delay:
            time.sleep(target_delay - current_delay)

        # --- Setup ---
        sender_name = event.get('sender')
        receivers = event.get('receiver', [])
        
        # Get size, default to 1KB if missing
        try:
            size_bytes = int(float(event.get('size', 1024)))
        except:
            size_bytes = 1024

        if sender_name not in host_map: continue
        phys_sender = host_map[sender_name]

        for rx_name in receivers:
            if rx_name not in host_map: continue
            phys_rx = host_map[rx_name]
            if phys_sender == phys_rx: continue

            # --- LAUNCH CLIENT (Non-Blocking) ---
            # -c: Client
            # -n: Number of bytes to send (Critical for FCT!)
            # -p: Port 5001
            # -y C: CSV output (Optional, but default text is easier for simple regex)
            cmd = f'iperf -c {phys_rx.IP()} -p 5001 -n {size_bytes}'
            
            # popen() starts the process and returns immediately (non-blocking)
            # shell=True lets us pass the full command string
            p = phys_sender.popen(cmd, shell=True, stdout=sys.stdout, stderr=sys.stdout)
            
            # Use 'p.stdout' explicitly if you want to capture output in Python
            # To capture output for parsing, we must use PIPE
            import subprocess
            p = phys_sender.popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            
            active_flows.append({
                'src': phys_sender.name,
                'dst': phys_rx.name,
                'proc': p,
                'bytes': size_bytes
            })

    info(f'*** All flows launched. Waiting for completion... ***\n')

    # 4. Harvest Results
    fcts = []
    total_bytes = 0
    errors = 0

    for flow in active_flows:
        # communicate() blocks until the process finishes
        out, err = flow['proc'].communicate() 
        
        duration = parse_iperf_fct(out)
        
        if duration is not None:
            fcts.append(duration)
            total_bytes += flow['bytes']
            # Optional: Print individual completion
            # print(f"Flow {flow['src']}->{flow['dst']} finished in {duration}s")
        else:
            errors += 1
            # print(f"Error parsing flow {flow['src']}->{flow['dst']}:\n{out}\n{err}")

    # 5. Cleanup
    for h in net.hosts:
        h.cmd('killall -9 iperf')

    # 6. Report
    info('\n' + '='*40 + '\n')
    info('   IPERF RESULTS   \n')
    info('='*40 + '\n')
    
    if len(fcts) > 0:
        fcts = [f * 1000.0 for f in fcts] # Convert to ms
        info(f'Total Flows:      {len(active_flows)}\n')
        info(f'Successful:       {len(fcts)}\n')
        info(f'Failed/Parse Err: {errors}\n')
        info('-'*20 + '\n')
        info(f'Avg FCT:          {sum(fcts)/len(fcts):.2f} ms\n')
        info(f'Max FCT:          {max(fcts):.2f} ms\n')
        info(f'Total Volume:     {total_bytes} Bytes\n')
    else:
        info('No successful flows recorded.\n')
    
    info('='*40 + '\n')