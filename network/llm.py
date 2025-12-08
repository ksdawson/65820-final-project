import json
import time
from datetime import datetime
from mininet.log import info

def parse_iso_time(t_str):
    '''Parses '2025-12-06T15:57:25.701630' into a float timestamp'''
    # Adjust format string if your input varies
    dt = datetime.strptime(t_str, '%Y-%m-%dT%H:%M:%S.%f')
    return dt.timestamp()

def replay_trace(net, trace_file_path):
    info(f'\n*** Loading trace file: {trace_file_path} ***\n')
    
    with open(trace_file_path, 'r') as f:
        events = json.load(f)

    # 1. Setup: Start Listeners on ALL hosts
    # We map JSON ID '0' -> Mininet 'h1', '1' -> 'h2', etc.
    # Adjust this mapping if your hosts are named differently!
    host_map = {} 
    
    # Sort hosts by name to ensure consistent mapping (h1, h2, h3...)
    sorted_hosts = sorted(net.hosts, key=lambda x: x.name)
    
    for i, h in enumerate(sorted_hosts):
        host_map[i] = h
        # Start server in background
        h.cmd('python3 traffic_tool.py -m server -p 8000 &')

    info('*** Servers started. Preparing trace replay... ***\n')

    # 2. Normalize Timestamps
    # Sort events by time just in case JSON is out of order
    events.sort(key=lambda x: parse_iso_time(x['time_sent']))
    
    start_time_trace = parse_iso_time(events[0]['time_sent'])
    start_time_wall = time.time()
    
    info(f'*** Replaying {len(events)} events... ***\n')

    for i, event in enumerate(events):
        # Calculate when this event should happen relative to start
        event_time = parse_iso_time(event['time_sent'])
        target_delay = event_time - start_time_trace
        
        # Calculate how long we have been running
        current_wall_delta = time.time() - start_time_wall
        
        # Sleep if we are ahead of schedule
        sleep_needed = target_delay - current_wall_delta
        if sleep_needed > 0:
            time.sleep(sleep_needed)

        # 3. Execute Event
        sender_id = event['sender']
        receivers = event['receiver']
        # Convert KB to Bytes (1 KB = 1024 Bytes)
        # Using int() to avoid fractional bytes
        payload_bytes = int(event['data_size(kb)'] * 1024)
        
        if sender_id not in host_map:
            info(f'Warning: Sender ID {sender_id} not found in host map. Skipping.\n')
            continue

        sender_host = host_map[sender_id]
        
        # Handle Multicast/Broadcast (One sender -> Multiple Receivers)
        for rx_id in receivers:
            if rx_id not in host_map:
                continue
                
            rx_host = host_map[rx_id]
            rx_ip = rx_host.IP()
            
            # Construct command
            # We run in background '&' so the python script doesn't block Mininet
            # if we have multiple receivers for one event
            cmd = (f'python3 traffic_tool.py -m client '
                   f'-t {rx_ip} -p 8000 -b {payload_bytes} &')
            
            sender_host.cmd(cmd)
            
            info(f'[{target_delay:.2f}s] {sender_host.name} -> {rx_host.name} '
                 f'({payload_bytes} bytes)\n')

    # Wait a bit for stragglers to finish
    time.sleep(2)
    
    # Cleanup
    info('*** Trace finished. Killing servers... ***\n')
    for h in net.hosts:
        h.cmd('killall -9 python3')