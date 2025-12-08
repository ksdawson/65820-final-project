import random
import re
from mininet.log import info

def parse_iperf_bandwidth(output):
    # Regex to find bandwidth (e.g., 9.54 Gbits/sec)
    match = re.search(r'(\d+\.?\d*)\s([KMG]bits/sec)', output)
    if match:
        val = float(match.group(1))
        unit = match.group(2)
        # Normalize to Mbps
        if 'Gbits' in unit: val *= 1000
        elif 'Kbits' in unit: val /= 1000
        return val
    return 0.0

def run_traffic_test(net, duration=15, num_flows=10):
    info(f'\n*** Starting Traffic Test: {num_flows} concurrent flows for {duration}s ***\n')
    
    hosts = net.hosts
    # Start iperf servers on ALL hosts
    # -s: server, -u: udp (optional, using TCP here), & runs in background
    for h in hosts:
        h.cmd('iperf -s &')

    # Generate random source-destination pairs
    # Ensure src != dst
    flow_pairs = []
    while len(flow_pairs) < num_flows:
        src = random.choice(hosts)
        dst = random.choice(hosts)
        if src != dst and (src, dst) not in flow_pairs:
            flow_pairs.append((src, dst))

    info(f'*** Generated {len(flow_pairs)} random pairs. Starting transmission...\n')

    # Start iperf clients simultaneously
    # We store the Popen objects to read output later
    client_processes = []
    for i, (src, dst) in enumerate(flow_pairs):
        # -t: duration, -c: client target, -f m: format in Mbps
        cmd = f'iperf -c {dst.IP()} -t {duration} -f m'
        # sendCmd() and waitOutput() are blocking, so we use popen() for concurrency
        p = src.popen(cmd, shell=True)
        client_processes.append((src, dst, p))
        info(f'    Flow {i+1}: {src.name} -> {dst.name}\n')

    # Wait for the test to finish
    # We parse the output as it finishes
    results = []
    for src, dst, p in client_processes:
        out, err = p.communicate() # Wait for process to finish
        out = out.decode() # Decode bytes to string
        bw = parse_iperf_bandwidth(out)
        results.append(bw)
        info(f'    Result {src.name}->{dst.name}: {bw:.2f} Mbps\n')

    # Cleanup: Kill iperf servers
    for h in hosts:
        h.cmd('killall -9 iperf')

    # Summary Stats
    total_bw = sum(results)
    avg_bw = total_bw / len(results) if results else 0
    info('\n*** Test Complete ***\n')
    info(f'Total Aggregate Bandwidth: {total_bw:.2f} Mbps\n')
    info(f'Average Flow Bandwidth:    {avg_bw:.2f} Mbps\n')

    return total_bw