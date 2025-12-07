import sys
import time
import re
from mininet.net import Mininet
from mininet.node import RemoteController, OVSKernelSwitch
from mininet.link import TCLink
from mininet.log import setLogLevel, info
from mininet.cli import CLI
from vl2 import VL2Topo 

def parse_flow_output_ports(switch_name):
    """
    Inspects the OVS flow table of a switch and returns a set of 
    output ports currently being used for forwarding IP traffic.
    """
    # Run ovs-ofctl command on the switch to dump flows
    # We filter for 'ip' flows to avoid seeing ARP/LLDP noise
    output = sys.modules['__main__'].net.get(switch_name).cmd(
        f'ovs-ofctl -O OpenFlow13 dump-flows {switch_name} ip'
    )
    
    # Regex to find 'actions=output:X' or 'actions=...,output:X'
    # This captures the port number
    ports = set()
    for line in output.split('\n'):
        if 'actions=' in line:
            match = re.search(r'output:(\d+)', line)
            if match:
                ports.add(int(match.group(1)))
    return ports

def test_vlb_logic(net):
    """
    Verifies that the VL2 Controller is actually using Equal-Cost Multi-Path (ECMP)
    style logic (random VLB) by checking if multiple uplinks are used.
    """
    info('\n*** Testing Valiant Load Balancing Logic ***\n')
    
    # 1. Select a ToR switch and its hosts
    # In your Topo: t0 connects to h0...h19. 
    # Uplinks are usually last ports (21, 22).
    tor = 't0'
    src_hosts = ['h0', 'h1', 'h2', 'h3', 'h4'] # Test multiple sources
    dst_host = 'h40' # A host on a different ToR (t2)
    
    info(f'* Generating traffic from {tor} hosts ({src_hosts}) to {dst_host}...\n')
    
    # 2. Generate traffic to force the controller to install flows
    h_dst = net.get(dst_host)
    for h_name in src_hosts:
        h_src = net.get(h_name)
        # Send a single ping to trigger flow installation
        h_src.cmd(f'ping -c 1 -W 1 {h_dst.IP()}')
    
    # 3. Inspect Flow Table on ToR
    info(f'* Inspecting flow table on {tor}...\n')
    used_ports = parse_flow_output_ports(tor)
    
    # 4. Analyze Results
    # We expect traffic to go out via ports > 20 (the uplinks)
    # We expect MORE THAN ONE uplink to be used if VLB is working
    uplink_ports = {p for p in used_ports if p > 20}
    
    info(f'  > Raw Output Ports found: {used_ports}\n')
    info(f'  > Uplink Ports used: {uplink_ports}\n')
    
    if len(uplink_ports) > 1:
        info('*** PASS: Traffic is balanced across multiple uplinks! ***\n')
    elif len(uplink_ports) == 1:
        info('*** WARNING: Traffic is using only ONE uplink. Run again to check for randomness. ***\n')
    else:
        info('*** FAIL: No uplink traffic detected. Check controller logic or port mapping. ***\n')

def run_verification():
    setLogLevel('info')
    
    # Initialize Topology and Net
    topo = VL2Topo()
    net = Mininet(topo=topo, 
                  controller=RemoteController, 
                  switch=OVSKernelSwitch,
                  link=TCLink)
    
    # Make 'net' globally accessible for the helper function
    sys.modules['__main__'].net = net

    try:
        info('*** Starting Network ***\n')
        net.start()
        
        # Wait for controller to connect and stabilize
        info('*** Waiting 5 seconds for controller convergence... ***\n')
        time.sleep(5)
        
        # Test 1: Basic Connectivity
        info('\n*** Test 1: Ping All (Connectivity Check) ***\n')
        loss = net.pingAll()
        if loss > 0:
            info(f'*** FAIL: Packet loss detected ({loss}%). Check links or ARP handling. ***\n')
        else:
            info('*** PASS: Full connectivity established. ***\n')

        # Test 2: VLB Logic
        test_vlb_logic(net)
        
        # Drop into CLI for manual inspection if needed
        info('\n*** Running CLI (type "exit" to quit) ***\n')
        CLI(net)
        
    finally:
        info('*** Stopping Network ***\n')
        net.stop()

if __name__ == '__main__':
    run_verification()