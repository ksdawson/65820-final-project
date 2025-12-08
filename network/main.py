from mininet.net import Mininet
from mininet.log import setLogLevel
from mininet.node import RemoteController
from mininet.cli import CLI
from vl2 import VL2Topo
import time
from vl2_perf import run_traffic_test
from llm import replay_trace
from multi_llm import run_multi_trace_experiment
import os

PRIORITY_QUEUE = False

def configure_priority_queues(net):
    print("*** Configuring Priority Queues on Switches ***")
    for switch in net.switches:
        for intf in switch.intfList():
            if intf.name == 'lo': continue
            
            # OVS Command to create specific QoS queues
            # We use linux-htb. 
            # priority=1 is HIGHER than priority=2 in HTB config
            cmd = (
                f"ovs-vsctl -- set Port {intf.name} qos=@newqos -- "
                f"--id=@newqos create QoS type=linux-htb other-config:max-rate=1000000000 queues=0=@q0,1=@q1 -- "
                f"--id=@q0 create Queue other-config:min-rate=10000000 other-config:priority=2 -- " # Low Priority
                f"--id=@q1 create Queue other-config:min-rate=10000000 other-config:priority=1"    # High Priority
            )
            os.system(cmd)

def host_hello(net):
    print("*** Making hosts known to network (Sending 1 packet per host)...")
    for host in net.hosts:
        # Send a single ping to a dummy IP
        # The '&' runs it in the background so we don't wait for timeout
        host.cmd('ping -c 1 10.255.255.255 &') 
    
    # Give the controller a moment to process the flood of PacketIns
    time.sleep(2) 
    print("*** Network warmed up. Controller graph populated.")

def setup_network():
    # Initialize Network
    topo = VL2Topo(D_A=4, D_I=4, server_link=100, switch_link=1000)
    net = Mininet(topo=topo, controller=None) # Don't add default controller
    net.addController('c0', controller=RemoteController, ip='127.0.0.1', port=6633)

    # Start network first
    net.start()

    # Set OpenFlow 1.3 for compatibility with ryu controllers
    for switch in net.switches:
        switch.cmd('ovs-vsctl set bridge {} protocols=OpenFlow13'.format(switch.name))

    # Setup priority queues
    if PRIORITY_QUEUE:
        configure_priority_queues(net)

    return net

def run():
    # Initialize Network
    net = setup_network()

    # Wait for switches to connect to controller
    print("*** Waiting for switches to connect...")
    time.sleep(5)

    # Make hosts known to the network
    host_hello(net)

    # Test
    # net.pingAll()

    # Drop into CLI for manual testing
    # CLI(net)

    # Run perf test
    # run_traffic_test(net)

    # Test single trace
    # replay_trace(net, '../trace_generation/agent_trace/coding_trace_0.json')

    # Test multi trace
    # trace_set_one = [
    #     '../trace_generation/full_trace/full_coding_trace_0.json',
    #     '../trace_generation/full_trace/full_coding_trace_1.json'
    # ]
    trace_set_two = [
        '../trace_generation/full_trace/full_explain_trace_0.json',
        '../trace_generation/full_trace/full_explain_trace_1.json',
        '../trace_generation/full_trace/full_explain_trace_2.json',
        '../trace_generation/full_trace/full_explain_trace_3.json'
    ]
    # trace_set_three = [
    #     '../trace_generation/full_trace/full_mesh_trace_0.json',
    #     '../trace_generation/full_trace/full_mesh_trace_1.json'
    # ]
    # run_multi_trace_experiment(net, trace_set_one)
    run_multi_trace_experiment(net, trace_set_two)
    # run_multi_trace_experiment(net, trace_set_three)

    # Stop network
    net.stop()

if __name__ == '__main__':
    setLogLevel('info')
    run()