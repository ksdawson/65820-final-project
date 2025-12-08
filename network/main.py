from mininet.net import Mininet
from mininet.log import setLogLevel
from mininet.node import RemoteController
from mininet.cli import CLI
from vl2 import VL2Topo
import time

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
    topo = VL2Topo(D_A=2, D_I=2)
    net = Mininet(topo=topo, controller=None) # Don't add default controller
    net.addController('c0', controller=RemoteController, ip='127.0.0.1', port=6633)

    # Start network first
    net.start()

    # Set OpenFlow 1.3 for compatibility with ryu controllers
    for switch in net.switches:
        switch.cmd('ovs-vsctl set bridge {} protocols=OpenFlow13'.format(switch.name))

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
    CLI(net)

    # Stop network
    net.stop()

if __name__ == '__main__':
    setLogLevel('info')
    run()