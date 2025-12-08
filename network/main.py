from mininet.net import Mininet
from mininet.log import setLogLevel
from mininet.node import RemoteController
from mininet.cli import CLI
from vl2 import VL2Topo
import time

def setup_network():
    # Initialize Network
    topo = VL2Topo(D_A=2, D_I=2)
    net = Mininet(topo=topo, controller=None)  # Don't add default controller
    net.addController('c0', controller=RemoteController, ip='127.0.0.1', port=6633)

    # Start network first
    net.start()

    # Enable STP on all switches to prevent broadcast storms from loops
    # Also set OpenFlow 1.3 for compatibility with sanity_check.py
    for switch in net.switches:
        # switch.cmd('ovs-vsctl set bridge {} stp_enable=true'.format(switch.name))
        switch.cmd('ovs-vsctl set bridge {} protocols=OpenFlow13'.format(switch.name))

    return net

def run():
    # Initialize Network
    net = setup_network()

    # Wait for switches to connect to controller and STP to converge
    print("*** Waiting for switches to connect and STP to converge...")
    time.sleep(5) # STP needs ~5s to converge

    # Test
    # net.pingAll()

    # Drop into CLI for manual testing
    CLI(net)

    # Stop network
    net.stop()

if __name__ == '__main__':
    setLogLevel('info')
    run()