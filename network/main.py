from mininet.net import Mininet
from mininet.log import setLogLevel
from mininet.node import RemoteController
from vl2 import VL2Topo

def setup_network():
    # Initialize Network
    topo = VL2Topo(D_A=2, D_I=2)
    net = Mininet(topo=topo, controller=RemoteController)
    c0 = net.addController('c0', controller=RemoteController, ip='127.0.0.1', port=6633)

    # Configure OVS to use OpenFlow 1.3
    for t in range(1):
        s = net.get(f't{t}')
        s.cmd('ovs-vsctl set bridge s1 protocols=OpenFlow13')
    for a in range(2):
        s = net.get(f'a{a}')
        s.cmd('ovs-vsctl set bridge s1 protocols=OpenFlow13')
    for i in range(1):
        s = net.get(f'i{1}')
        s.cmd('ovs-vsctl set bridge s1 protocols=OpenFlow13')
    return net

def run():
    # Initialize Network
    net = setup_network()

    # Test
    net.pingAll()

    # Stop network
    net.stop()

if __name__ == '__main__':
    setLogLevel('info')
    run()