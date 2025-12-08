from mininet.net import Mininet
from mininet.log import setLogLevel
from mininet.node import RemoteController
from vl2 import VL2Topo

def setup_network():
    # Initialize Network
    topo = VL2Topo(D_A=2, D_I=2)
    net = Mininet(topo=topo, controller=None)  # Don't add default controller
    net.addController('c0', controller=RemoteController, ip='127.0.0.1', port=6633)

    # Start network first
    net.start()

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