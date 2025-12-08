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

    return net

def run():
    # Initialize Network
    net = setup_network()

    # Wait for switches to connect to controller
    print("*** Waiting for switches to connect to controller...")
    time.sleep(3)

    # Test
    net.pingAll()

    # Drop into CLI for manual testing
    CLI(net)

    # Stop network
    net.stop()

if __name__ == '__main__':
    setLogLevel('info')
    run()