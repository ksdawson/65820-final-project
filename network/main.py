from mininet.net import Mininet
from mininet.log import setLogLevel
from mininet.node import RemoteController, OVSSwitch
from vl2 import VL2Topo

def run():
    # Initialize Topology
    topo = VL2Topo(D_A=2, D_I=2)
    net = Mininet(topo=topo, controller=RemoteController)
    c0 = net.addController('c0', controller=RemoteController, ip='127.0.0.1', port=6633, protocols='OpenFlow13')
    net.start()

    # Stop the network when done
    net.stop()

if __name__ == '__main__':
    setLogLevel('info')
    run()