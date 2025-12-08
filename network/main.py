from mininet.net import Mininet
from mininet.log import setLogLevel
from mininet.node import RemoteController
from vl2 import VL2Topo

def run():
    # Initialize Topology
    topo = VL2Topo(D_A=2, D_I=2)
    
    # Initialize Mininet with RemoteController
    net = Mininet(topo=topo, controller=RemoteController)
    
    print("Starting network...")
    net.start()
    
    print("Testing connectivity (Ping All)...")
    net.pingAll()
    
    # print("Running CLI...")
    # CLI(net)
    
    print("Stopping network...")
    net.stop()

if __name__ == '__main__':
    setLogLevel('info')
    run()