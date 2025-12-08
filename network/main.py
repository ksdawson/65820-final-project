from mininet.net import Mininet
from mininet.log import setLogLevel
from mininet.node import RemoteController, OVSSwitch
from vl2 import VL2Topo

def run():
    # Initialize Topology
    topo = VL2Topo(D_A=2, D_I=2)
    
    net = Mininet(
        topo=topo, 
        controller=RemoteController, 
        switch=OVSSwitch,
        autoSetMacs=True
    )
    
    # Apply protocols to all switches
    for switch in net.switches:
        switch.opts['protocols'] = 'OpenFlow13'

    print("Starting network...")
    net.start()
    
    print("Waiting 5 seconds for LLDP discovery...")
    import time
    time.sleep(5) 
    
    print("Testing connectivity...")
    net.pingAll()

    net.stop()

if __name__ == '__main__':
    setLogLevel('info')
    run()