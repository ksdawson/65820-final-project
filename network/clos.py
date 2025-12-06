from mininet.topo import Topo
from mininet.link import TCLink
from utils import visualize_topo

class ClosTopo(Topo):
    def __init__(self, spines=3, leaves=4, hosts_per_leaf=4):
        # Setup base class
        super(ClosTopo, self).__init__()

        # Create layer 2 of clos topology
        hosts = {}
        leaf_switches = {}
        for l in range(leaves):
            # Create leaf (ToR) switch
            switch_name = f'l{l}'
            switch = self.addSwitch(switch_name)
            leaf_switches[switch_name] = switch
            for h in range(hosts_per_leaf):
                # Create host
                host_name = f'l{l}_h{h}'
                host = self.addHost(host_name)
                hosts[host_name] = host

                # Connect to leaf switch
                self.addLink(host, switch, cls=TCLink,
                    bw=10, # Mbps
                    delay='0ms',
                    loss=1, # %
                    max_queue_size=100 # num packets
                )

        # Create layer 3
        spine_switches = []
        for s in range(spines):
            # Create spine switch
            switch_name = f's{s}'
            switch = self.addSwitch(switch_name)
            spine_switches[switch_name] = switch
            for name, leaf in leaf_switches.items():
                # Connect to spine switch
                self.addLink(leaf, switch, cls=TCLink,
                    bw=10, delay='0ms', loss=1, max_queue_size=100
                )

        # Store state
        self.hosts = hosts
        self.leaf_switches = leaf_switches
        self.spine_switches = spine_switches

if __name__ == '__main__':
    # Make clos network
    topo = ClosTopo()

    # Visualize topo
    visualize_topo(topo)