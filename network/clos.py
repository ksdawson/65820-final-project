from mininet.topo import Topo
from mininet.link import TCLink
import networkx as nx

class ClosTopo(Topo):
    def __init__(self, spines=3, leaves=4, hosts_per_leaf=4):
        # Setup base class
        super(ClosTopo, self).__init__()

        # Create layer 2 of clos topology
        hosts = []
        leaf_switches = []
        for l in range(leaves):
            # Create leaf (ToR) switch
            switch_name = f'l{l}'
            switch = self.addSwitch(switch_name)
            leaf_switches.append(switch_name)
            for h in range(hosts_per_leaf):
                # Create host
                host_name = f'l{l}_h{h}'
                host = self.addHost(host_name)
                hosts.append(host_name)

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
            spine_switches.append(switch_name)
            for l in range(leaves):
                # Connect to spine switch
                leaf_switch = self.getNodeByName(leaf_switches[l])
                self.addLink(leaf_switch, switch, cls=TCLink,
                    bw=10, delay='0ms', loss=1, max_queue_size=100
                )

if __name__ == '__main__':
    # Make clos network
    topo = ClosTopo()

    # Build NetworkX graph from Mininet topo
    G = topo.to_networkx()

    # Save as DOT file
    nx.drawing.nx_pydot.write_dot(G, 'clos.dot')