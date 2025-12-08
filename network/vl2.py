from mininet.topo import Topo
from mininet.link import TCLink
# from utils import visualize_topo

class VL2Topo(Topo):
    def __init__(self, D_A=8, D_I=8, server_link=1, switch_link=10):
        """
        Setups a VL2 datacenter network according to https://dl.acm.org/doi/10.1145/1594977.1592576.
        Args:
            D_A (int): Number of ports on aggregation switches.
            D_I (int): Number of ports on intermediate switches.
            server_link (int): Server link bandwidth in Gbps.
            switch_link (int): Switch link bandwidth in Gbps.
        """
        # Setup base class
        super(VL2Topo, self).__init__()

        # Num of nodes
        num_inter = D_A // 2
        num_aggr = D_I
        num_tor = D_A * D_I // 4
        num_host = 20 * num_tor

        # Setup nodes with explicit DPIDs (formatted as hex strings for Mininet)
        # We use hex(val)[2:] to remove the '0x' prefix
        self._inter_switches = {f'i{i}': self.addSwitch(f'i{i}', dpid=hex(1000 + i)[2:]) for i in range(num_inter)}
        self._aggr_switches = {f'a{a}': self.addSwitch(f'a{a}', dpid=hex(2000 + a)[2:]) for a in range(num_aggr)}
        self._tor_switches = {f't{t}': self.addSwitch(f't{t}', dpid=hex(3000 + t)[2:]) for t in range(num_tor)}

        # Connect hosts to ToRs
        for t in range(num_tor):
            tor = self._tor_switches[f't{t}']
            for h in range(20):
                host = self._hosts[f'h{t*20+h}']
                self.addLink(host, tor, cls=TCLink,
                    bw=server_link, # Mbps
                    delay='0ms',
                    loss=1, # %
                    max_queue_size=100 # num packets
                )
        # Connect ToRs to aggregate switches
        for t in range(2*num_tor):
            # Connects to two aggr
            tor = self._tor_switches[f't{t//2}']
            aggr = self._aggr_switches[f'a{t%num_aggr}']
            self.addLink(tor, aggr, cls=TCLink, bw=switch_link, delay='0ms', loss=1, max_queue_size=100)
        # Connect aggregate to intermediate switches
        for a in range(D_A//2*num_aggr):
            # Connects to D_A/2 inter
            aggr = self._aggr_switches[f'a{a//(D_A//2)}']
            inter = self._inter_switches[f'i{a%num_inter}']
            self.addLink(aggr, inter, cls=TCLink, bw=switch_link, delay='0ms', loss=1, max_queue_size=100)

if __name__ == '__main__':
    # Make VL2 networks
    topo2b2 = VL2Topo(D_A=2, D_I=2)
    topo4b4 = VL2Topo(D_A=4, D_I=4)

    # Visualize topo
    # visualize_topo(topo2b2, 'vl2_2b2_topology')
    # visualize_topo(topo4b4, 'vl2_4b4_topology')