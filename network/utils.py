import networkx as nx
import matplotlib.pyplot as plt
from mininet.topo import Topo

def visualize_topo(topo, filename='topology.png'):
    g = nx.Graph()
    
    # Extract Nodes and distinguish Hosts vs Switches
    hosts = topo.hosts()
    switches = topo.switches()
    
    # Add nodes to the graph
    g.add_nodes_from(hosts, type='host')
    g.add_nodes_from(switches, type='switch')
    
    # Extract Links
    # topo.links() returns a list of (node1, node2) tuples
    g.add_edges_from(topo.links())

    # Setup Layout
    # 'spring_layout' usually provides a good aesthetic for network maps
    pos = nx.spring_layout(g, seed=42) 

    plt.figure(figsize=(10, 8))

    # Draw Switches (e.g., Red Squares)
    nx.draw_networkx_nodes(
        g, pos, 
        nodelist=switches, 
        node_color='#ffcccc',
        node_shape='s',
        node_size=2000, 
        edgecolors='black',
        label='Switches'
    )

    # Draw Hosts
    nx.draw_networkx_nodes(
        g, pos, 
        nodelist=hosts, 
        node_color='#cce5ff',
        node_shape='o',
        node_size=1500, 
        edgecolors='black',
        label='Hosts'
    )

    # Draw Edges and Labels
    nx.draw_networkx_edges(g, pos, width=2, edge_color='#888888')
    nx.draw_networkx_labels(g, pos, font_size=10, font_family='sans-serif', font_weight='bold')

    # Final visual tweaks
    plt.title('Mininet Topology Visualization', fontsize=15)
    plt.legend(scatterpoints=1)
    plt.axis('off') # Hide axis coordinates
    
    # Save to file
    plt.savefig(filename, dpi=300, bbox_inches='tight')
    plt.close()
    print(f'Topology saved to {filename}')

if __name__ == '__main__':
    # Define a simple custom topology to test
    class SimpleTopo(Topo):
        def build(self):
            h1 = self.addHost('h1')
            h2 = self.addHost('h2')
            s1 = self.addSwitch('s1')
            self.addLink(h1, s1)
            self.addLink(h2, s1)

    # Instantiate and visualize
    my_topo = SimpleTopo()
    visualize_topo(my_topo, 'my_network.png')