import networkx as nx
import matplotlib.pyplot as plt
from mininet.topo import Topo
from graphviz import Graph

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
    # plt.legend(scatterpoints=1)
    plt.axis('off') # Hide axis coordinates
    
    # Save to file
    plt.savefig(filename, dpi=300, bbox_inches='tight')
    plt.close()
    print(f'Topology saved to {filename}')

def save_topology_graphviz(topo, filename="topology", format="png"):
    """
    Visualizes a Mininet topology using Graphviz.
    Automatically handles hierarchical layout.
    """
    # Create a Graphviz object
    # 'engine=dot' is best for hierarchical/tree structures
    # 'engine=neato' is better for mesh/ring structures
    dot = Graph(name='Mininet Topology', format=format, engine='dot')
    
    # Global graph settings for a cleaner look
    dot.attr(overlap='false', splines='true')
    dot.attr('node', shape='circle', style='filled', fontname='Helvetica')
    
    # 1. Add Switches (Square shape, Red color)
    for switch in topo.switches():
        dot.node(switch, shape='square', fillcolor='#ffcccc', label=switch)
        
    # 2. Add Hosts (Circle shape, Blue color)
    for host in topo.hosts():
        dot.node(host, shape='circle', fillcolor='#cce5ff', label=host)
        
    # 3. Add Links
    # Links in Mininet are unordered (src, dst), Graphviz handles them as undirected edges
    for link in topo.links():
        src, dst = link
        dot.edge(src, dst)

    # 4. Render output
    # This creates 'filename.png' (or whatever format you chose)
    output_path = dot.render(filename=filename, cleanup=True)
    print(f"Topology saved to {output_path}")

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
    # visualize_topo(my_topo, 'example_topo.png')

    save_topology_graphviz(my_topo, 'example_topology')