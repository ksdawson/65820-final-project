from mininet.topo import Topo
from graphviz import Graph

def visualize_topo(topo, filename='topology', format='png'):
    # Create a Graphviz object
    # 'engine=dot' is best for hierarchical/tree structures
    # 'engine=neato' is better for mesh/ring structures
    dot = Graph(name='Mininet Topology', format=format, engine='dot')
    
    # Global graph settings for a cleaner look
    dot.attr(overlap='false', splines='true')
    dot.attr('node', shape='circle', style='filled', fontname='Helvetica')
    
    # Add Switches (Square shape, Red color)
    for switch in topo.switches():
        dot.node(switch, shape='square', fillcolor='#ffcccc', label=switch)
        
    # Add Hosts (Circle shape, Blue color)
    for host in topo.hosts():
        dot.node(host, shape='circle', fillcolor='#cce5ff', label=host)
        
    # Add Links
    # Links in Mininet are unordered (src, dst), Graphviz handles them as undirected edges
    for link in topo.links():
        src, dst = link
        dot.edge(src, dst)

    # Render output
    output_path = dot.render(filename=filename, cleanup=True)
    print(f'Topology saved to {output_path}')

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
    visualize_topo(my_topo, 'example_topology')