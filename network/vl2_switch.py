import networkx as nx
from ryu.topology import event
from ryu.topology.api import get_switch, get_link
from ryu.controller.handler import set_ev_cls
from ryu.app.simple_switch_13 import SimpleSwitch13
import logging

class VL2Switch(SimpleSwitch13):
    def __init__(self, *args, **kwargs):
        super(VL2Switch, self).__init__(*args, **kwargs)
        self.logger.setLevel(logging.WARNING) # don't print everything
        self.net_graph = nx.DiGraph()

    @set_ev_cls(event.EventSwitchEnter)
    def handler_switch_enter(self, ev):
        self._update_topology()

    @set_ev_cls(event.EventLinkAdd)
    def handler_link_add(self, ev):
        self._update_topology()

    def _update_topology(self):
        # Clear current graph to rebuild (simplest approach)
        self.net_graph.clear()
        
        # Get all switches
        switch_list = get_switch(self, None)
        switches = [switch.dp.id for switch in switch_list]
        self.net_graph.add_nodes_from(switches)

        # Get all links
        link_list = get_link(self, None)
        links = [(link.src.dpid, link.dst.dpid, {'port': link.src.port_no}) 
                 for link in link_list]
        self.net_graph.add_edges_from(links)

        # Print the graph
        self.logger.info("--- Current Topology ---")
        self.logger.info("Nodes: %s", self.net_graph.nodes())
        self.logger.info("Edges: %s", self.net_graph.edges())