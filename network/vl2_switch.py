from ryu.base import app_manager
from ryu.controller.handler import set_ev_cls
from ryu.topology import event
from ryu.topology.api import get_switch, get_link
import networkx as nx

class VL2Switch(app_manager.RyuApp):
    def __init__(self, *args, **kwargs):
        super(VL2Switch, self).__init__(*args, **kwargs)
        self.net = nx.DiGraph()

    @set_ev_cls(event.EventSwitchEnter)
    def get_topology_data(self, ev):
        # 1. Add Switches (Nodes)
        switch_list = get_switch(self, None)
        switches = [switch.dp.id for switch in switch_list]
        self.net.add_nodes_from(switches)

        # 2. Add Links (Edges)
        # get_link returns links usually *after* LLDP exchange
        links_list = get_link(self, None)
        links = [(link.src.dpid, link.dst.dpid, {'port': link.src.port_no}) 
                 for link in links_list]
        self.net.add_edges_from(links)

        print(f"Nodes: {self.net.nodes()}")
        print(f"Edges: {self.net.edges()}")

    @set_ev_cls(event.EventLinkAdd)
    def link_add_handler(self, ev):
        link = ev.link
        # Add the specific link that was just detected
        self.net.add_edge(link.src.dpid, link.dst.dpid, port=link.src.port_no)
        print(f"Link Added: {link.src.dpid} -> {link.dst.dpid}")