from ryu.base import app_manager
from ryu.controller import ofp_event
from ryu.controller.handler import CONFIG_DISPATCHER, MAIN_DISPATCHER, set_ev_cls
from ryu.ofproto import ofproto_v1_3
from ryu.topology import event
import networkx as nx
import random

class VL2Switch(app_manager.RyuApp):
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]

    def __init__(self, *args, **kwargs):
        # Setup
        super(VL2Switch, self).__init__(*args, **kwargs)

        # Network topology
        self.network_graph = nx.DiGraph()
        self.hosts = set()
        self.tor_switches = set()
        self.aggr_switches = set()
        self.inter_switches = set()

    ################################################################
    # Topology learning functions
    ################################################################

    def classify_switch(self, dpid):
        if 1000 <= dpid < 2000:
            return 'INTERMEDIATE'
        elif 2000 <= dpid < 3000:
            return 'AGGREGATE'
        elif 3000 <= dpid < 4000:
            return 'TOR'
        else:
            self.logger.warning(f'Unknown switch DPID: {dpid}')
            return 'UNKNOWN'

    @set_ev_cls(ofp_event.EventOFPSwitchFeatures, CONFIG_DISPATCHER)
    def switch_features_handler(self, ev):
        dpid = ev.msg.datapath.id
        # Add node
        self.network_graph.add_node(dpid)
        # Get switch type
        switch_type = self.classify_switch(dpid)
        self.logger.info(f'{switch_type} switch connected: {dpid}')
        if switch_type == 'INTERMEDIATE':
            self.inter_switches.add(dpid)
        elif switch_type == 'AGGREGATE':
            self.aggr_switches.add(dpid)
        elif switch_type == 'TOR':
            self.tor_switches.add(dpid)
        
    @set_ev_cls(event.EventLinkAdd)
    def link_add_handler(self, ev):
        src = ev.link.src 
        dst = ev.link.dst
        self.logger.info(f'Link discovered: {src} to {dst}')
        # Add edge using .dpid for nodes and .port_no for the attribute
        self.network_graph.add_edge(src.dpid, dst.dpid, port=src.port_no)

    @set_ev_cls(event.EventLinkDelete)
    def link_delete_handler(self, ev):
        src = ev.link.src
        dst = ev.link.dst
        self.logger.info(f'Link removed: {src} to {dst}')
        try:
            self.network_graph.remove_edge(src.dpid, dst.dpid)
        except nx.NetworkXError:
            # Edge was already removed
            pass

    @set_ev_cls(event.EventSwitchLeave)
    def switch_leave_handler(self, ev):
        dpid = ev.switch.dp.id
        self.logger.info(f'Switch disconnected: {dpid}')
        try:
            self.network_graph.remove_node(dpid)
        except nx.NetworkXError:
            # Node was already removed
            pass

    ################################################################
    # Routing functions
    ################################################################

    