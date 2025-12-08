from ryu.base import app_manager
from ryu.controller import ofp_event
from ryu.controller.handler import CONFIG_DISPATCHER, MAIN_DISPATCHER, set_ev_cls
from ryu.ofproto import ofproto_v1_3
from ryu.topology import event
import networkx as nx

class VL2Switch(app_manager.RyuApp):
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]

    def __init__(self, *args, **kwargs):
        super(VL2Switch, self).__init__(*args, **kwargs)
        self.network_graph = nx.DiGraph()

    ################################################################
    # Topology learning functions
    ################################################################

    @set_ev_cls(ofp_event.EventOFPSwitchFeatures, CONFIG_DISPATCHER)
    def switch_features_handler(self, ev):
        dpid = ev.msg.datapath.id
        self.logger.info(f'Switch connected: {dpid}')
        # Add node
        self.network_graph.add_node(dpid)
        
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