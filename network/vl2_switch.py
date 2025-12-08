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

    def get_random_intermediate_node(self):
        if not self.inter_switches:
            return None
        return random.choice(tuple(self.inter_switches))

    def get_ecmp_path(self, src_dpid, dst_dpid):
        # Uses hops as the dist metric
        try:
            paths = list(nx.all_shortest_paths(self.network_graph, src_dpid, dst_dpid))
            return random.choice(paths)
        except (nx.NetworkXNoPath, nx.NodeNotFound):
            return None

    def get_vl2_path(self, src_dpid, dst_dpid):
        # Pick a random intermediate node (Valiant Load Balancing)
        intermediate_node = self.get_random_intermediate_node()
        if not intermediate_node:
            self.logger.warning('No intermediate nodes available. Falling back to direct shortest path.')
            return self.get_ecmp_path(src_dpid, dst_dpid)

        # Get paths from src to inter to dst using ECMP
        # Path A: Source -> Intermediate
        path_a = self.get_ecmp_path(src_dpid, intermediate_node)
        # Path B: Intermediate -> Destination
        path_b = self.get_ecmp_path(intermediate_node, dst_dpid)
        if not path_a or not path_b:
            return None

        # Combine paths
        full_path = path_a[:-1] + path_b
        return full_path
    
    ################################################################
    # Packet handling
    ################################################################

    def is_host_port(self, dpid, port_no):
        # 1. First, check if I am a ToR. If I'm an Aggregate or Spine, 
        # I physically cannot be connected to a host in VL2.
        if self.classify_switch(dpid) != 'TOR':
            return False

        # 2. Check if this port connects to another switch in our graph
        # We look at all outgoing edges from this switch 'dpid'
        if dpid in self.network_graph:
            for neighbor in self.network_graph[dpid]:
                # Check the 'port' attribute of the edge (dpid -> neighbor)
                # Note: We access the edge data using self.network_graph[u][v]
                edge_data = self.network_graph[dpid][neighbor]
                
                if edge_data.get('port') == port_no:
                    # We found a match! This port connects to 'neighbor' (a switch).
                    return False
        
        # 3. If we didn't find a switch neighbor on this port, it's a host.
        return True

    @set_ev_cls(ofp_event.EventOFPPacketIn, MAIN_DISPATCHER)
    def _packet_in_handler(self, ev):
        msg = ev.msg
        datapath = msg.datapath
        dpid = datapath.id
        in_port = msg.match['in_port']

        # CHECK: Where am I?
        if self.is_host_port(dpid, in_port):
            self.logger.info(f"Packet received from HOST on ToR {dpid} (Port {in_port})")
        else:
            self.logger.info(f"Packet received from SWITCH on {dpid} (Port {in_port})")