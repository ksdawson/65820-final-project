from ryu.base import app_manager
from ryu.controller import ofp_event
from ryu.controller.handler import CONFIG_DISPATCHER, MAIN_DISPATCHER, set_ev_cls
from ryu.ofproto import ofproto_v1_3
from ryu.topology import event
from ryu.lib.packet import packet, ethernet, ether_types, ipv4
import networkx as nx
import random

LOGGING = False

class VL2Switch(app_manager.RyuApp):
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]

    def __init__(self, *args, **kwargs):
        # Setup
        super(VL2Switch, self).__init__(*args, **kwargs)

        # Network topology
        self.network_graph = nx.DiGraph()
        self.datapaths = {}
        self.hosts = set()
        self.tor_switches = set()
        self.aggr_switches = set()
        self.inter_switches = set()

    ################################################################
    # Helper functions
    ################################################################

    def get_hosts(self):
        hosts = [n for n, d in self.network_graph.nodes(data=True) if d.get('type') == 'HOST']
        return hosts

    ################################################################
    # Hardware functions
    ################################################################

    def send_packet(self, datapath, port, pkt):
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        pkt.serialize()
        data = pkt.data
        actions = [parser.OFPActionOutput(port)]
        out = parser.OFPPacketOut(datapath=datapath, buffer_id=ofproto.OFP_NO_BUFFER,
            in_port=ofproto.OFPP_CONTROLLER, actions=actions, data=data)
        datapath.send_msg(out)

    def install_path_flow(self, path, ev, dst_mac, src_mac=None, dscp=None, queue_id=0):
        msg = ev.msg
        parser = msg.datapath.ofproto_parser
        
        # Iterate through the path to stitch the rules together
        for i in range(len(path) - 1):
            current_node = path[i]
            next_node = path[i+1]
            
            if isinstance(current_node, str): continue

            # Get the Output Port to the next hop
            out_port = self.network_graph[current_node][next_node]['port']
            
            if current_node not in self.datapaths:
                continue
            dp = self.datapaths[current_node]
            
            # ### NEW: Create specific Match and Queue Action
            # If we have a DSCP value, we match on it to differentiate traffic types
            if dscp is not None:
                match = parser.OFPMatch(eth_dst=dst_mac, eth_type=0x0800, ip_dscp=dscp)
            else:
                match = parser.OFPMatch(eth_dst=dst_mac)
            
            # Action: Set Queue ID first, then Output
            actions = [
                parser.OFPActionSetQueue(queue_id),
                parser.OFPActionOutput(out_port)
            ]
            
            # Install the Flow (Higher priority 20 for specific DSCP flows)
            priority = 20 if dscp is not None else 10
            self.add_flow(dp, priority, match, actions)
            
            # Forward packet if this is the current switch
            if current_node == msg.datapath.id:
                self.send_packet(dp, out_port, packet.Packet(msg.data))

    def add_flow(self, datapath, priority, match, actions, buffer_id=None):
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        inst = [parser.OFPInstructionActions(ofproto.OFPIT_APPLY_ACTIONS, actions)]
        if buffer_id:
            mod = parser.OFPFlowMod(datapath=datapath, buffer_id=buffer_id,
                                    priority=priority, match=match,
                                    instructions=inst)
        else:
            mod = parser.OFPFlowMod(datapath=datapath, priority=priority,
                                    match=match, instructions=inst)
        datapath.send_msg(mod)

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
            if LOGGING:
                self.logger.warning(f'Unknown switch DPID: {dpid}')
            return 'UNKNOWN'

    @set_ev_cls(ofp_event.EventOFPSwitchFeatures, CONFIG_DISPATCHER)
    def switch_features_handler(self, ev):
        # Add node
        datapath = ev.msg.datapath
        dpid = datapath.id
        self.network_graph.add_node(dpid)
        self.datapaths[dpid] = datapath

        # Store switch type
        switch_type = self.classify_switch(dpid)
        if switch_type == 'INTERMEDIATE':
            self.inter_switches.add(dpid)
        elif switch_type == 'AGGREGATE':
            self.aggr_switches.add(dpid)
        elif switch_type == 'TOR':
            self.tor_switches.add(dpid)
        if LOGGING:
            self.logger.info(f'{switch_type} switch connected: {dpid}')
        
        # Install Table-Miss Flow Entry
        # Priority 0 (Lowest) -> Match Everything -> Send to Controller
        ofproto = datapath.ofproto
        match = datapath.ofproto_parser.OFPMatch()
        actions = [datapath.ofproto_parser.OFPActionOutput(ofproto.OFPP_CONTROLLER,
                                          ofproto.OFPCML_NO_BUFFER)]
        self.add_flow(datapath, 0, match, actions)
        
    @set_ev_cls(event.EventLinkAdd)
    def link_add_handler(self, ev):
        src = ev.link.src 
        dst = ev.link.dst
        if LOGGING:
            self.logger.info(f'Link discovered: {src} to {dst}')
        # Add edge using .dpid for nodes and .port_no for the attribute
        self.network_graph.add_edge(src.dpid, dst.dpid, port=src.port_no)

    @set_ev_cls(event.EventLinkDelete)
    def link_delete_handler(self, ev):
        src = ev.link.src
        dst = ev.link.dst
        if LOGGING:
            self.logger.info(f'Link removed: {src} to {dst}')
        try:
            self.network_graph.remove_edge(src.dpid, dst.dpid)
        except nx.NetworkXError:
            # Edge was already removed
            pass

    @set_ev_cls(event.EventSwitchLeave)
    def switch_leave_handler(self, ev):
        dpid = ev.switch.dp.id
        if LOGGING:
            self.logger.info(f'Switch disconnected: {dpid}')
        if dpid in self.datapaths:
            del self.datapaths[dpid]
        try:
            self.network_graph.remove_node(dpid)
        except nx.NetworkXError:
            # Node was already removed
            pass

    ################################################################
    # VL2 functions
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
            if LOGGING:
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

    def handle_broadcast(self, dpid, in_port, msg):
        # We must flood this packet to ALL ToR switches
        for tor_dpid in self.tor_switches:
            if tor_dpid not in self.datapaths:
                continue
            tor_dp = self.datapaths[tor_dpid]
            tor_parser = tor_dp.ofproto_parser
            actions = []
            # Flood ports 1-20 (Host facing ports)
            for port in range(1, 21):
                # CRITICAL: If this is the source switch, 
                # don't send it back to the host that sent it!
                if tor_dpid == dpid and port == in_port:
                    continue
                actions.append(tor_parser.OFPActionOutput(port))
            # Send the packet out
            if actions:
                out = tor_parser.OFPPacketOut(
                    datapath=tor_dp,
                    buffer_id=tor_dp.ofproto.OFP_NO_BUFFER,
                    in_port=tor_dp.ofproto.OFPP_CONTROLLER,
                    actions=actions,
                    data=msg.data)
                tor_dp.send_msg(out)
        return

    @set_ev_cls(ofp_event.EventOFPPacketIn, MAIN_DISPATCHER)
    def _packet_in_handler(self, ev):
        # Algo: When flow first enters the network (at the src ToR) calculate the entire path
        # and install flow rules on every switch along the path. Switches in the middle
        # should never trigger then.

        # Get msg info
        msg = ev.msg
        datapath = msg.datapath
        ofproto = datapath.ofproto

        # Get pkt info
        pkt = packet.Packet(msg.data)
        eth = pkt.get_protocols(ethernet.ethernet)[0]

        # Get switch info
        dpid = datapath.id
        in_port = msg.match['in_port']
        switch_type = self.classify_switch(dpid)

        # Ignore LLDP packets as they're used for topology learning
        if eth.ethertype == ether_types.ETH_TYPE_LLDP:
            if LOGGING:
                self.logger.info(f'LLDP packet received on {switch_type} switch on {dpid} (Port {in_port})')
            return
        
        # Get host info
        src_mac = eth.src
        dst_mac = eth.dst
        is_host = switch_type == 'TOR' and 1 <= in_port and in_port <= 20

        parser = datapath.ofproto_parser
        # Match for Distributed Inference (Intra-group)
        match_intra = parser.OFPMatch(eth_type=0x0800, ip_dscp=4) 
        # Match for Agent-Agent (Inter-group)
        match_inter = parser.OFPMatch(eth_type=0x0800, ip_dscp=8)

        # Learn the src host location if we haven't seen it
        if is_host:
            # Ensure the Node exists
            if src_mac not in self.network_graph:
                self.network_graph.add_node(src_mac, type='HOST')
            # Check if edge exists and points to the correct port
            update_edge = True
            if self.network_graph.has_edge(dpid, src_mac):
                if self.network_graph[dpid][src_mac].get('port') == in_port:
                    update_edge = False
            if update_edge:
                self.network_graph.add_edge(dpid, src_mac, port=in_port)
                self.network_graph.add_edge(src_mac, dpid) # Return path
                # Debug logging
                if LOGGING:
                    self.logger.info(f"Host Edge Updated: {src_mac} on Port {in_port}. Total Hosts: {len(self.get_hosts())}")

        # Default: Queue 0 (Low Priority)
        queue_id = 0
        dscp_val = None
        # Check for IPv4 and extract DSCP
        if eth.ethertype == ether_types.ETH_TYPE_IP:
            ip_pkt = pkt.get_protocol(ipv4.ipv4)
            if ip_pkt:
                dscp_val = ip_pkt.tos >> 2  # Extract DSCP (top 6 bits)
                
                # If Agent-Agent traffic (DSCP 8 / ToS 32), use Queue 1 (High Priority)
                if dscp_val == 8:
                    queue_id = 1
                # If Intra-group (DSCP 4), keep default Queue 0

        # Switch logic
        if switch_type == 'TOR':
            if is_host:
                # From host
                if LOGGING:
                    self.logger.info(f'Packet received from host on ToR switch on {dpid} (Port {in_port})')

                # If host doesn't know its dst host's mac address it sends a broadcast
                # So we should return the mac address to it
                if dst_mac == 'ff:ff:ff:ff:ff:ff':
                    self.handle_broadcast(dpid, in_port, msg)

                # Install flows for packet
                if self.network_graph.has_edge(dpid, dst_mac):
                    # Path is simply [Current_Switch, Destination_MAC]
                    # This utilizes the edge we created in Host Learning which has the 'port'
                    path = [dpid, dst_mac]
                    self.install_path_flow(path, ev, dst_mac, dscp=dscp_val, queue_id=queue_id)
                    if LOGGING:
                        self.logger.info(' -> Local Switching (Intra-Rack)')
                else:
                    # VL2 logic
                    path = self.get_vl2_path(dpid, dst_mac)
                    if path:
                        self.install_path_flow(path, ev, dst_mac, dscp=dscp_val, queue_id=queue_id)
                        if LOGGING:
                            self.logger.info(' -> Remote Destination (Inter-Rack)')
                    else:
                        # Try to learn mac address
                        self.handle_broadcast(dpid, in_port, msg)
            else:
                # From aggr
                if LOGGING:
                    self.logger.warning(f'Packet received from aggr on ToR switch on {dpid} (Port {in_port})')
        elif switch_type == 'AGGREGATE':
            is_tor = 1 <= in_port and in_port <= 2
            if is_tor:
                # From ToR
                if LOGGING:
                    self.logger.warning(f'Packet received from ToR on aggr switch on {dpid} (Port {in_port})')
            else:
                # From inter
                if LOGGING:
                    self.logger.warning(f'Packet received from inter on aggr switch on {dpid} (Port {in_port})')
        elif switch_type == 'INTERMEDIATE':
            # From aggr
            if LOGGING:
                self.logger.warning(f'Packet received from aggr on inter switch on {dpid} (Port {in_port})')
        else:
            if LOGGING:
                self.logger.warning(f'Packet received on {switch_type} switch on {dpid} (Port {in_port})')