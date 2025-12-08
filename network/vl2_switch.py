from ryu.base import app_manager
from ryu.controller import ofp_event
from ryu.controller.handler import CONFIG_DISPATCHER, MAIN_DISPATCHER, set_ev_cls
from ryu.ofproto import ofproto_v1_3
from ryu.topology import event
from ryu.lib.packet import packet, ethernet, ether_types
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
    # Hardware functions
    ################################################################

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
            self.logger.warning(f'Unknown switch DPID: {dpid}')
            return 'UNKNOWN'

    @set_ev_cls(ofp_event.EventOFPSwitchFeatures, CONFIG_DISPATCHER)
    def switch_features_handler(self, ev):
        # Add node
        datapath = ev.msg.datapath
        dpid = datapath.id
        self.network_graph.add_node(dpid)

        # Store switch type
        switch_type = self.classify_switch(dpid)
        if switch_type == 'INTERMEDIATE':
            self.inter_switches.add(dpid)
        elif switch_type == 'AGGREGATE':
            self.aggr_switches.add(dpid)
        elif switch_type == 'TOR':
            self.tor_switches.add(dpid)
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

    def install_path_flow(self, path, ev, dst_mac, src_mac=None):
        msg = ev.msg
        parser = msg.datapath.ofproto_parser
        
        # We iterate through the path to stitch the rules together
        # We stop before the last element because the last element is the Host MAC, not a switch
        for i in range(len(path) - 1):
            current_node = path[i]
            next_node = path[i+1]
            
            # Skip if current_node is not a switch (just in case)
            if isinstance(current_node, str): continue

            # Get the Output Port to the next hop
            out_port = self.network_graph[current_node][next_node]['port']
            
            # Get the Datapath object for this switch
            if current_node not in self.datapaths:
                self.logger.error(f'Cannot install flow: Datapath {current_node} not found!')
                continue
            dp = self.datapaths[current_node]
            
            # Create the Flow Match and Actions
            # Match: Destination MAC (standard L2 forwarding)
            match = parser.OFPMatch(eth_dst=dst_mac)
            actions = [parser.OFPActionOutput(out_port)]
            
            # Install the Flow
            self.add_flow(dp, 10, match, actions)
            
            # Optimization: If this is the switch holding the packet NOW, send it immediately
            if current_node == msg.datapath.id:
                self._send_packet(dp, out_port, packet.Packet(msg.data))

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
            # self.logger.info(f'LLDP packet received on {switch_type} switch on {dpid} (Port {in_port})')
            return
        
        # Get host info
        src_mac = eth.src
        dst_mac = eth.dst
        is_host = switch_type == 'TOR' and 1 <= in_port and in_port <= 20

        # Learn the src host location if we haven't seen it
        if is_host and src_mac not in self.network_graph:
            self.network_graph.add_node(src_mac, type='HOST')
            self.network_graph.add_edge(dpid, src_mac, port=in_port)
            self.network_graph.add_edge(src_mac, dpid) # Return path

            # Returns a list of host MAC addresses
            hosts = [n for n, d in self.network_graph.nodes(data=True) if d.get('type') == 'HOST']
            self.logger.info(f'{len(hosts)} hosts connected to graph')

            # self.logger.info(f'[HOST LEARNED] MAC: {src_mac} attached to Switch: {dpid} Port: {in_port}')
            # self.logger.info(f'Current Graph Nodes: {self.network_graph.nodes()}')

        # Switch logic
        if switch_type == 'TOR':
            if is_host:
                # From host
                # self.logger.info(f'Packet received from host on ToR switch on {dpid} (Port {in_port})')

                # Install flows for packet
                if self.network_graph.has_edge(dpid, dst_mac):
                    self.logger.info(' -> Local Switching (Intra-Rack)')
                    
                    # Path is simply [Current_Switch, Destination_MAC]
                    # This utilizes the edge we created in Host Learning which has the 'port'
                    path = [dpid, dst_mac]
                    self.install_path_flow(path, ev, dst_mac)
                else:
                    # self.logger.info(' -> Remote Destination (Inter-Rack)')
                    # TODO VL2 logic
                    return
            else:
                # From aggr
                # self.logger.info(f'Packet received from aggr on ToR switch on {dpid} (Port {in_port})')

                # Send to host
                if self.network_graph.has_edge(dpid, dst_mac):
                     # Deliver to the host
                     path = [dpid, dst_mac]
                     self.install_path_flow(path, ev, dst_mac)
                else:
                    #  self.logger.warning(f'Error: Packet at {dpid} for unknown local host {dst_mac}')
                    return
        elif switch_type == 'AGGREGATE':
            is_tor = 1 <= in_port and in_port <= 2
            if is_tor:
                # From ToR
                # self.logger.info(f'Packet received from ToR on aggr switch on {dpid} (Port {in_port})')

                # Send to inter
                return
            else:
                # From inter
                # self.logger.info(f'Packet received from inter on aggr switch on {dpid} (Port {in_port})')

                # Send to ToR
                return
        elif switch_type == 'INTERMEDIATE':
            # From aggr
            # self.logger.info(f'Packet received from aggr on inter switch on {dpid} (Port {in_port})')

            # Send to aggr
            return
        else:
            # self.logger.info(f'Packet received on {switch_type} switch on {dpid} (Port {in_port})')

            return