import random
from collections import deque

from ryu.ofproto import ether_types
from ryu.lib.packet import packet, ethernet, ipv4

from ryu.controller import ofp_event
from ryu.controller.handler import set_ev_cls, CONFIG_DISPATCHER, MAIN_DISPATCHER

from ryu.app.simple_switch_13 import SimpleSwitch13  # assuming your base class is here

from ryu.topology import event
from ryu.topology.api import get_all_link


class VLB_SimpleSwitch13(SimpleSwitch13):
    """
    Extends SimpleSwitch13 with:
      - VLB at ToR switches for new host flows
      - Random intermediate selection per new flow
      - ECMP to distribute traffic along multiple uplinks to the intermediate
    """

    def __init__(self, *args, **kwargs):
        super(VLB_SimpleSwitch13, self).__init__(*args, **kwargs)

        # Track the topology
        self.adjacency = {}       # adjacency[src_dpid][dst_dpid] = port_on_src_to_dst
        self.datapaths = {}       # dpid -> datapath object
        self.intermediates = set()  # dpids of intermediate switches
        self.tors = set()           # dpids of ToR switches

        # Optional: mapping host MAC -> ToR dpid
        self.host_to_tor = {}

    # ----- Track switches as they connect -----
    @set_ev_cls(ofp_event.EventOFPStateChange, [MAIN_DISPATCHER, CONFIG_DISPATCHER])
    def _state_change_handler(self, ev):
        dp = ev.datapath
        if ev.state == MAIN_DISPATCHER:
            self.datapaths[dp.id] = dp
        else:
            self.datapaths.pop(dp.id, None)

    # ----- Track links to build adjacency -----
    @set_ev_cls(event.EventLinkAdd)
    def link_add_handler(self, ev):
        links = get_all_link(self)
        self.adjacency = {}
        for l in links:
            src = l.src.dpid
            dst = l.dst.dpid
            src_port = l.src.port_no
            self.adjacency.setdefault(src, {})[dst] = src_port

    # ----- Utility: BFS shortest path -----
    def shortest_path(self, src, dst):
        q = deque([src])
        prev = {src: None}
        while q:
            u = q.popleft()
            if u == dst:
                break
            for v in self.adjacency.get(u, {}):
                if v not in prev:
                    prev[v] = u
                    q.append(v)
        if dst not in prev:
            return None
        # reconstruct path
        path = []
        cur = dst
        while cur is not None:
            path.append(cur)
            cur = prev[cur]
        path.reverse()
        return path

    # ----- Helper to create an ECMP group on a switch -----
    def add_ecmp_group(self, datapath, group_id, port_list):
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser

        buckets = []
        for p in port_list:
            actions = [parser.OFPActionOutput(p)]
            bucket = parser.OFPBucket(actions=actions)
            buckets.append(bucket)

        req = parser.OFPGroupMod(datapath, ofproto.OFPGC_ADD,
                                 ofproto.OFPGT_SELECT, group_id, buckets)
        datapath.send_msg(req)

    # ----- Override PacketIn handler -----
    @set_ev_cls(ofp_event.EventOFPPacketIn, MAIN_DISPATCHER)
    def _packet_in_handler(self, ev):
        msg = ev.msg
        datapath = msg.datapath
        dpid = datapath.id
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        in_port = msg.match['in_port']

        pkt = packet.Packet(msg.data)
        eth = pkt.get_protocols(ethernet.ethernet)[0]

        if eth.ethertype == ether_types.ETH_TYPE_LLDP:
            return  # ignore LLDP

        dst = eth.dst
        src = eth.src

        # Learn MAC → port
        self.mac_to_port.setdefault(dpid, {})
        self.mac_to_port[dpid][src] = in_port

        # Track host → ToR
        if dpid in self.tors:
            self.host_to_tor[src] = dpid

        # ----- Check if this packet is from a host at a ToR -----
        if dpid in self.tors and src in self.host_to_tor:
            # Pick a random intermediate
            if not self.intermediates:
                out_port = ofproto.OFPP_FLOOD
            else:
                chosen_int = random.choice(list(self.intermediates))
                path = self.shortest_path(dpid, chosen_int)
                if path is None or len(path) < 2:
                    out_port = ofproto.OFPP_FLOOD
                else:
                    # ECMP: find all next hops to chosen intermediate (here simplified as all neighbors closer to intermediate)
                    neighbors = [n for n in self.adjacency[dpid] if n in path]
                    ports = [self.adjacency[dpid][n] for n in neighbors]
                    if not ports:
                        out_port = ofproto.OFPP_FLOOD
                    else:
                        # Create ECMP group id based on chosen intermediate
                        group_id = int(chosen_int)  # simple mapping
                        self.add_ecmp_group(datapath, group_id, ports)
                        actions = [parser.OFPActionGroup(group_id)]

                        # Install flow for this new flow
                        match = parser.OFPMatch(in_port=in_port, eth_src=src, eth_dst=dst)
                        self.add_flow(datapath, 10, match, actions)

                        # Send PacketOut
                        data = None
                        if msg.buffer_id == ofproto.OFP_NO_BUFFER:
                            data = msg.data
                        out = parser.OFPPacketOut(datapath=datapath, buffer_id=msg.buffer_id,
                                                  in_port=in_port, actions=actions, data=data)
                        datapath.send_msg(out)
                        return  # handled, do not continue to normal SimpleSwitch behavior

        # ----- Default SimpleSwitch behavior -----
        if dst in self.mac_to_port[dpid]:
            out_port = self.mac_to_port[dpid][dst]
        else:
            out_port = ofproto.OFPP_FLOOD

        actions = [parser.OFPActionOutput(out_port)]

        # Install flow for known dst
        if out_port != ofproto.OFPP_FLOOD:
            match = parser.OFPMatch(in_port=in_port, eth_src=src, eth_dst=dst)
            if msg.buffer_id != ofproto.OFP_NO_BUFFER:
                self.add_flow(datapath, 1, match, actions, msg.buffer_id)
                return
            else:
                self.add_flow(datapath, 1, match, actions)

        data = None
        if msg.buffer_id == ofproto.OFP_NO_BUFFER:
            data = msg.data

        out = parser.OFPPacketOut(datapath=datapath, buffer_id=msg.buffer_id,
                                  in_port=in_port, actions=actions, data=data)
        datapath.send_msg(out)
