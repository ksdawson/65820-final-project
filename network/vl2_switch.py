import random
from ryu.controller import ofp_event
from ryu.controller.handler import MAIN_DISPATCHER, set_ev_cls
from ryu.lib.packet import packet, ethernet, ether_types, ipv4
from ryu.app.simple_switch_13 import SimpleSwitch13
import logging

class VL2Controller(SimpleSwitch13):
    def __init__(self, *args, **kwargs):
        # Setup
        super(VL2Controller, self).__init__(*args, **kwargs)
        self.logger.info("VL2Controller: Active with VLB Logic")
        self.logger.setLevel(logging.WARNING) # don't print everything

        # In VL2, we need to know which ports point "UP" to the spine
        # and which point "DOWN" to hosts.
        # For simulation simplicity, let's assume specific ports are uplinks.
        self.UPLINK_PORTS = [1, 2] # Example: Ports connecting ToR to Aggr

    @set_ev_cls(ofp_event.EventOFPPacketIn, MAIN_DISPATCHER)
    def _packet_in_handler(self, ev):
        msg = ev.msg
        datapath = msg.datapath
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        in_port = msg.match['in_port']

        pkt = packet.Packet(msg.data)
        eth = pkt.get_protocols(ethernet.ethernet)[0]
        
        # ALLOW ARP TO FUNCTION NORMALLY (Discovery)
        # We let the parent class handle ARP so hosts can find MAC addresses.
        if eth.ethertype == ether_types.ETH_TYPE_ARP:
            # WARNING: In a real VL2 topology with loops, this needs Spanning Tree 
            # or an ARP Proxy. For now, we trust Mininet's TTL or linear topology behavior.
            super(VL2Controller, self)._packet_in_handler(ev)
            return

        # INTERCEPT IP TRAFFIC (The VLB Logic)
        if eth.ethertype == ether_types.ETH_TYPE_IP:
            ip_pkt = pkt.get_protocol(ipv4.ipv4)[0]
            src_ip = ip_pkt.src
            dst_ip = ip_pkt.dst

            # Logic: If we are a ToR and receiving from a Host, 
            # pick a RANDOM uplink (VLB).
            
            # Simple heuristic: If destination is NOT known in our local L2 table,
            # it means the destination is likely across the fabric.
            if eth.dst not in self.mac_to_port.get(datapath.id, {}):
                
                # Pick a random uplink to spread traffic
                out_port = random.choice(self.UPLINK_PORTS)
                
                self.logger.info(f"VLB: Routing flow {src_ip}->{dst_ip} on Switch {datapath.id} via Port {out_port}")

                actions = [parser.OFPActionOutput(out_port)]

                # Install a flow for this specific stream so packets stick to this path
                # We match on IP src/dst to ensure flow consistency
                match = parser.OFPMatch(in_port=in_port, 
                                      eth_type=ether_types.ETH_TYPE_IP,
                                      ipv4_src=src_ip, 
                                      ipv4_dst=dst_ip)
                
                self.add_flow(datapath, 10, match, actions, msg.buffer_id)
                
                # We handled the packet, so we return here. 
                # We DO NOT call super() because we don't want standard L2 learning 
                # to override our random choice.
                if msg.buffer_id == ofproto.OFP_NO_BUFFER:
                    data = msg.data
                    out = parser.OFPPacketOut(datapath=datapath, buffer_id=msg.buffer_id,
                                              in_port=in_port, actions=actions, data=data)
                    datapath.send_msg(out)
                return

        # Fallback for local traffic (Host A to Host B on same switch)
        super(VL2Controller, self)._packet_in_handler(ev)