from ryu.controller import ofp_event
from ryu.controller.handler import MAIN_DISPATCHER, set_ev_cls
from ryu.lib.packet import packet, icmp
from ryu.app.simple_switch import SimpleSwitch

class VL2Switch(SimpleSwitch):
    def __init__(self, *args, **kwargs):
        super(VL2Switch, self).__init__(*args, **kwargs)
        self.logger.info("VL2Switch: Starting up with inherited logic...")

    # Override the packet in handler
    # NOTE: We must re-declare the decorator to ensure this method captures the event
    @set_ev_cls(ofp_event.EventOFPPacketIn, MAIN_DISPATCHER)
    def _packet_in_handler(self, ev):
        msg = ev.msg
        
        # --- CUSTOM LOGIC START ---
        # Let's peek at the packet to see if it's ICMP
        pkt = packet.Packet(msg.data)
        icmp_pkt = pkt.get_protocol(icmp.icmp)

        if icmp_pkt:
            self.logger.info("CustomSwitch: BLOCKING ICMP packet from host.")
            # We simply return, effectively dropping the packet 
            # because no flow is installed and no PacketOut is sent.
            return
        # --- CUSTOM LOGIC END ---

        # Fallback to the standard learning switch logic
        super(VL2Switch, self)._packet_in_handler(ev)