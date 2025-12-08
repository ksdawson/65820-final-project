from ryu.base import app_manager
from ryu.controller import ofp_event
from ryu.controller.handler import CONFIG_DISPATCHER, MAIN_DISPATCHER, set_ev_cls
from ryu.ofproto import ofproto_v1_3
from ryu.topology import event
from ryu.topology.api import get_switch, get_link

class SanityCheck(app_manager.RyuApp):
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]

    def __init__(self, *args, **kwargs):
        super(SanityCheck, self).__init__(*args, **kwargs)

    # 1. BASIC HANDSHAKE (If this doesn't fire, Mininet isn't connected)
    @set_ev_cls(ofp_event.EventOFPSwitchFeatures, CONFIG_DISPATCHER)
    def switch_features_handler(self, ev):
        dpid = ev.msg.datapath.id
        self.logger.info(f"✅ SWITCH CONNECTED! DPID: {dpid}")

    # 2. TOPOLOGY DISCOVERY (If this doesn't fire, --observe-links is wrong)
    @set_ev_cls(event.EventLinkAdd)
    def link_add_handler(self, ev):
        src = ev.link.src.dpid
        dst = ev.link.dst.dpid
        self.logger.info(f"🔗 LINK DISCOVERED: {src} <--> {dst}")