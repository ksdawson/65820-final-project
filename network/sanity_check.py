from ryu.lib.packet import packet, ethernet
from ryu.app import simple_switch_13
from ryu.controller import ofp_event
from ryu.controller.handler import CONFIG_DISPATCHER, MAIN_DISPATCHER
from ryu.controller.handler import set_ev_cls
from ryu.ofproto import ofproto_v1_3
from ryu.lib.dpid import dpid_to_str
from ryu.lib.packet import ether_types
from ryu.topology import event, switches

class MyRyuApp(simple_switch_13.SimpleSwitch13):
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]

    def __init__(self, *args, **kwargs):
        super(MyRyuApp, self).__init__(*args, **kwargs)
        self.mac_to_port = {}
        self.topology_api = self.get_topology_api()

    @set_ev_cls(event.EventSwitchEnter, MAIN_DISPATCHER)
    def get_topology_data(self, ev):
        # Get all switches
        switches = self.topology_api.get_all_switches(None)
        self.logger.info("Switches: %s", switches)

        # Get all links
        links = self.topology_api.get_all_links(None)
        self.logger.info("Links: %s", links)