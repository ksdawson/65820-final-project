from mininet.net import Mininet
from mininet.log import setLogLevel
from vl2 import VL2Topo

def send_bytes_dst_to_src(src, dst, byte_count):
    # Start iperf server on the 'src' (Receiver) in the background
    # -s: server mode
    # -p: port 5001
    # &: run in background
    src.cmd('iperf -s -p 5001 &')

    # Run iperf client on the 'dst' (Sender)
    # -c: connect to client
    # -n: number of bytes to send (e.g., 10M, 1000)
    # -p: port 5001
    output = dst.cmd(f'iperf -c {src.IP()} -p 5001 -n {byte_count}')
    
    # Clean up the server process on 'src'
    # 'kill %iperf' kills the job started in the background shell
    src.cmd('kill %iperf')
    
    return output

def run():
    # Initialize Network
    topo = VL2Topo(D_A=2, D_I=2)
    net = Mininet(topo=topo)
    net.start()

    # Get hosts
    h0 = net.get('h0') 
    h1 = net.get('h1')

    # Example: Send 50 Megabytes from h1 (Dest) to h0 (Src)
    output = send_bytes_dst_to_src(src=h0, dst=h1, byte_count='50M')
    with open('vl2_output.txt', 'w') as file:
        file.write(output)

    # Stop network
    net.stop()

if __name__ == '__main__':
    setLogLevel('info')
    run()