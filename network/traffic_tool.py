import socket
import argparse
import threading
import json
import time
import sys

# Define a simple protocol constants
ACK_BYTE = b'\xACK' 

def run_server(port):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind(('', port))
    s.listen(10)
    
    while True:
        try:
            conn, _ = s.accept()
            t = threading.Thread(target=handle_connection, args=(conn,))
            t.daemon = True
            t.start()
        except Exception as e:
            pass

def handle_connection(conn):
    try:
        while True:
            # Receive data until the sender shuts down their write side
            data = conn.recv(4096)
            if not data: 
                break
        
        # KEY CHANGE: Send an application-level ACK to signal receipt
        try:
            conn.sendall(ACK_BYTE)
        except:
            pass
            
    finally:
        conn.close()

def run_client(target_ip, port, num_bytes):
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        
        start_time = time.time()
        s.connect((target_ip, port))
        
        chunk_size = 4096
        # Pre-allocate buffer to avoid allocation overhead during timing
        chunk = b'x' * chunk_size 
        sent = 0
        
        while sent < num_bytes:
            remaining = num_bytes - sent
            to_send = min(chunk_size, remaining)
            # Slicing bytes creates copies; usually negligible but worth noting for massive throughput
            s.sendall(chunk[:to_send]) 
            sent += to_send
        
        # KEY CHANGE: Shutdown write side to signal server we are done sending
        s.shutdown(socket.SHUT_WR)
        
        # Block until we receive the ACK from the server
        s.recv(1)
        
        end_time = time.time()
        s.close()
        
        duration = end_time - start_time
        
        # Avoid division by zero
        throughput = (num_bytes * 8) / (duration * 1e6) if duration > 1e-9 else 0.0

        print(json.dumps({
            'event': 'flow_complete',
            'target_ip': target_ip,
            'bytes': num_bytes,
            'duration_sec': duration,
            'throughput_mbps': throughput
        }), flush=True)

    except Exception as e:
        print(json.dumps({
            'event': 'error',
            'error': str(e)
        }), flush=True)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-m', '--mode', choices=['server', 'client'], required=True)
    parser.add_argument('-t', '--target', help='Target IP')
    parser.add_argument('-p', '--port', type=int, default=8000)
    parser.add_argument('-b', '--bytes', type=int, default=0)
    
    args = parser.parse_args()
    
    if args.mode == 'server':
        run_server(args.port)
    else:
        run_client(args.target, args.port, args.bytes)