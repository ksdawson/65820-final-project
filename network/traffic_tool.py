import socket
import argparse
import threading
import json
import time

def run_server(port):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind(('', port))
    s.listen(10) # Backlog for bursty concurrent connections
    
    while True:
        try:
            conn, _ = s.accept()
            # Spin off a thread so we don't block other incoming connections
            t = threading.Thread(target=drain_socket, args=(conn,))
            t.daemon = True
            t.start()
        except:
            pass

def drain_socket(conn):
    try:
        while True:
            data = conn.recv(4096)
            if not data: break
    finally:
        conn.close()

def run_client(target_ip, port, num_bytes):
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        
        # Measure Start Time
        start_time = time.time()
        
        s.connect((target_ip, port))
        
        chunk_size = 4096
        chunk = b'x' * chunk_size
        sent = 0
        
        while sent < num_bytes:
            remaining = num_bytes - sent
            to_send = min(chunk_size, remaining)
            s.sendall(chunk[:to_send])
            sent += to_send
            
        s.close()
        
        # Measure End Time
        end_time = time.time()
        duration = end_time - start_time
        
        # Output Metrics as JSON
        # FLUSH is critical so it appears in the log file immediately
        print(json.dumps({
            'event': 'flow_complete',
            'target_ip': target_ip,
            'bytes': num_bytes,
            'duration_sec': duration,
            'throughput_mbps': (num_bytes * 8) / (duration * 1e6) if duration > 0 else 0
        }), flush=True)

    except Exception as e:
        # Log errors too
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