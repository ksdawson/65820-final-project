import copy
import json
import random

from langgraph.managed.base import V

MAX_GPU_PER_HOST = 10

def create_server_dict(num_tor: int, num_host: int) -> dict:
    servers = {i:{j:MAX_GPU_PER_HOST for j in range(num_host)} for i in range(num_tor)}
    return servers

def start_process(agents: dict, servers: dict) -> dict:
    # Work on a copy so we can rollback if allocation fails
    servers_copy = copy.deepcopy(servers)
    node_map = {}

    for agent in agents:
        for node, gpu_count in agents[agent]:
            # Find hosts with enough available GPUs
            eligible_hosts = [
                (tor_id, host_id)
                for tor_id, hosts in servers_copy.items()
                for host_id, available in hosts.items()
                if available >= gpu_count
            ]
            
            if not eligible_hosts:
                print(f"Cannot allocate node {node} requiring {gpu_count} GPUs")
                return servers, None
            
            # Pick a random eligible host
            selected_tor, selected_host = random.choice(eligible_hosts)
            
            # Allocate the node to this host
            servers_copy[selected_tor][selected_host] -= gpu_count
            node_map[node] = (selected_tor, selected_host)  # Map node to its allocated host
    
    return servers_copy, node_map

def end_process(agents: dict, servers: dict, node_map: dict) -> dict:
    for agent in agents:
        for node, gpu_count in agents[agent]:
            selected_tor, selected_host = node_map[node]
            servers[selected_tor][selected_host] += gpu_count
    return servers

if __name__ == "__main__":
    servers = create_server_dict(4, 20)

    trace_path = "full_trace/full_coding_trace_0.json"
    with open(trace_path, "r") as f:
        trace = json.load(f)

    agents = trace[0]

    for i in range(500):
        servers, node_map = start_process(agents, servers)
        print(i)
    # servers, node_map = start_process(agents, servers)            
    
    # servers = end_process(agents, servers, node_map)
    
    # print(agents)

    # print(servers)

