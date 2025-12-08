import json
import random
import os
from pathlib import Path


MSG_SIZE = 119435 # bytes
SECONDS_PER_TOKEN = 0.004  # 4 ms in seconds

AGENT_TRACE_DIR = "agent_trace"
FULL_TRACE_DIR = "full_trace"


def get_time_breakdown(size: int, generation_time: float) -> float:
    # size is in kilobytes, generation_time is in seconds
    tokens = size * 1000 / 4  # KB -> bytes -> tokens (4 bytes per token)
    decode = tokens * SECONDS_PER_TOKEN  # seconds
    prefill = generation_time - decode  # seconds
    return (prefill, decode)


def get_message_size_and_interval(input_size: int, output_size: int, generation_time: float, nodes: int) -> float:
    prefill_time, decode_time = get_time_breakdown(output_size, generation_time)

    prefill_interval = prefill_time / nodes
    decode_interval = SECONDS_PER_TOKEN

    prefill_size = MSG_SIZE * (input_size * 1000 / 4) * 2  # input KB -> bytes -> tokens
    decode_size = MSG_SIZE
    return {"prefill_interval": prefill_interval, "decode_interval": decode_interval, "prefill_size": prefill_size, "decode_size": decode_size}


def process_agent_trace(trace_path: str, output_path: str):
    """Process a single agent trace and generate a full trace file."""
    with open(trace_path, "r") as f:
        trace: list[dict] = json.load(f)

    senders = {}
    types = ["pipeline", "hybrid"]

    for entry in trace:
        if entry["sender"] not in senders and entry["sender"] != -1:
            senders[entry["sender"]] = random.choice(types)

    nodes = {}

    for sender, sendertype in senders.items():
        if sendertype == "pipeline":
            nodes[sender] = [(str(sender+i/10), 1) for i in range(8)]
        elif sendertype == "hybrid":
            nodes[sender] = [(str(sender+i/10), 2) for i in range(4)]
        elif sendertype == "tensor":
            nodes[sender] = [(str(sender+.1), 8)]

    full_trace = []
    input_size = 0
    cumulative_time = 0  # Track cumulative time in seconds

    for entry in trace:
        if entry["sender"] == -1 or -1 in entry["receiver"] or entry['llm_gen_time'] == 0 or input_size == 0:
            input_size = entry["data_size(kb)"]
            continue

        message_pattern = get_message_size_and_interval(input_size, entry["data_size(kb)"], entry['llm_gen_time'], len(nodes[entry["sender"]]))
        node_list = nodes[entry["sender"]]
        num_nodes = len(node_list)

        full_entry = {}
        local_time = 0  # Time within this entry (seconds)

        for i in range(num_nodes-1):
            local_time += message_pattern["prefill_interval"]
            full_entry = {
                "sender": node_list[i][0],
                "receiver": [node_list[i+1][0]],
                "time": cumulative_time + local_time,
                "size": message_pattern["prefill_size"],
            }
            full_trace.append(full_entry)

        while local_time + message_pattern["decode_interval"] < entry['llm_gen_time']:
            local_time += message_pattern["decode_interval"]
            for i in range(num_nodes-1):
                full_entry = {
                    "sender": node_list[i][0],
                    "receiver": [node_list[i+1][0]],
                    "time": cumulative_time + local_time,
                    "size": message_pattern["decode_size"],
                }
                full_trace.append(full_entry)

        full_trace.append({
            "sender": node_list[-1][0],
            "receiver": [str(r)+".0" for r in entry["receiver"]],
            "time": cumulative_time + entry['llm_gen_time'],
            "size": entry["data_size(kb)"]*1000,
        })

        cumulative_time += entry['llm_gen_time']  # Add this entry's duration to cumulative
        input_size = entry["data_size(kb)"]

    with open(output_path, "w") as f:
        json.dump(full_trace, f, indent=4)

    total_size = sum(entry["size"] for entry in full_trace)
    return len(full_trace), total_size


def main():
    """Process all agent traces and generate full traces."""
    # Get all JSON files in agent_trace directory
    agent_trace_files = sorted(Path(AGENT_TRACE_DIR).glob("*.json"))
    
    for trace_file in agent_trace_files:
        # Generate output filename: e.g., "explain_trace_0.json" -> "full_explain_trace_0.json"
        output_filename = f"full_{trace_file.name}"
        output_path = os.path.join(FULL_TRACE_DIR, output_filename)
        
        print(f"Processing: {trace_file.name}")
        
        try:
            num_entries, total_size = process_agent_trace(str(trace_file), output_path)
            print(f"  → Generated: {output_filename}")
            print(f"     Entries: {num_entries:,}, Size: {total_size/1e9:.2f} GB\n")
        except Exception as e:
            print(f"  ✗ Error: {e}\n")
    
    print("Done!")


if __name__ == "__main__":
    main()