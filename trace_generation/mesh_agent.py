import os
import json
import time
import asyncio
import random
from datetime import datetime
from typing import List, Dict, Optional, Set
from dataclasses import dataclass, field
from dotenv import load_dotenv
from langchain_openai import ChatOpenAI

load_dotenv()

# Global trace storage (thread-safe for async)
trace_data = []
trace_lock = asyncio.Lock()

def get_next_trace_filename(prefix: str) -> str:
    """Find the next available trace filename with incrementing number."""
    n = 0
    while os.path.exists(f"{prefix}_{n}.json"):
        n += 1
    return f"{prefix}_{n}.json"

@dataclass
class Message:
    """A message sent between nodes in the mesh."""
    sender_id: int
    receiver_id: int
    content: str
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())
    msg_type: str = "chat"  # chat, broadcast, query, response

async def add_trace_entry(
    sender: int,
    receiver: List[int],
    content: str,
    llm_gen_time: float
):
    """Add an entry to the trace (async-safe).
    
    Args:
        sender: Node ID (-1 for user, 0+ for mesh nodes)
        receiver: List of receiver node IDs (always a list, even for single receivers).
    """
    async with trace_lock:
        trace_data.append({
            "sender": sender,
            "receiver": receiver,
            "time_sent": datetime.now().isoformat(),
            "llm_gen_time": round(llm_gen_time, 4),
            "data_size(kb)": round(len(content.encode('utf-8')) / 1024, 4)
        })

class MeshNode:
    """A single node in the mesh network."""
    
    # Different node personalities/roles for variety
    NODE_ROLES = [
        "analytical thinker who focuses on logic and data",
        "creative brainstormer who generates novel ideas",
        "devil's advocate who challenges assumptions",
        "synthesizer who combines different perspectives",
        "pragmatist who focuses on practical implementation",
        "researcher who digs deep into specific topics",
        "critic who identifies weaknesses and risks",
        "optimist who highlights opportunities and benefits",
        "systems thinker who considers broader implications",
        "specialist who provides domain expertise",
    ]
    
    def __init__(self, node_id: int, network: 'MeshNetwork', llm: ChatOpenAI):
        self.node_id = node_id
        self.network = network
        self.llm = llm
        self.inbox: asyncio.Queue = asyncio.Queue()
        self.conversation_history: List[Dict] = []
        self.role = self.NODE_ROLES[node_id % len(self.NODE_ROLES)]
        self.is_active = True
        self.messages_sent = 0
        self.messages_received = 0
        
    @property
    def name(self) -> str:
        return f"Node_{self.node_id}"
    
    def get_system_prompt(self) -> str:
        return f"""You are {self.name}, an AI agent in a mesh network of {self.network.num_nodes} nodes.
Your role: You are a {self.role}.

Communication guidelines:
- Keep responses concise (2-4 sentences)
- Reference other nodes' ideas when relevant
- Contribute your unique perspective based on your role
- You can address specific nodes or broadcast to all
- Format: Start with a brief insight, then optionally mention who you want to hear from next

Remember: You're part of a collaborative discussion. Build on others' ideas."""

    async def process_message(self, message: Message) -> Optional[str]:
        """Process an incoming message and generate a response."""
        # Add to conversation history
        self.conversation_history.append({
            "role": "user",
            "content": f"[From Node_{message.sender_id}]: {message.content}"
        })
        self.messages_received += 1
        
        # Build messages for LLM
        messages = [{"role": "system", "content": self.get_system_prompt()}]
        # Keep last 10 messages for context window management
        messages.extend(self.conversation_history[-10:])
        
        # Generate response
        start_time = time.time()
        try:
            response = await asyncio.to_thread(self.llm.invoke, messages)
            llm_gen_time = time.time() - start_time
            content = response.content
            
            # Add to conversation history
            self.conversation_history.append({
                "role": "assistant",
                "content": content
            })
            
            return content, llm_gen_time
        except Exception as e:
            print(f"[{self.name}] Error generating response: {e}")
            return None, 0.0
    
    async def send_message(self, receiver_id: int, content: str, msg_type: str = "chat"):
        """Send a message to another node."""
        if receiver_id == self.node_id:
            return  # Don't send to self
            
        message = Message(
            sender_id=self.node_id,
            receiver_id=receiver_id,
            content=content,
            msg_type=msg_type
        )
        
        await self.network.route_message(message)
        self.messages_sent += 1
    
    async def broadcast(self, content: str, exclude: Set[int] = None, log_trace: bool = True, llm_gen_time: float = 0.0):
        """Broadcast a message to all other nodes.
        
        Args:
            content: Message content to broadcast
            exclude: Set of node IDs to exclude from broadcast
            log_trace: Whether to log a trace entry for this broadcast
            llm_gen_time: LLM generation time (if this was an LLM response)
        """
        exclude = exclude or set()
        exclude.add(self.node_id)  # Don't send to self
        
        # Get list of target nodes
        target_ids = [node_id for node_id in range(self.network.num_nodes) if node_id not in exclude]
        
        if not target_ids:
            return
        
        # Log single trace entry with all receivers
        if log_trace:
            await add_trace_entry(
                sender=self.node_id,
                receiver=target_ids,
                content=content,
                llm_gen_time=llm_gen_time
            )
        
        # Send individual messages (without additional tracing)
        tasks = []
        for node_id in target_ids:
            tasks.append(self._send_message_no_trace(node_id, content, "broadcast"))
        
        await asyncio.gather(*tasks)
    
    async def _send_message_no_trace(self, receiver_id: int, content: str, msg_type: str = "chat"):
        """Send a message without logging (used internally for broadcasts)."""
        if receiver_id == self.node_id:
            return
            
        message = Message(
            sender_id=self.node_id,
            receiver_id=receiver_id,
            content=content,
            msg_type=msg_type
        )
        
        await self.network.route_message(message)
        self.messages_sent += 1
    
    async def run_inbox_processor(self):
        """Process messages from the inbox continuously."""
        while self.is_active:
            try:
                # Wait for a message with timeout
                message = await asyncio.wait_for(self.inbox.get(), timeout=1.0)
                
                # Process the message
                result = await self.process_message(message)
                if result:
                    content, llm_gen_time = result
                    
                    # Log trace for non-broadcast messages (broadcasts are logged at send time)
                    if message.msg_type != "broadcast":
                        await add_trace_entry(
                            sender=message.sender_id,
                            receiver=[self.node_id],
                            content=content,
                            llm_gen_time=llm_gen_time
                        )
                    
                    # Respond back or to random nodes based on communication pattern
                    await self.respond_to_message(message, content, llm_gen_time)
                    
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                print(f"[{self.name}] Inbox processor error: {e}")
    
    async def respond_to_message(self, original_message: Message, response_content: str, llm_gen_time: float = 0.0):
        """Decide how to respond after processing a message."""
        # Random communication pattern to make it more dynamic
        pattern = random.choice(["reply", "broadcast_some", "chain", "silent"])
        
        if pattern == "reply":
            # Reply directly to sender
            await self.send_message(original_message.sender_id, response_content, "response")
            
        elif pattern == "broadcast_some":
            # Send to a random subset of nodes
            num_targets = random.randint(1, max(1, self.network.num_nodes // 3))
            targets = random.sample(
                [i for i in range(self.network.num_nodes) if i != self.node_id],
                min(num_targets, self.network.num_nodes - 1)
            )
            
            # Log single trace entry with all receivers
            await add_trace_entry(
                sender=self.node_id,
                receiver=targets,
                content=response_content,
                llm_gen_time=llm_gen_time
            )
            
            # Send individual messages without additional tracing
            tasks = [self._send_message_no_trace(t, response_content, "broadcast") for t in targets]
            await asyncio.gather(*tasks)
            
        elif pattern == "chain":
            # Forward to one random node (not sender)
            candidates = [i for i in range(self.network.num_nodes) 
                         if i != self.node_id and i != original_message.sender_id]
            if candidates:
                target = random.choice(candidates)
                await self.send_message(target, response_content, "chain")
        
        # "silent" pattern - no response (gives natural pauses)


class MeshNetwork:
    """A mesh network of interconnected AI agent nodes."""
    
    def __init__(self, num_nodes: int, model: str = "gpt-4o-mini"):
        self.num_nodes = num_nodes
        self.llm = ChatOpenAI(model=model)
        self.nodes: List[MeshNode] = []
        self.is_running = False
        self.message_count = 0
        
        # Create nodes
        for i in range(num_nodes):
            node = MeshNode(i, self, self.llm)
            self.nodes.append(node)
        
    
    async def route_message(self, message: Message):
        """Route a message to the appropriate node."""
        if 0 <= message.receiver_id < self.num_nodes:
            await self.nodes[message.receiver_id].inbox.put(message)
            self.message_count += 1
    
    async def inject_topic(self, topic: str, initiator_id: int = 0):
        """Inject a topic into the network to start discussion."""
        # Add to trace as user input (-1 represents external user)
        await add_trace_entry(
            sender=-1,
            receiver=[initiator_id],
            content=topic,
            llm_gen_time=0.0
        )
        
        # Create initial message
        initial_node = self.nodes[initiator_id]
        initial_node.conversation_history.append({
            "role": "user",
            "content": f"[TOPIC TO DISCUSS]: {topic}"
        })
        
        # Generate initial response
        start_time = time.time()
        messages = [
            {"role": "system", "content": initial_node.get_system_prompt()},
            {"role": "user", "content": f"[TOPIC TO DISCUSS]: {topic}"}
        ]
        response = await asyncio.to_thread(self.llm.invoke, messages)
        llm_gen_time = time.time() - start_time
        
        initial_content = response.content
        initial_node.conversation_history.append({
            "role": "assistant",
            "content": initial_content
        })
        
        # Get all receiver IDs for the initial broadcast
        all_receivers = [i for i in range(self.num_nodes) if i != initiator_id]
        
        await add_trace_entry(
            sender=initiator_id,
            receiver=all_receivers,
            content=initial_content,
            llm_gen_time=llm_gen_time
        )
        
        
        # Broadcast to all other nodes to start the discussion (don't log again, already logged above)
        await initial_node.broadcast(
            f"Let's discuss: {topic}\n\nMy initial thoughts: {initial_content}",
            log_trace=False
        )
    
    async def run(self, topic: str, duration_seconds: float = 30.0, max_messages: int = 50):
        
        self.is_running = True
        
        # Start inbox processors for all nodes
        processor_tasks = [
            asyncio.create_task(node.run_inbox_processor()) 
            for node in self.nodes
        ]
        
        # Inject the initial topic
        await self.inject_topic(topic)
        
        # Run for specified duration or until max messages
        start_time = time.time()
        try:
            while (time.time() - start_time < duration_seconds and 
                   self.message_count < max_messages):
                await asyncio.sleep(0.5)
                
                # Occasionally inject some spontaneous communication
                if random.random() < 0.3:  # 10% chance each half-second
                    random_node = random.choice(self.nodes)
                    if random_node.conversation_history:
                        # Ask a follow-up or make a new observation
                        prompts = [
                            "What aspects haven't we considered yet?",
                            "Can someone build on my previous point?",
                            "I'd like to hear a different perspective.",
                            "Let's focus on the practical implications.",
                            "What are the potential risks here?",
                        ]
                        spontaneous_msg = random.choice(prompts)
                        target = random.choice([n for n in self.nodes if n != random_node])
                        await random_node.send_message(
                            target.node_id, 
                            spontaneous_msg,
                            "spontaneous"
                        )
        
        finally:
            # Stop all nodes
            for node in self.nodes:
                node.is_active = False
            
            # Cancel processor tasks
            for task in processor_tasks:
                task.cancel()
            
            await asyncio.gather(*processor_tasks, return_exceptions=True)
        
        self.is_running = False
        
    
    def get_full_conversation(self) -> List[Dict]:
        """Get the combined conversation history from all nodes."""
        all_messages = []
        for node in self.nodes:
            for msg in node.conversation_history:
                all_messages.append({
                    "node": node.name,
                    "role": msg["role"],
                    "content": msg["content"]
                })
        return all_messages


async def main():
    """Main entry point for running the mesh network."""
    # Configuration
    NUM_NODES = random.randint(3, 10)  # Random number of nodes in the mesh (3-10)
    DISCUSSION_DURATION = random.uniform(15.0, 45.0)  # Random duration (15-45 seconds)
    MAX_MESSAGES = 1000  # Maximum messages before stopping
    TRACE_FILENAME = get_next_trace_filename("agent_trace/mesh_trace")
    
    # Topic for discussion
    TOPIC = """Design a distributed system for real-time collaborative document editing.
Consider: consistency models, conflict resolution, scalability, and user experience.
What architecture and algorithms would you recommend?"""
    
    # Clear previous trace
    global trace_data
    trace_data = []
    
    # Create and run the mesh network
    network = MeshNetwork(num_nodes=NUM_NODES)
    
    await network.run(
        topic=TOPIC,
        duration_seconds=DISCUSSION_DURATION,
        max_messages=MAX_MESSAGES
    )
    
    # Save trace to JSON file
    with open(TRACE_FILENAME, "w") as f:
        json.dump(trace_data, f, indent=2)


if __name__ == "__main__":
    asyncio.run(main())

