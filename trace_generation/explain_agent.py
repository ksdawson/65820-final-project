import os
import json
import time
from datetime import datetime
from typing import Literal
from dotenv import load_dotenv
from langchain_openai import ChatOpenAI
from langgraph.graph import StateGraph, MessagesState, START, END

load_dotenv()

llm = ChatOpenAI(model="gpt-4o-mini")

# Global trace storage
trace_data = []

def get_next_trace_filename(prefix: str) -> str:
    """Find the next available trace filename with incrementing number."""
    n = 0
    while os.path.exists(f"{prefix}_{n}.json"):
        n += 1
    return f"{prefix}_{n}.json"

# Node ID mapping: 0=user, 1=supervisor, 2=researcher, 3=writer, 4=critic, -1=end
NODE_IDS = {"user": 0, "supervisor": 1, "researcher": 2, "writer": 3, "critic": 4, "end": -1}

def add_trace_entry(sender: int, receiver: list, content: str, llm_gen_time: float):
    """Add an entry to the trace."""
    trace_data.append({
        "sender": sender,
        "receiver": receiver,
        "time_sent": datetime.now().isoformat(),
        "llm_gen_time": round(llm_gen_time, 4),
        "data_size(kb)": round(len(content.encode('utf-8')) / 1024, 4)
    })

# Specialized agents
def researcher(state: MessagesState):
    """Researches and gathers information."""
    system = "You are a research agent. Gather key facts and information about the user's topic. Be concise."
    messages = [{"role": "system", "content": system}] + state["messages"]
    start_time = time.time()
    response = llm.invoke(messages)
    llm_gen_time = time.time() - start_time
    content = f"[Researcher]: {response.content}"
    add_trace_entry(NODE_IDS["researcher"], [NODE_IDS["supervisor"]], content, llm_gen_time)
    return {"messages": [{"role": "assistant", "content": content}]}

def writer(state: MessagesState):
    """Writes content based on research."""
    system = "You are a writing agent. Based on the research provided, write a clear, engaging response. Be concise."
    messages = [{"role": "system", "content": system}] + state["messages"]
    start_time = time.time()
    response = llm.invoke(messages)
    llm_gen_time = time.time() - start_time
    content = f"[Writer]: {response.content}"
    add_trace_entry(NODE_IDS["writer"], [NODE_IDS["supervisor"]], content, llm_gen_time)
    return {"messages": [{"role": "assistant", "content": content}]}

def critic(state: MessagesState):
    """Reviews and provides final feedback."""
    system = "You are a critic agent. Review the work and provide a final polished response to the user. Be concise."
    messages = [{"role": "system", "content": system}] + state["messages"]
    start_time = time.time()
    response = llm.invoke(messages)
    llm_gen_time = time.time() - start_time
    content = f"[Critic]: {response.content}"
    add_trace_entry(NODE_IDS["critic"], [NODE_IDS["supervisor"]], content, llm_gen_time)
    return {"messages": [{"role": "assistant", "content": content}]}

def supervisor(state: MessagesState) -> dict:
    """Routes to the next agent or ends."""
    system = """You are a supervisor managing a team: researcher, writer, critic.
    Based on the conversation, decide who should act next.
    - If no research has been done, route to 'researcher'
    - If research exists but no writing, route to 'writer'  
    - If writing exists but no review, route to 'critic'
    - If all steps are complete, route to 'FINISH'
    
    Respond with ONLY one word: researcher, writer, critic, or FINISH"""
    
    messages = [{"role": "system", "content": system}] + state["messages"]
    start_time = time.time()
    response = llm.invoke(messages)
    llm_gen_time = time.time() - start_time
    decision = response.content.strip().lower()
    next_agent = decision if decision in ["researcher", "writer", "critic"] else "end"
    add_trace_entry(NODE_IDS["supervisor"], [NODE_IDS[next_agent]], decision, llm_gen_time)
    return {"next": decision}

def route_supervisor(state: dict) -> Literal["researcher", "writer", "critic", "__end__"]:
    """Route based on supervisor decision."""
    next_agent = state.get("next", "FINISH").lower()
    if next_agent == "finish" or next_agent not in ["researcher", "writer", "critic"]:
        return "__end__"
    return next_agent

# Build the graph
class AgentState(MessagesState):
    next: str

graph = StateGraph(AgentState)

# Add nodes
graph.add_node("supervisor", supervisor)
graph.add_node("researcher", researcher)
graph.add_node("writer", writer)
graph.add_node("critic", critic)

# Add edges
graph.add_edge(START, "supervisor")
graph.add_conditional_edges("supervisor", route_supervisor)
graph.add_edge("researcher", "supervisor")
graph.add_edge("writer", "supervisor")
graph.add_edge("critic", "supervisor")

graph = graph.compile()

# Run it
if __name__ == "__main__":
    # Configuration
    trace_filename = get_next_trace_filename("explain_trace")
    user_content = "Explain quantum computing"
    
    # Add initial user message to trace
    add_trace_entry(NODE_IDS["user"], [NODE_IDS["supervisor"]], user_content, 0.0)
    
    result = graph.invoke({"messages": [{"role": "user", "content": user_content}]})
    
    print("\n" + "="*50)
    print("CONVERSATION TRACE:")
    print("="*50)
    for msg in result["messages"]:
        # Handle both dict and LangChain message objects
        if hasattr(msg, "type"):
            role = msg.type
            content = msg.content
        else:
            role = msg.get("role", "unknown")
            content = msg.get("content", str(msg))
        print(f"\n[{role.upper()}]: {content}")
    
    # Save trace to JSON file
    with open(trace_filename, "w") as f:
        json.dump(trace_data, f, indent=2)
    print(f"\n{'='*50}")
    print(f"Trace saved to {trace_filename} ({len(trace_data)} entries)")
