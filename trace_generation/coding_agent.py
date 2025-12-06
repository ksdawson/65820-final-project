import os
import json
import time
import asyncio
from datetime import datetime
from typing import List, Dict
from dotenv import load_dotenv
from langchain_openai import ChatOpenAI

load_dotenv()

llm = ChatOpenAI(model="gpt-4o-mini")

# Global trace storage
trace_data = []
trace_lock = asyncio.Lock()

def get_next_trace_filename(prefix: str) -> str:
    """Find the next available trace filename with incrementing number."""
    n = 0
    while os.path.exists(f"{prefix}_{n}.json"):
        n += 1
    return f"{prefix}_{n}.json"

# Node IDs: 0=user, 1=supervisor, 2=synthesizer, 3+=workers, -1=end
USER_ID = 0
SUPERVISOR_ID = 1
SYNTHESIZER_ID = 2
WORKER_START_ID = 3  # Workers are 3, 4, 5, ...

async def add_trace_entry(sender: int, receiver: List[int], content: str, llm_gen_time: float):
    """Add an entry to the trace (async-safe)."""
    async with trace_lock:
        trace_data.append({
            "sender": sender,
            "receiver": receiver,
            "time_sent": datetime.now().isoformat(),
            "llm_gen_time": round(llm_gen_time, 4),
            "data_size(kb)": round(len(content.encode('utf-8')) / 1024, 4)
        })


class TaskSupervisor:
    """Supervisor that splits problems into tasks and assigns them to workers."""
    
    def __init__(self, num_workers: int):
        self.num_workers = num_workers
        self.tasks: List[str] = []
        self.worker_results: Dict[int, str] = {}
    
    async def split_into_tasks(self, user_request: str) -> List[str]:
        """Split the user request into discrete coding tasks."""
        system = f"""You are a project manager splitting a coding project into exactly {self.num_workers} parallel tasks.
Each task should be a self-contained coding component that can be developed independently.
Output ONLY a numbered list of {self.num_workers} tasks, one per line.
Format: 
1. [Task description]
2. [Task description]
...

Be specific about what each task should implement."""

        messages = [
            {"role": "system", "content": system},
            {"role": "user", "content": user_request}
        ]
        
        start_time = time.time()
        response = await asyncio.to_thread(llm.invoke, messages)
        llm_gen_time = time.time() - start_time
        
        content = response.content
        
        # Parse tasks from response
        lines = content.strip().split('\n')
        tasks = []
        for line in lines:
            line = line.strip()
            if line and (line[0].isdigit() or line.startswith('-')):
                # Remove numbering/bullets
                task = line.lstrip('0123456789.-) ').strip()
                if task:
                    tasks.append(task)
        
        # Ensure we have exactly num_workers tasks
        while len(tasks) < self.num_workers:
            tasks.append(f"Additional component {len(tasks) + 1}")
        tasks = tasks[:self.num_workers]
        
        self.tasks = tasks
        
        # Log trace: supervisor sending tasks to all workers
        worker_ids = [WORKER_START_ID + i for i in range(self.num_workers)]
        await add_trace_entry(SUPERVISOR_ID, worker_ids, content, llm_gen_time)
        
        return tasks
    
    async def collect_result(self, worker_id: int, result: str):
        """Collect a result from a worker."""
        self.worker_results[worker_id] = result
    
    def get_all_results(self) -> Dict[int, str]:
        """Get all collected results."""
        return self.worker_results


class CodeWorker:
    """A worker node that implements a specific coding task."""
    
    def __init__(self, worker_id: int, worker_index: int):
        self.worker_id = worker_id  # Absolute ID (3, 4, 5, ...)
        self.worker_index = worker_index  # Index (0, 1, 2, ...)
    
    async def execute_task(self, task: str, project_context: str) -> str:
        """Execute a coding task and return the implementation."""
        system = f"""You are Worker {self.worker_index}, a specialized coding agent.
Your task is to implement ONE specific component of a larger project.
Write clean, production-ready code with comments.
Focus ONLY on your assigned task. Be thorough but concise.
Include all necessary imports and exports for integration."""

        user_msg = f"""Project Context: {project_context}

Your Assigned Task: {task}

Implement this component now. Provide complete, working code."""

        messages = [
            {"role": "system", "content": system},
            {"role": "user", "content": user_msg}
        ]
        
        start_time = time.time()
        response = await asyncio.to_thread(llm.invoke, messages)
        llm_gen_time = time.time() - start_time
        
        content = response.content
        
        # Log trace: worker sending result back to supervisor
        await add_trace_entry(self.worker_id, [SUPERVISOR_ID], content, llm_gen_time)
        
        return content


class CodeSynthesizer:
    """Synthesizes multiple code components into a unified program."""
    
    async def synthesize(self, project_context: str, worker_outputs: Dict[int, str], tasks: List[str]) -> str:
        """Combine all worker outputs into a coherent program."""
        
        # Build the components section
        components_text = ""
        for i, (worker_id, output) in enumerate(sorted(worker_outputs.items())):
            task = tasks[i] if i < len(tasks) else f"Task {i}"
            components_text += f"\n{'='*50}\nCOMPONENT {i+1}: {task}\n{'='*50}\n{output}\n"
        
        system = """You are a senior software architect responsible for integrating code components.
Your job is to:
1. Review all components for compatibility
2. Create a unified project structure
3. Add any missing integration code (imports, exports, main entry points)
4. Provide a complete, working codebase with clear file organization
5. Add a README with setup instructions

Output a well-organized, complete project that integrates all components."""

        user_msg = f"""Project Goal: {project_context}

The following components were developed by parallel workers:
{components_text}

Now synthesize these into a complete, integrated project."""

        messages = [
            {"role": "system", "content": system},
            {"role": "user", "content": user_msg}
        ]
        
        start_time = time.time()
        response = await asyncio.to_thread(llm.invoke, messages)
        llm_gen_time = time.time() - start_time
        
        content = response.content
        
        # Log trace: synthesizer sending final result
        await add_trace_entry(SYNTHESIZER_ID, [-1], content, llm_gen_time)
        
        return content


async def run_coding_pipeline(user_request: str, num_workers: int = 4):
    """Run the full coding pipeline with parallel workers."""
    
    print(f"\n{'='*60}")
    print(f"CODING AGENT - Parallel Task Execution")
    print(f"Workers: {num_workers}")
    print(f"{'='*60}\n")
    
    # Log user input
    await add_trace_entry(USER_ID, [SUPERVISOR_ID], user_request, 0.0)
    
    # Step 1: Supervisor splits the problem
    print("[SUPERVISOR] Analyzing and splitting project into tasks...")
    supervisor = TaskSupervisor(num_workers)
    tasks = await supervisor.split_into_tasks(user_request)
    
    print(f"\n[SUPERVISOR] Created {len(tasks)} tasks:")
    for i, task in enumerate(tasks):
        print(f"  {i+1}. {task}")
    
    # Step 2: Workers execute tasks in parallel
    print(f"\n[WORKERS] Executing {num_workers} tasks in parallel...")
    
    workers = [CodeWorker(WORKER_START_ID + i, i) for i in range(num_workers)]
    
    async def worker_task(worker: CodeWorker, task: str):
        print(f"  Worker {worker.worker_index} starting: {task[:50]}...")
        result = await worker.execute_task(task, user_request)
        await supervisor.collect_result(worker.worker_id, result)
        print(f"  Worker {worker.worker_index} completed!")
        return result
    
    # Run all workers simultaneously
    await asyncio.gather(*[
        worker_task(workers[i], tasks[i]) 
        for i in range(num_workers)
    ])
    
    # Log supervisor collecting all results
    worker_ids = [WORKER_START_ID + i for i in range(num_workers)]
    await add_trace_entry(SUPERVISOR_ID, [SYNTHESIZER_ID], 
                          f"Collected {num_workers} worker results", 0.0)
    
    # Step 3: Synthesize results
    print("\n[SYNTHESIZER] Combining components into final program...")
    synthesizer = CodeSynthesizer()
    final_result = await synthesizer.synthesize(
        user_request, 
        supervisor.get_all_results(),
        tasks
    )
    
    print("\n[SYNTHESIZER] Complete!")
    
    return {
        "tasks": tasks,
        "worker_outputs": supervisor.get_all_results(),
        "final_program": final_result
    }


async def main():
    """Main entry point."""
    # Configuration
    NUM_WORKERS = 4  # Number of parallel worker nodes
    trace_filename = get_next_trace_filename("coding_trace")
    
    user_content = """I want to build a calculator app with dedicated frontend and backend.
Use React with TypeScript for the frontend, Node.js with Express for the backend.
Include basic arithmetic operations, history of calculations, and a clean UI."""
    
    # Run the pipeline
    result = await run_coding_pipeline(user_content, num_workers=NUM_WORKERS)
    
    # Print summary
    print(f"\n{'='*60}")
    print("EXECUTION SUMMARY")
    print(f"{'='*60}")
    print(f"Tasks completed: {len(result['tasks'])}")
    print(f"Total trace entries: {len(trace_data)}")
    
    # Print final synthesized program
    print(f"\n{'='*60}")
    print("FINAL SYNTHESIZED PROGRAM")
    print(f"{'='*60}")
    print(result['final_program'])
    
    # Save trace to JSON file
    with open(trace_filename, "w") as f:
        json.dump(trace_data, f, indent=2)
    print(f"\n{'='*60}")
    print(f"Trace saved to {trace_filename} ({len(trace_data)} entries)")


if __name__ == "__main__":
    asyncio.run(main())
