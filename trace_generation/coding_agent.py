import os
import json
import time
import asyncio
import random
from datetime import datetime
from typing import List, Dict, Tuple
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

# Node IDs: -1=user, 1=supervisor, 2=synthesizer, 3+=workers, -1=end
USER_ID = -1
SUPERVISOR_ID = 0
SYNTHESIZER_ID = 1
WORKER_START_ID = 2 # Workers are 2, 3, 4, ...

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


class TaskQueue:
    """Thread-safe task queue for workers to pull from."""
    
    def __init__(self):
        self.tasks: asyncio.Queue[Tuple[int, str]] = asyncio.Queue()  # (task_id, task_description)
        self.total_tasks = 0
    
    async def add_task(self, task_id: int, task: str):
        """Add a task to the queue."""
        await self.tasks.put((task_id, task))
        self.total_tasks += 1
    
    async def get_task(self) -> Tuple[int, str] | None:
        """Get a task from the queue. Returns None if empty."""
        try:
            return self.tasks.get_nowait()
        except asyncio.QueueEmpty:
            return None
    
    def is_empty(self) -> bool:
        return self.tasks.empty()


class TaskSupervisor:
    """Supervisor that splits problems into tasks and manages task queue."""
    
    def __init__(self, num_workers: int, num_tasks: int):
        self.num_workers = num_workers
        self.num_tasks = num_tasks
        self.tasks: List[str] = []
        self.task_queue = TaskQueue()
        self.worker_results: Dict[int, List[Tuple[int, str]]] = {}  # worker_id -> [(task_id, result), ...]
        self.results_lock = asyncio.Lock()
    
    async def split_into_tasks(self, user_request: str) -> List[str]:
        """Split the user request into discrete coding tasks."""
        system = f"""You are a project manager splitting a coding project into {self.num_tasks} tasks.
Each task should be a self-contained coding component that can be developed independently.
Output ONLY a numbered list of {self.num_tasks} tasks, one per line.
Format: 
1. [Task description]
2. [Task description]
...

Be specific about what each task should implement. Make tasks varied in complexity."""

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
        
        # Ensure we have at least num_tasks tasks
        while len(tasks) < self.num_tasks:
            tasks.append(f"Additional component {len(tasks) + 1}")
        tasks = tasks[:self.num_tasks]
        
        self.tasks = tasks
        
        # Add all tasks to the queue
        for i, task in enumerate(tasks):
            await self.task_queue.add_task(i, task)
        
        # Log trace: supervisor broadcasting task list to all workers
        worker_ids = [WORKER_START_ID + i for i in range(self.num_workers)]
        await add_trace_entry(SUPERVISOR_ID, worker_ids, content, llm_gen_time)
        
        return tasks
    
    async def collect_result(self, worker_id: int, task_id: int, result: str):
        """Collect a result from a worker for a specific task."""
        async with self.results_lock:
            if worker_id not in self.worker_results:
                self.worker_results[worker_id] = []
            self.worker_results[worker_id].append((task_id, result))
    
    def get_all_results(self) -> Dict[int, str]:
        """Get all collected results, combining multiple results per worker."""
        combined = {}
        for worker_id, results in self.worker_results.items():
            # Combine all results from this worker
            combined_result = "\n\n".join([
                f"=== Task {task_id + 1}: {self.tasks[task_id]} ===\n{result}" 
                for task_id, result in sorted(results)
            ])
            combined[worker_id] = combined_result
        return combined
    
    def get_all_task_results(self) -> List[Tuple[int, str]]:
        """Get all results ordered by task ID."""
        all_results = []
        for results in self.worker_results.values():
            all_results.extend(results)
        return sorted(all_results, key=lambda x: x[0])


class CodeWorker:
    """A worker node that implements coding tasks from a queue."""
    
    def __init__(self, worker_id: int, worker_index: int):
        self.worker_id = worker_id  # Absolute ID (2, 3, 4, ...)
        self.worker_index = worker_index  # Index (0, 1, 2, ...)
        self.tasks_completed = 0
    
    async def execute_task(self, task_id: int, task: str, project_context: str) -> str:
        """Execute a coding task and return the implementation."""
        system = f"""You are Worker {self.worker_index}, a specialized coding agent.
Your task is to implement ONE specific component of a larger project.
Write clean, production-ready code with comments.
Focus ONLY on your assigned task. Be thorough but concise.
Include all necessary imports and exports for integration."""

        user_msg = f"""Project Context: {project_context}

Your Assigned Task (Task #{task_id + 1}): {task}

Implement this component now. Provide complete, working code."""

        messages = [
            {"role": "system", "content": system},
            {"role": "user", "content": user_msg}
        ]
        
        start_time = time.time()
        response = await asyncio.to_thread(llm.invoke, messages)
        llm_gen_time = time.time() - start_time
        
        content = response.content
        self.tasks_completed += 1
        
        # Log trace: worker sending result back to supervisor
        await add_trace_entry(self.worker_id, [SUPERVISOR_ID], content, llm_gen_time)
        
        return content
    
    async def run_worker_loop(self, supervisor: TaskSupervisor, project_context: str):
        """Continuously pull and execute tasks until queue is empty."""
        while True:
            # Try to get a task from the queue
            task_data = await supervisor.task_queue.get_task()
            
            if task_data is None:
                # No more tasks available
                break
            
            task_id, task = task_data
            
            # Log trace: supervisor assigning task to this worker
            await add_trace_entry(SUPERVISOR_ID, [self.worker_id], f"Assigned task {task_id + 1}: {task}", 0.0)
            
            # Execute the task
            result = await self.execute_task(task_id, task, project_context)
            
            # Report result back to supervisor
            await supervisor.collect_result(self.worker_id, task_id, result)


class CodeSynthesizer:
    """Synthesizes multiple code components into a unified program."""
    
    async def synthesize(self, project_context: str, task_results: List[Tuple[int, str]], tasks: List[str]) -> str:
        """Combine all task outputs into a coherent program."""
        
        # Build the components section
        components_text = ""
        for task_id, output in task_results:
            task = tasks[task_id] if task_id < len(tasks) else f"Task {task_id}"
            components_text += f"\n{'='*50}\nCOMPONENT {task_id + 1}: {task}\n{'='*50}\n{output}\n"
        
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
        
        # Log trace: synthesizer sending final result back to supervisor
        await add_trace_entry(SYNTHESIZER_ID, [SUPERVISOR_ID], content, llm_gen_time)
        
        return content


async def run_coding_pipeline(user_request: str, num_workers: int = 4, num_tasks: int = None):
    """Run the full coding pipeline with parallel workers pulling from a task queue.
    
    Args:
        user_request: The coding project request
        num_workers: Number of parallel workers
        num_tasks: Number of tasks to generate (default: 1.5x to 2.5x workers)
    """
    # Generate more tasks than workers if not specified
    if num_tasks is None:
        num_tasks = random.randint(int(num_workers * 1), int(num_workers * 2.5))
    
    # Log user input
    await add_trace_entry(USER_ID, [SUPERVISOR_ID], user_request, 0.0)
    
    # Step 1: Supervisor splits the problem into tasks
    supervisor = TaskSupervisor(num_workers, num_tasks)
    tasks = await supervisor.split_into_tasks(user_request)
    
    # Step 2: Create workers and run them in parallel
    # Each worker will pull tasks from the queue until empty
    workers = [CodeWorker(WORKER_START_ID + i, i) for i in range(num_workers)]
    
    # Run all workers simultaneously - they'll pull tasks from the queue
    await asyncio.gather(*[
        worker.run_worker_loop(supervisor, user_request)
        for worker in workers
    ])
    
    # Log supervisor collecting all results
    worker_ids = [WORKER_START_ID + i for i in range(num_workers)]
    await add_trace_entry(SUPERVISOR_ID, [SYNTHESIZER_ID], 
                          f"Collected results for {num_tasks} tasks from {num_workers} workers", 0.0)
    
    # Step 3: Synthesize results
    synthesizer = CodeSynthesizer()
    task_results = supervisor.get_all_task_results()
    final_result = await synthesizer.synthesize(
        user_request, 
        task_results,
        tasks
    )
    
    return {
        "tasks": tasks,
        "task_results": task_results,
        "worker_outputs": supervisor.get_all_results(),
        "final_program": final_result,
        "workers_used": num_workers,
        "tasks_completed": sum(w.tasks_completed for w in workers)
    }


async def main():
    """Main entry point."""
    # Configuration
    NUM_WORKERS = random.randint(2, 5)  # Number of parallel worker nodes
    trace_filename = get_next_trace_filename("agent_trace/coding_trace")
    
    user_content = """I want to build a calculator app with dedicated frontend and backend.
Use React with TypeScript for the frontend, Node.js with Express for the backend.
Include basic arithmetic operations, history of calculations, and a clean UI."""
    
    # Run the pipeline
    result = await run_coding_pipeline(user_content, num_workers=NUM_WORKERS)
    
    
    # Save trace to JSON file
    with open(trace_filename, "w") as f:
        json.dump(trace_data, f, indent=2)


if __name__ == "__main__":
    asyncio.run(main())
