# MQTT Scheduler in Rust

## Task Description
Write a scheduler that accepts, prioritizes, and handles incoming tasks. The scheduler should
accept tasks via MQTT messages and distribute them across a fixed but configurable number of
worker threads. The processing time is known beforehand and is passed in the MQTT message –
simply let the handling thread sleep for the specified time. Tasks also have a given chance of
success, which is also known beforehand and passed in the message.
Each task also comes with a deadline. The scheduler should schedule the tasks in a way that all
tasks can be handled in time. If deadlines cannot be met, try to order the tasks in a way that
maximizes the number of tasks that can be completed on time.


The MQTT messages instructing the scheduler should provide the following information:
- An ID
- A very brief description or name
- A deadline
- Processing time
- A success probability (in percentage)
  The scheduler should send the following responses/information via MQTT messages:
- When a task has started on a worker thread
- When a task has finished or failed


  To achieve this:
1. Set up an MQTT broker (e.g. Mosquitto) in a container. (DONE)
2. Implement the scheduler with its worker threads (in Rust).
3. Write a small producer (e.g. a Python script) to feed the scheduler with tasks.

## Implementation

Since an optimal scheduling routine must be found, FIFO and RR are ruled out.
Non-preemtive scheduling is easier to implement, so let's go with that.

non-preemptive scheduling routines that match the task description:

EDF

SJF