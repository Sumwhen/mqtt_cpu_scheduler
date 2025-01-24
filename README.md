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
2. Implement the scheduler with its worker threads (in Rust). (IMPLEMENTED, not DONE)
3. Write a small producer (e.g. a Python script) to feed the scheduler with tasks. (DONE)

## Goal

Since an optimal, concurrent scheduling routine must be found, FIFO and RR are ruled out.
It's an CPU-scheduling problem, and intuitively STR (shortest time remaining) would be a better
solution than EDF, especially for pre-emptying the least-prio task.

sort the priority queue by time left after completion
```
let time_remaining = task.deadline - now.as_millis().unwrap()
let time_left_after_completion = time_remaining - task.processing_time
```
with the least amount of time remaining having the highest priority.
the task with the lowest priority should be preemtied in case of new tasks arriving while
the workers busy.

## Implementation

Implemented is an async worker dispatcher with EDF.
Payload:
```rust
struct Task {
  id: u32,
  // implemented as timespan (time remaining in ms), but should be timestamp 
  deadline: u64, 
  // ms
  processing_time: u64,
  // [0,1]
  success: f32,
  // owned string as task description
  name: String
}
```

### Possible Improvements
1. STR => deqeue
2. one additional thread for publishing instead of publishing in each worker thread
3. really limit the amount of threads working, wrap everything in a runtime maybe