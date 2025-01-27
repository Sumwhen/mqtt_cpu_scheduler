use rand::Rng;
use rumqttc::{AsyncClient, ClientError, Event, MqttOptions, Packet, QoS};
use serde::Deserialize;
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::sync::{Arc, Mutex};
use std::time::SystemTime;
use tokio::runtime::Runtime;
use tokio::time::{self, Duration, Instant};

const TOPIC: &str = "schedule";
const PUBLISH_TOPIC: &str = "schedule/publish";
const NUMBER_OF_WORKERS: usize = 5;
const KEEPALIVE_INTERVAL: u64 = 10;

const BROKER_HOST: &str = "localhost";
const BROKER_PORT: u16 = 1883;

// the task description being sent over the network
#[derive(Deserialize, Debug, Clone)]
struct Task {
    id: u64,
    name: String,
    success: f32,
    // timestamp since EPOCH
    deadline: u128,
    processing_time: u64,
}

struct ScheduledTask {
    task: Task,
    // milliseconds remaining at time of reception
    deadline: u64,
    // moment of reception
    received: Instant,
}

// tasks are unique by ID
impl PartialEq for ScheduledTask {
    fn eq(&self, other: &Self) -> bool {
        self.task.id == other.task.id
    }
}
impl Eq for ScheduledTask {}

// these traits are used to sort the priority queue.
// right now, EDF (earlies deadline first) ordering is used.
impl PartialOrd for ScheduledTask {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(&other))
    }
}

trait TimeRemaining {
    fn time_remaining(&self) -> u64;
}

impl TimeRemaining for ScheduledTask {
    fn time_remaining(&self) -> u64 {
        (self.deadline - self.received.elapsed().as_millis() as u64) - self.task.processing_time
    }
}
impl Ord for ScheduledTask {
    fn cmp(&self, other: &Self) -> Ordering {
        other.time_remaining().cmp(&self.time_remaining())
    }
}


fn main() {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(NUMBER_OF_WORKERS + 1)
        .build()
        .unwrap()
        .block_on(async {
            let mut mqttoptions = MqttOptions::new("scheduler-async", BROKER_HOST, BROKER_PORT);
            mqttoptions.set_keep_alive(Duration::from_secs(KEEPALIVE_INTERVAL));
            let (client, mut eventloop) = AsyncClient::new(mqttoptions, 10);
            client.subscribe(TOPIC, QoS::AtMostOnce).await.unwrap();

            // priority queue, state for all threads
            let task_queue: Arc<Mutex<BinaryHeap<ScheduledTask>>> =
                Arc::new(Mutex::new(BinaryHeap::new()));


            // thread dispatcher, clones for thread ownership
            let task_queue_clone = task_queue.clone();
            tokio::spawn(async move {
                loop {
                    let now = Instant::now();

                    let mut tasks_to_run = Vec::new();
                    // empty the priority queue
                    {
                        let mut q = task_queue_clone.lock().unwrap();
                        while let Some(task) = q.peek() {
                            // add any and all tasks rcv'ed via messages into the queue, except they are not possible
                            // println!("TAKING FROM QUEUE: {} - {}", task.name, task.processing_time);
                            if task.task.processing_time > task.deadline {
                                // client.publish(PUBLISH_TOPIC, QoS::AtLeastOnce, false,
                                //                format!("impossible task {} dropped", task.name))
                                // .await.unwrap();
                                println!(
                                    "IMPOSSIBLE TASK: {} - {}",
                                    task.task.name, task.task.processing_time
                                );
                                q.pop();
                                continue;
                            }

                            tasks_to_run.push(q.pop().unwrap());
                        }
                    }
                    // for each task that was in the queue, spawn a thread
                    for scheduled_task in tasks_to_run {
                        // println!("Scheduling task #{}", scheduled_task.id);
                        let client_clone = client.clone();

                        /* println!("METRICS =============\n{} - {}",
                                thread_pool.metrics().num_alive_tasks(), thread_pool.metrics().num_workers());


                        */
                        tokio::spawn(async move {
                            //publish_msg(&client_clone,
                            //            format!("Task spawned: \n {:#?}", scheduled_task))
                            //    .await.expect("Error with publishing spawning message");
                            let future = tokio::task::spawn_blocking(move || {
                                time::sleep(Duration::from_millis(
                                    scheduled_task.task.processing_time,
                                ))
                            }).await.unwrap();

                            let rn = random_percent();

                            let s = if rn <= scheduled_task.task.success {
                                "succeeded".to_string()
                            } else {
                                "failed".to_string()
                            };

                            future.await;
                            let dl =
                                match now.elapsed().as_millis() < scheduled_task.deadline as u128 {
                                    true => "completed in time".to_string(),
                                    false => "missed deadline".to_string(),
                                };
                            publish_msg(
                                &client_clone,
                                format!(
                                    "COMPLETED {:8}. [{:9}] {}",
                                    scheduled_task.task.name, s, dl
                                ),
                            )
                            .await
                            .expect("Error publishing finishing message");
                        });
                    }
                }
            });

            // MQTT client loop until error
            while let Ok(event) = eventloop.poll().await {
                // listen for incoming publishings only
                if let Event::Incoming(Packet::Publish(packet)) = event {
                    if let Ok(payload) = String::from_utf8(packet.payload.to_vec()) {
                        if let Ok(task) = serde_json::from_str::<Task>(&payload) {
                            let mut queue = task_queue.lock().unwrap();
                            let timestamp = SystemTime::now()
                                .duration_since(SystemTime::UNIX_EPOCH)
                                .unwrap()
                                .as_millis();
                            println!("Task received {:?}", &task);

                            let st = ScheduledTask {
                                deadline: (&task.deadline - timestamp) as u64,
                                task,
                                received: Instant::now(),
                            };
                            queue.push(st);
                        } else {
                            eprintln!("Failed to deserialize task {:?}", payload);
                        }
                    } else {
                        eprintln!("Failed to parse payload as UTF-8");
                    }
                }
            }
        })
}

async fn publish_msg(client: &AsyncClient, msg: String) -> Result<(), ClientError> {
    client
        .publish(PUBLISH_TOPIC, QoS::AtLeastOnce, false, msg)
        .await
}

fn random_percent() -> f32 {
    let mut rng = rand::rng();
    rng.random()
}
