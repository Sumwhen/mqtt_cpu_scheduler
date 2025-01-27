use console_subscriber;
use parking_lot::Mutex;
use rand::Rng;
use rumqttc::{AsyncClient, ClientError, Event, MqttOptions, Packet, QoS};
use serde::Deserialize;
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::sync::Arc;
use std::time::SystemTime;
use tokio::sync::mpsc::unbounded_channel;
use tokio::sync::Semaphore;
use tokio::task::JoinHandle;
use tokio::time::{Duration, Instant};

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

#[derive(Debug)]
struct ScheduledTask {
    task: Task,
    // milliseconds remaining at time of reception
    deadline: u64,
    // moment of reception
    received: Instant,
}

struct FinishedTask {
    task: ScheduledTask,
    finished: Instant,
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
    fn time_remaining(&self) -> i64;
}

impl TimeRemaining for ScheduledTask {
    fn time_remaining(&self) -> i64 {
        self.deadline as i64
            - self.received.elapsed().as_millis() as i64
            - self.task.processing_time as i64
    }
}
impl Ord for ScheduledTask {
    fn cmp(&self, other: &Self) -> Ordering {
        other.time_remaining().cmp(&self.time_remaining())
    }
}

#[tokio::main]
async fn main() {
    console_subscriber::init();
    let mut mqttoptions = MqttOptions::new("scheduler-async", BROKER_HOST, BROKER_PORT);
    mqttoptions.set_keep_alive(Duration::from_secs(KEEPALIVE_INTERVAL));
    let (client, mut eventloop) = AsyncClient::new(mqttoptions, 10);
    client.subscribe(TOPIC, QoS::AtMostOnce).await.unwrap();

    // channel used for communication between worker threads and the publishing thread
    let (tx, mut rx) = unbounded_channel::<FinishedTask>();
    // publishing thread
    let client_clone = client.clone();
    // commit thread to work-stealing runtime
    tokio::spawn(async move {
        while let Some(finished_task) = rx.recv().await {
            let rn = random_percent();

            let s = if rn <= finished_task.task.task.success {
                "success".to_string()
            } else {
                "fail".to_string()
            };
            let dl = match (finished_task.finished - finished_task.task.received).as_millis()
                < finished_task.task.deadline as u128
            {
                true => "completed in time".to_string(),
                false => "missed deadline".to_string(),
            };
            let real_time = finished_task.finished - finished_task.task.received;

            publish_msg(
                &client_clone,
                format!(
                    "COMPLETED {:8}. in {}ms (max {}ms, min {}ms) [{:9}] {}",
                    finished_task.task.task.name,
                    real_time.as_millis(),
                    finished_task.task.deadline,
                    finished_task.task.task.processing_time,
                    s,
                    dl
                ),
            )
            .await
            .expect("Error publishing finishing message");
        }
    });

    // priority queue, state for all worker threads
    let task_queue: Arc<Mutex<BinaryHeap<ScheduledTask>>> = Arc::new(Mutex::new(BinaryHeap::new()));
    let permits = Arc::new(Semaphore::new(NUMBER_OF_WORKERS));

    // thread dispatcher, clones for thread ownership
    let permits_clone = permits.clone();
    let task_queue_clone = task_queue.clone();
    tokio::spawn(async move {
        loop {
            let mut tasks_to_run = Vec::new();
            // empty the priority queue
            {
                let mut q = task_queue_clone.lock();
                while let Some(_task) = q.peek() {
                    // add all tasks to the queue
                    println!("Adding task to the queue. {:?}", _task.task);
                    tasks_to_run.push(q.pop().unwrap());
                }
            }

            let mut tasks: Arc<Vec<JoinHandle<()>>> = Arc::new(Vec::new());
            let mut task_clone = tasks.clone();
            // for each task that was in the queue, spawn a thread
            for scheduled_task in tasks_to_run {
                println!("Scheduling task #{}", scheduled_task.task.id);
                {
                    let txn = tx.clone();
                    let prm_clone = permits_clone.clone().acquire_owned();
                    let permit = prm_clone.await.unwrap();
                    tokio::spawn(async move {
                        println!("Awaiting lock task #{}", scheduled_task.task.id);
                        println!("Task {} is ready", scheduled_task.task.name);
                        tokio::time::sleep(Duration::from_millis(
                            scheduled_task.task.processing_time,
                        ))
                        .await;
                        let finished_task = FinishedTask {
                            task: scheduled_task,
                            finished: Instant::now(),
                        };
                        drop(permit);
                        txn.send(finished_task).unwrap();
                    });
                }
            }
            // await a short time to poll the last future
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    });

    // MQTT client loop until error
    while let Ok(event) = eventloop.poll().await {
        // listen for incoming publishings only
        if let Event::Incoming(Packet::Publish(packet)) = event {
            if let Ok(payload) = String::from_utf8(packet.payload.to_vec()) {
                if let Ok(task) = serde_json::from_str::<Task>(&payload) {
                    let mut queue = task_queue.lock();
                    let timestamp = SystemTime::now()
                        .duration_since(SystemTime::UNIX_EPOCH)
                        .unwrap()
                        .as_millis();
                    // println!("Task received {:?}", &task);

                    let st = ScheduledTask {
                        deadline: (task.deadline as i64 - timestamp as i64) as u64,
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
