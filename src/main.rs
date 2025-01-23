use rand::Rng;
use rumqttc::{AsyncClient, ClientError, Event, MqttOptions, Packet, QoS};
use serde::Deserialize;
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::fmt::format;
use std::sync::{Arc, Mutex};
use std::time::SystemTime;
use tokio::{
    sync::Semaphore,
    time::{self, Duration, Instant},
};

const TOPIC: &str = "schedule";
const PUBLISH_TOPIC: &str = "schedule/publish";
const NUMBER_OF_WORKERS: usize = 5;
const KEEPALIVE_INTERVAL: u64 = 10;

// the task description being sent over the network
#[derive(Deserialize, Debug, Clone)]
struct Task {
    id: u64,
    name: String,
    success: f32,
    deadline: u64,
    processing_time: u64,
}

// tasks are unique by ID
impl PartialEq for Task {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}
impl Eq for Task {}

// these traits are used to sort the priority queue.
// right now, EDF (earlies deadline first) ordering is used.
impl PartialOrd for Task {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.deadline.cmp(&other.deadline))
    }
}

impl Ord for Task {
    fn cmp(&self, other: &Self) -> Ordering {
        self.deadline.cmp(&other.deadline)
    }
}

#[tokio::main]
async fn main() {
    let mut mqttoptions = MqttOptions::new("scheduler-async", "localhost", 1883);
    mqttoptions.set_keep_alive(Duration::from_secs(KEEPALIVE_INTERVAL));
    let (client, mut eventloop) = AsyncClient::new(mqttoptions, 10);
    client.subscribe(TOPIC, QoS::AtMostOnce).await.unwrap();

    // priority queue, state for all threads
    let task_queue: Arc<Mutex<BinaryHeap<Task>>> = Arc::new(Mutex::new(BinaryHeap::new()));
    let semaphores = Arc::new(Semaphore::new(NUMBER_OF_WORKERS + 1));


    // thread dispatcher, clones for thread ownership
    let task_queue_clone = task_queue.clone();
    let semaphores_clone = semaphores.clone();
    tokio::spawn(async move {
        loop {
            // TODO: let deadline be a ms uint, and compare to this instant

            let now = Instant::now();

            let mut tasks_to_run = Vec::new();
            {
                let mut q = task_queue_clone.lock().unwrap();
                while let Some(task) = q.peek() {
                    // add any and all tasks rcv'ed via messages into the queue, except they are not possible
                    if task.deadline - task.processing_time >= 0
                    {
                        println!("TAKING FROM QUEUE: {} - {}", task.name, task.processing_time);
                        tasks_to_run.push(q.pop().unwrap());
                    } else {
                        // client.publish(PUBLISH_TOPIC, QoS::AtLeastOnce, false,
                        //                format!("impossible task {} dropped", task.name))
                        // .await.unwrap();
                        println!("DROPPING {} - {}", task.name, task.processing_time);
                    }
                }
            }

            for scheduled_task in tasks_to_run {


                let lock = semaphores_clone.clone().acquire_owned().await.unwrap();
                println!("Scheduling task #{}", scheduled_task.id);
                let client_clone = client.clone();


                // spawn a thread only if available
                tokio::spawn(async move {

                    publish_msg(&client_clone,
                                format!("Task spawned: \n {:#?}", scheduled_task))
                        .await.expect("Error with publishing spawning message");
                    time::sleep(Duration::from_millis(scheduled_task.processing_time)).await;
                    let timestamp = SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap();

                    let rn =random_percent();

                    let s = if rn <= scheduled_task.success {"succeeded"} else {"failed"};
                    let dl = match timestamp.as_millis() < scheduled_task.deadline as u128 {
                        true => "completed in time",
                        false => "missed deadline"
                    };
                    drop(lock);
                    publish_msg(&client_clone,
                                format!("Task {}: \nQ: {}\nDL: {}", scheduled_task.name, "NA", scheduled_task.deadline))
                        .await.expect("Error with publishing finishing message");
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
                    println!("Task received {:?}", &task);
                    queue.push(task);
                } else {
                    eprintln!("Failed to deserialize task");
                }
            } else {
                eprintln!("Failed to parse payload as UTF-8");
            }
        }
    }
}

async fn publish_msg(client: &AsyncClient, msg: String) -> Result<(), ClientError> {
    client.publish(PUBLISH_TOPIC, QoS::AtLeastOnce, false, msg).await
}

fn random_percent() -> f32 {
    let mut rng = rand::rng();
    rng.random()
}