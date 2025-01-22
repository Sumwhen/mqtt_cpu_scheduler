use std::async_iter::AsyncIterator;
use std::borrow::Borrow;
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::sync::{Arc, Mutex};
use rumqttc::{MqttOptions, AsyncClient, Client, QoS, Event, Packet};
use tokio::{time::{self, Duration, Instant}, sync::{Semaphore}};
use rumqttc::mqttbytes::v4::Publish as PublishV4;
use serde::{Deserialize};
use rand::{Rng, SeedableRng};



const TOPIC: &str = "schedule";
const NUMBER_OF_WORKERS: usize = 5;
const KEEPALIVE_INTERVAL: u64 = 10;
const RAND_SEED: u64 = 123456789;


// the task description being sent over the network
#[derive(Deserialize, Debug, Clone)]
struct Task {
    id: u64,
    name: String,
    success: f32,
    deadline: u64,
    processing_time: u64
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
    let semaphores = Arc::new(Semaphore::new(NUMBER_OF_WORKERS));



    // thread dispatcher, clones for thread ownership
    let task_queue_clone = task_queue.clone();
    let semaphores_clone = semaphores.clone();


    tokio::spawn(async move {
        loop {
            let now = Instant::now();

            let mut tasks_to_run = Vec::new();
            {
                let mut q = task_queue_clone.lock().unwrap();
                while let Some(task) = q.peek() {
                    // add any and all tasks rcv'ed via messages into the queue
                    // TODO: drop impossible tasks
                    tasks_to_run.push(q.pop().unwrap());
                }
            }

            for scheduled_task in tasks_to_run {
                let lock = semaphores_clone.clone().acquire_owned().await.unwrap();
                // spawn a thread only if available
                tokio::spawn( async move {
                    time::sleep(Duration::from_secs(scheduled_task.processing_time)).await;
                    let timestamp = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap();

                    let mut rng = rand::rng();
                    let rn = rng.random::<f32>();
                    let success = rn <= scheduled_task.success;
                    if scheduled_task.deadline < timestamp.as_millis() as u64 {
                        println!("Task {} {} in time", scheduled_task.name, if success "succeeded" else "failed");
                    } else {
                        println!("Task {} missed deadline, {}", scheduled_task.name, if success "succeeded" else "failed");
                    }
                    drop(lock);
                });

            }
        }
    });

    // MQTT client loop until error
    while let Some(event) = eventloop.poll().await {
        // listen for incoming publishings only
        if let Ok(Event::Incoming(Packet::Publish(packet))) = event {
            if let Ok(payload) = String::from_utf8(packet.payload.to_vec()) {
                if let Ok(task) = serde_json::from_str::<Task>(&payload) {
                    let mut queue = task_queue.lock().unwrap();
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

fn start() {
    let topic = "schedule";
    let mut mqttoptions = MqttOptions::new("scheduler-sync", "localhost", 1883);
    mqttoptions.set_keep_alive(Duration::from_secs(5));

    let (mut client, mut connection) = Client::new(mqttoptions, 10);
    client.subscribe(topic, QoS::AtMostOnce).unwrap();


    // Iterate to poll the eventloop for connection progress
    for (i, notification) in connection.iter().enumerate() {
        println!("Notification = {:?}", notification);
        match notification {
            Ok(Event::Incoming(Packet::Publish(x))) => {
                println!("Received {:?}", x);
                let pl = unwrap_payload(x);
                println!("Payload = {:?}", pl);
            }
            Ok(_) => {}
            Err(e) => {}
        }
    }
}

fn unwrap_payload(p: PublishV4) -> Option<Task> {
    let pl = p.payload;
    let json = serde_json::from_slice::<Task>(pl.borrow()).unwrap();
    Some(json)
}