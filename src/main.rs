use rand::Rng;
use rumqttc::{AsyncClient, ClientError, Event, MqttOptions, Packet, QoS};
use serde::Deserialize;
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::sync::{Arc, Mutex};
use tokio::runtime::Runtime;
use tokio::time::{self, Duration, Instant};

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
        Some(self.cmp(&other))
    }
}

impl Ord for Task {
    fn cmp(&self, other: &Self) -> Ordering {
        other.deadline.cmp(&self.deadline)
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

    let thread_pool: Runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(NUMBER_OF_WORKERS)
        .enable_time()
        .build().unwrap();


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
                    println!("TAKING FROM QUEUE: {} - {}", task.name, task.processing_time);
                    if task.processing_time > task.deadline {
                        // client.publish(PUBLISH_TOPIC, QoS::AtLeastOnce, false,
                        //                format!("impossible task {} dropped", task.name))
                        // .await.unwrap();
                        println!("IMPOSSIBLE TASK{} - {}", task.name, task.processing_time);
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
                thread_pool.spawn(async move {

                    //publish_msg(&client_clone,
                    //            format!("Task spawned: \n {:#?}", scheduled_task))
                    //    .await.expect("Error with publishing spawning message");
                    let future = time::sleep(Duration::from_millis(scheduled_task.processing_time));


                    let rn = random_percent();

                    let s = if rn <= scheduled_task.success {"succeeded".to_string()}
                        else {"failed".to_string()};

                    future.await;
                    let dl = match now.elapsed().as_millis() < scheduled_task.deadline as u128 {
                        true => "completed in time".to_string(),
                        false => "missed deadline".to_string(),
                    };
                    publish_msg(&client_clone,
                                format!("COMPLETED {:8}. [{:9}] {}", scheduled_task.name, s, dl))
                        .await.expect("Error publishing finishing message");
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