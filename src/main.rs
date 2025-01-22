use std::borrow::Borrow;
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::sync::{Arc, Mutex};
use rumqttc::{MqttOptions, AsyncClient, Client, QoS, Event, Packet};
use std::time::Duration;
use rumqttc::mqttbytes::v4::Publish as PublishV4;
use serde::{Deserialize};


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


const TOPIC: &str = "schedule";
const NUMBER_OF_WORKERS: usize = 5;


#[tokio::main]
async fn main() {
    let mut mqttoptions = MqttOptions::new("scheduler-async", "localhost", 1883);
    mqttoptions.set_keep_alive(Duration::from_secs(NUMBER_OF_WORKERS as u64));
    let (client, eventloop) = AsyncClient::new(mqttoptions, 10);
    client.subscribe(TOPIC, QoS::AtMostOnce).await.unwrap();

    // priority queue, state for all threads
    let task_queue: Arc<Mutex<BinaryHeap<SchedulerTask>>> = Arc::new(Mutex::new(BinaryHeap::new()));>


    tokio::spawn(async move {

    })
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