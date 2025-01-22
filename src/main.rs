use rumqttc::Event::Incoming;
use std::ffi::c_float;
use std::borrow::Borrow;
use rumqttc::{MqttOptions, AsyncClient, Client, QoS, Event, Packet};
use std::time::Duration;
use std::thread;
use rumqttc::Packet::Publish;
use rumqttc::v5::mqttbytes::v5::Publish as PublishV5;
use rumqttc::mqttbytes::v4::Publish as PublishV4;
use threadpool::ThreadPool;
use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
struct Task {
    id: usize,
    name: String,
    success: c_float,
    deadline: usize,
    processing_time: usize
}

async fn async_start() {
    let topic = "schedule";
    let mut mqttoptions = MqttOptions::new("scheduler-async", "localhost", 1883);
    mqttoptions.set_keep_alive(Duration::from_secs(5));

    let (mut client, mut eventloop) = AsyncClient::new(mqttoptions, 10);
    client.subscribe(topic, QoS::AtMostOnce).await.unwrap();



    while let Ok(notification) = eventloop.poll().await {
        println!("Received = {:?}", notification);

    }
}
const TOPIC: &str = "schedule";
const NUMBER_OF_WORKERS: usize = 5;

fn main() {
    // read from config file
    let pool = ThreadPool::new(NUMBER_OF_WORKERS);

    start()
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
            Ok(Incoming(Publish(x))) => {
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