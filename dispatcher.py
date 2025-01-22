#!/usr/bin/env python

import json
import time
import random
import argparse
import paho.mqtt.client as mqtt

broker = "localhost"
port = 1883

# config



# argparse:
parser = argparse.ArgumentParser(
    prog="mqtt random msg dispatcher"
)

parser.add_argument('-t', '--topic', type=str,
    nargs='?',
    help='topic to publish to')
parser.add_argument('-n', '--nmsg', type=int,
    nargs='?',
    help='number of messages to publish')

parser.add_argument('-s', '--sleep', type=int,
    nargs='?',
    help='timeout between publishings')

parser.add_argument('-b', '--batch_size', type=int,
    nargs='?',
    help='with batchsize provided, msgs are published in b batches of size n')

parser.add_argument('--seed', nargs='?',
    help='seed to be used')

args = parser.parse_args()

topic = "schedule" if not args.topic else args.topic
batch_size = 0 if not args.batch_size else args.batch_size
seed = random.randint(0, 1000) if not args else args.seed
sleep = 100 if not args.sleep else args.sleep
n_msg = 5 if not args.nmsg else args.nmsg


print(f"SEED {seed}")
if batch_size:
    print(f"running {batch_size} batches, {n_msg} each")
print(f"timeout between {'batches' if batch_size else 'publishings'}: {sleep} s")
print(f"publishing total {batch_size * n_msg if batch_size else n_msg} messages to '{topic}''")


def on_connect(client, userdata, flags, rc, properties):
    if rc == 0:
        client.connected_flag = True
        print("connected to broker")
    else:
        print(f"Failed to connect, return code {rc}")

def on_publish(client, userdata, mid, reason_code, properties):
    print(f"Message published with message id {mid}")


def create_msg(name, seconds, deadline, id, success):
    return json.dumps({
        "id": id,
        "deadline": time.time() + (1000 * deadline),
        "success": success,
        "name": name,
        "processing_time": seconds * 1000
    })
msg_id = 1
def random_msg():
    global msg_id
    name = "random_000"+str(msg_id)
    deadline = random.random() * 5
    success = random.random() * 0.98
    processing_time = min(random.random() * 2, deadline * 0.9)

    msg_id =+ 1
    return create_msg(name, processing_time, deadline, msg_id - 1, success)



unacked_publish = set()

client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
client.connected_flag = False
client.on_connect = on_connect
client.on_publish = on_publish


client.user_data_set(unacked_publish)

print("connecting....")

client.connect(broker, port, 60)


client.loop_start()

if not batch_size:
    msg_infos = []
    for i in range(n_msg):
        msg_infos.append(client.publish(topic, random_msg(), qos=1))
        time.sleep(sleep / 1000)

    for info in msg_infos:
        info.wait_for_publish()

    print(f"published {n_msg} messages")

else:
    for i in range(b):
        print(f"BRANCH {i} of {b}")
        msg_infos = []
        for n in range(n_msg):
            msg_infos.append(client.publish(topic, random_msg(), qos=1))

        for i in msg_infos:
            i.wait_for_publish()

        time.sleep(sleep / 1000)

    print(f"published {n_msg * b} messages")


client.disconnect()
client.loop_stop()



