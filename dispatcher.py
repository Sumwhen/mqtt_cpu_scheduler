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

parser.add_argument('-d', '--dry', action='store_false', help='only print generated messages, do not send')

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
print(f"publishing total {batch_size * n_msg if batch_size else n_msg} messages to '{topic}'")


def on_connect(client, userdata, flags, rc, properties):
    if rc == 0:
        client.connected_flag = True
        print("connected to broker")
    else:
        print(f"Failed to connect, return code {rc}")

def on_publish(client, userdata, mid, reason_code, properties):
    print(f"Message published with message id {mid}")


def create_msg(name, ms, deadline, id, success):
    return json.dumps({
        "id": id, # unique id
        "deadline": deadline, # ms since epoch
        "success": success, # float between 0 and 1
        "name": name, # string with name
        "processing_time": int(ms) # ms needed for processing (int)
    })

# creates a random mqtt message for our purposes with supplied id, with seconds as intervals for processing
def random_msg(id):
    name = "random_000"+str(id)
    deadline_min = 2000
    deadline_max = 5000
    # deadline in seconds
    deadline = random.random() * deadline_max + deadline_min

    processing_time_max = deadline_min
    processing_time_min = 128
    processing_time = random.random() * processing_time_max + processing_time_min
    success = random.random() * 0.98

    return create_msg(name, int(processing_time), int(deadline) ,id, success)



unacked_publish = set()

client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
client.connected_flag = False
client.on_connect = on_connect
client.on_publish = on_publish


client.user_data_set(unacked_publish)

print("connecting....")

client.connect(broker, port, 60)


client.loop_start()

id = 1

if not batch_size:
    msg_infos = []
    for i in range(n_msg):
        msg = random_msg(id)
        print("publishing msg ", msg)
        if not args.dry:
            msg_infos.append(client.publish(topic, msg, qos=1))
        id += 1
        time.sleep(sleep / 1000)

    for info in msg_infos:
        info.wait_for_publish()

    print(f"published {n_msg} messages")



else:
    b = args.batch_size if args.batch_size else 1
    for i in range(b):
        print(f"BRANCH {i} of {b}")
        msg_infos = []
        for n in range(n_msg):
            msg = random_msg(id)
            if not args.dry:
                msg_infos.append(client.publish(topic, msg, qos=1))
            else:
                print("generated message", msg)
            id += 1

        for i in msg_infos:
            i.wait_for_publish()

        time.sleep(sleep / 1000)

    print(f"published {n_msg * b} messages")


client.disconnect()
client.loop_stop()



