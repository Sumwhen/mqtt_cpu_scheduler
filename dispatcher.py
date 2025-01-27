#!/usr/bin/env python

import json
import time
import random
import argparse
import string
import paho.mqtt.client as mqtt

broker = "localhost"
port = 1883

# config



# argparse:
parser = argparse.ArgumentParser(
    prog="mqtt random msg dispatcher"
)

parser.add_argument('-t', '--topic', type=str,
    help='topic to publish to')
parser.add_argument('-n', '--nmsg', type=int,
    help='number of messages to publish')

parser.add_argument('-s', '--sleep', type=int,
    help='timeout between publishings')

parser.add_argument('-b', '--batch_size', type=int,
    help='with batchsize provided, msgs are published in b batches of size n')

parser.add_argument('--seed',
    help='seed to be used')

parser.add_argument('--id', type=int, help="starting id")

parser.add_argument('-d', '--dry', action='store_true', help='only print generated messages, do not send')

parser.add_argument("--test", type=int, help="the amount of worker threads to be tested for an optimal solution")

args = parser.parse_args()

topic = "schedule" if not args.topic else args.topic
batch_size = 0 if not args.batch_size else args.batch_size
seed = random.randint(0, 1000) if not args else args.seed
sleep = 100 if not args.sleep else args.sleep
n_msg = 5 if not args.nmsg else args.nmsg


print(f"SEED {seed}")
if batch_size:
    print(f"running {batch_size} batches, {n_msg} each")
print(f"timeout between {'batches' if batch_size else 'publishings'}: {sleep}ms")
print(f"publishing total {batch_size * n_msg if batch_size else n_msg} messages to '{topic}'")


def on_connect(client, userdata, flags, rc, properties):
    if rc == 0:
        client.connected_flag = True
        print("connected to broker")
    else:
        print(f"Failed to connect, return code {rc}")

def on_publish(client, userdata, mid, reason_code, properties):
    print(f"Message published with message id {mid}")


## creates a message to send over mqtt, adding the current timestamp to the deadline
def create_msg(name, ms, deadline, id, success):
    return json.dumps({
        "id": id, # unique id
        "deadline": deadline + int(time.time() * 1000), # ms since epoch
        "success": success, # float between 0 and 1
        "name": name, # string with name
        "processing_time": int(ms) # ms needed for processing (int)
    })

## creates a random mqtt message for our purposes with supplied id, with intervals in ms for processing
def random_msg(m_id):
    name = f"random_" + f"{str(m_id) = :0<6s}"
    deadline_min = 2000
    deadline_max = 5000 + (m_id * 500)
    # deadline in seconds
    deadline = random.random() * deadline_max + deadline_min

    processing_time_max = deadline_min
    processing_time_min = 128
    processing_time = random.random() * processing_time_max + processing_time_min
    if processing_time > deadline:
        deadline = processing_time
    success = random.random() * 0.98

    return create_msg(name, int(processing_time), int(deadline), m_id, success)

## creates twice as many tasks with half of the deadline as processing time (should finish in time
def test_optimal(workers, name):
    msgs = []
    for i in range(workers * 2):
        msgs.append(create_msg("Task "+name+"--"+str(i), 1000, 1050, i, 1.))

    return msgs

def rand_name(n):
    char_set = string.ascii_uppercase + string.ascii_lowercase
    return ''.join(random.sample(char_set*n, n))


test_name = rand_name(4)
def main():

    # paho setup
    unacked_publish = set()

    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    client.connected_flag = False
    client.on_connect = on_connect
    client.on_publish = on_publish

    client.user_data_set(unacked_publish)
    client.connect(broker, port, 60)
    client.loop_start()

    msg_id = args.id if args.id else 1


    msg_infos = []

    if args.test:
        msgs = test_optimal(args.test, test_name)
        # publish them without delay
        for msg in msgs:
            msg_infos.append(client.publish(topic, msg, qos=1))

        for info in msg_infos:
            info.wait_for_publish()

        print(f"testing for {args.test} worker threads, generated {args.test * 2} tasks")


    elif batch_size:
        for i in range(n_msg):
            msg = random_msg(msg_id)
            # print("publishing msg ", msg)
            if not args.dry:
                msg_infos.append(client.publish(topic, msg, qos=1))
            msg_id += 1
            time.sleep(sleep / 1000)

        for info in msg_infos:
            info.wait_for_publish()

        print(f"published {n_msg} messages")



    else:
        b = args.batch_size if args.batch_size else 1
        for i in range(b):
            print(f"BRANCH {i} of {b}")
            for n in range(n_msg):
                msg = random_msg(msg_id)
                if not args.dry:
                    msg_infos.append(client.publish(topic, msg, qos=1))
                else:
                    pass
                    # print("generated message", msg)
                msg_id += 1

            for ii in msg_infos:
                ii.wait_for_publish()

            time.sleep(sleep / 1000)

        print(f"published {n_msg * b} messages")


    client.disconnect()
    client.loop_stop()


if __name__ == '__main__':
    main()


