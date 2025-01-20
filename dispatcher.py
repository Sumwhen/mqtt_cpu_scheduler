#!/usr/bin/env python

import time
import paho.mqtt.client as mqtt

broker = "localhost"
port = 1883
topic = "scheduler"



def on_connect(client, userdata, flags, rc):
    if rc == 0:
        client.connected_flag = True
        print("connected to broker")
    else:
        print(f"Failed to connect, return code {rc}")

def on_publish(client, userdata, mid):
    print(f"Message published with message id {mid}")


def create_msg(name, seconds, deadline, id, success):
    return str({
        "id": id,
        "deadline": time.time() + (1000 * deadline),
        "success": success,
        "name": name,
        "processing_time": seconds * 1000
    })


unacked_publish = set()


client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
client.connected_flag = False
client.on_connect = on_connect
client.on_message = on_publish


client.user_data_set(unacked_publish)

print("connecting....")

client.connect(broker, port, 60)
while not client.connected_flag:
    time.sleep(1)
print("connected")


client.loop_start()


msg_info = client.publish(topic, create_msg("msg1", 0.52, 30, 1, 0.87), qos=1)
unacked_publish.add(msg_info.mid)

while len(unacked_publish):
    print("publish not ack")
    time.sleep(1)

msg_info.wait_for_publish()
client.disconnect()
client.loop_stop()



