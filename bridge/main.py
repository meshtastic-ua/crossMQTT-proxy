import logging
import threading
import time
import typing as t

from threading import Thread

import google.protobuf.json_format as json_format
import paho.mqtt.client as mqtt

from meshtastic import mqtt_pb2 as mqtt_pb2


# Time between packets when forwarding is blocked
PACKET_BLOCK_TIME = 1800
# Queue text packets to check duplicates
PACKET_BLOCK_QUEUE = 10
# Node and packets main database
storage_msg: dict[str, t.Any] = dict()

# Connection parametrs
mqtt_pr = {
    'kyiv': {'server': '', 'user': '', 'passwd': '', 'topic': "msh/2/c/LongFast/", 'id': '!ffffff01'},
    'odessa': {'server': '', 'user': '', 'passwd': '', 'topic': "msh/2/c/LongFast/", 'id': '!ffffff02'},
}
clients: dict[str, "MqttListener"] = dict()


class MqttListener(Thread):

    def __init__(self, mqtt_param, serv_name):
        super().__init__()
        self.mqtt_param = mqtt_param
        self.serv_name = serv_name

    def publish(self, msg):
        """#Publish to MQTT"""
        for s in mqtt_pr:
            if s != self.serv_name:
                client = mqtt_pr[s]['client']
                result = client.publish(mqtt_pr[s]['topic'] + mqtt_pr[s]['id'], msg)
                status = result[0]
                if status != 0:
                    logging.info("%s send status %s", s, status)

    # Check recived packet function
    def check_recived_pack(self, client, userdata, msg):
        unix_time = int(time.time())
        try:
            m = mqtt_pb2.ServiceEnvelope().FromString(msg.payload)
            asDict = json_format.MessageToDict(m)
            portnum = asDict['packet']['decoded']['portnum']
            id = asDict['packet']['id']
            from_node = asDict['packet']['from']
            if not (from_node in storage_msg.keys()):
                storage_msg[from_node] = {portnum: {'id': [id], 'time': unix_time}}
                self.publish(msg.payload)
            else:
                if portnum in storage_msg[from_node].keys():
                    node_base = storage_msg[from_node][portnum]
                    if portnum in ['TEXT_MESSAGE_APP', 'TRACEROUTE_APP', 'ROUTING_APP']:
                        if id not in node_base['id']:
                            node_base['id'].append(id)
                            node_base['time'] = unix_time
                            logging.info("text msg from %s", self.serv_name)
                            self.publish(msg.payload)
                            if len(node_base['id']) > PACKET_BLOCK_QUEUE:
                                node_base['id'].pop(0)
                                logging.info("popped %s", node_base['id'])
                    else:
                        if (unix_time - node_base['time']) > PACKET_BLOCK_TIME:
                            node_base['id'].append(id)
                            node_base['time'] = unix_time
                            self.publish(msg.payload)
                            if len(node_base['id']) > PACKET_BLOCK_QUEUE:
                                node_base['id'].pop(0)

                else:
                    storage_msg[from_node][portnum] = {'id': [id], 'time': unix_time}
                    self.publish(msg.payload)
        except Exception:
            logging.error("MQTT store & forward failed")
            return

    def on_connect(self, client, userdata, flags, rc):
        """Connection callback"""
        logging.info("Connected with result code %s", rc)
        client.subscribe(mqtt_pr[self.serv_name]['topic'] + "#")

    def on_message(self, client, userdata, msg):
        """Received MQTT message callback"""
        receiver_thread = threading.Thread(
            target=self.check_recived_pack,
            args=(
                client,
                userdata,
                msg,
            ),
        )
        receiver_thread.start()

    def run(self):
        client = mqtt.Client()
        client.on_connect = self.on_connect
        client.on_message = self.on_message
        client.username_pw_set(self.mqtt_param['user'], self.mqtt_param['passwd'])
        client.connect(self.mqtt_param['server'], 1883, 60)
        mqtt_pr[self.serv_name]['client'] = client
        client.loop_forever()


if __name__ == "__main__":
    for name, config in mqtt_pr.items():
        logging.info("Creating Thread for %s", name)
        clients[name] = MqttListener(mqtt_pr[name], name)
        clients[name].start()
    while True:
        for name in mqtt_pr:
            if not clients[name].is_alive():
                logging.info("Restarting Thread for %s", name)
                clients[name] = MqttListener(mqtt_pr[name], name)
                clients[name].start()
        time.sleep(60)
