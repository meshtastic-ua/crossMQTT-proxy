#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# stdlib
import calendar
import datetime
import json
import time

# stdlib from
from threading import Thread
from traceback import print_exception

# 3rd party
import google.protobuf.json_format as json_format
import paho.mqtt.client as mqtt
# 3rd party from
from cryptography.hazmat.primitives.ciphers import (
    Cipher, algorithms, modes
)
from meshtastic import mqtt_pb2 as mqtt_pb2


#Time between packets when forwarding is blocked
PACKET_BLOCK_TIME = 1800
#Queue text packets to check duplicates
PACKET_BLOCK_QUEUE = 10

#Node and packets main database
storage_msg = {}



class MQTTCrypto:
    KEY = 'd4f1bb3a20290759f0bcffabcf4e6901'
    def __init__(self, key=None):
        self.key = bytes.fromhex(key) if key else bytes.fromhex(self.KEY)

    @staticmethod
    def init_nonce(fromNode, packetId):
        nonce = bytearray(16)
        nonce[:8] = struct.pack('<Q', packetId)
        nonce[8:12] = struct.pack('<I', fromNode)
        return nonce

    @staticmethod
    def decrypt(key, nonce, ciphertext):
        decryptor = Cipher(
            algorithms.AES(key),
            modes.CTR(nonce),
        ).decryptor()

        return decryptor.update(ciphertext) + decryptor.finalize()

    def decrypt_packet(self, packet):
        data = base64.b64decode(packet.get('encrypted'))
        nonce = self.init_nonce(packet.get('from'), packet.get('id'))
        r = self.decrypt(self.key, nonce, data)
        return mesh_pb2.Data().FromString(r)

    def encrypt_packet(self):
        pass

class MqttListener(Thread):
    def __init__(self, mqtt_param, serv_name, mqtt_pr):
        Thread.__init__(self)
        self.mqtt_param = mqtt_param
        self.serv_name = serv_name
        self.mqtt_pr = mqtt_pr
        self.crypto = MQTTCrypto()

    #Publish to MQTT function
    def publish(self, msg):
        for s in self.mqtt_pr:
            if s != self.serv_name:
                client = self.mqtt_pr[s]['client']
                result = client.publish(self.mqtt_pr[s]['topic'] + self.mqtt_pr[s]['id'], msg)
                status = result[0]
                if status != 0:
                    print("%s send status %s"%(s,status))

    #Check received packet function
    def check_received_pack(self, client, userdata, msg):
        date = datetime.datetime.now(datetime.UTC)
        utc_time = calendar.timegm(date.utctimetuple())
        try:
            m = mqtt_pb2.ServiceEnvelope().FromString(msg.payload)
        except Exception as exc:
            return
        try:
            full = json_format.MessageToDict(m.packet)
            is_encrypted = False
            # process encrypted messages
            if full.get('encrypted'):
                is_encrypted = True
                full['decoded'] = json_format.MessageToDict(self.crypto.decrypt_packet(full))

            # drop messages without decoded
            if not full.get('decoded', None):
               print("No decoded message in MQTT message: %s", full)
               return

            packet_id = full['id']
            from_node = full['from']
            portnum = full['decoded']['portnum']

            # drop range tests
            if portnum == 'RANGE_TEST_APP':
                print(f'Range test from {hex(from_node)} -> {self.serv_name}: {self.mqtt_param}')
                return

            if not (from_node in storage_msg.keys()):
                storage_msg[from_node] = {portnum:{'id':[packet_id], 'time': utc_time}}
                self.publish(msg.payload)
            else:
                if (portnum in storage_msg[from_node].keys()):
                    node_base = storage_msg[from_node][portnum]
                    if portnum in ['TEXT_MESSAGE_APP', 'TRACEROUTE_APP', 'ROUTING_APP']:
                        if (packet_id not in node_base['id']):
                            node_base['id'].append(packet_id)
                            node_base['time'] = utc_time
                            print("text msg from "+self.serv_name)
                            self.publish(msg.payload)
                            if len(node_base['id']) > PACKET_BLOCK_QUEUE:
                                node_base['id'].pop(0)
                                print("pop")
                    else:
                        if ((utc_time-node_base['time'])>PACKET_BLOCK_TIME):
                            node_base['id'].append(packet_id)
                            node_base['time'] = utc_time
                            self.publish(msg.payload)
                            if len(node_base['id']) > PACKET_BLOCK_QUEUE:
                                node_base['id'].pop(0)

                else:
                    storage_msg[from_node][portnum] = {'id':[packet_id], 'time': utc_time}
                    self.publish(msg.payload)
        except Exception as exc:
            print_exception(exc)
            return

    # The callback function of connection
    def on_connect(self, client, userdata, flags, reason_code, properties):
        print(f"Connected with result code {reason_code}")
        client.subscribe(self.mqtt_pr[self.serv_name]['topic']+"#")

    # The callback function for received message
    def on_message(self, client, userdata, msg):
        received_thread = Thread(target=self.check_received_pack, args=(client, userdata, msg,))
        received_thread.start()


    def run(self):
        client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, clean_session=True)
        client.on_connect = self.on_connect
        client.on_message = self.on_message
        client.username_pw_set(self.mqtt_param['user'], self.mqtt_param['passwd'])
        client.connect(self.mqtt_param['server'], 1883, 60)
        self.mqtt_pr[self.serv_name]['client'] = client
        client.loop_forever()



if __name__ == '__main__':
    mqtt_pr = json.loads(open('config.json', 'r').read())
    for i in mqtt_pr:
        print("Creating Thread for " +i)
        my_thread = MqttListener(mqtt_pr[i], i, mqtt_pr)
        my_thread.start()
        mqtt_pr[i]['thread'] = my_thread
    while(1):
        for i in mqtt_pr:
            if not (mqtt_pr[i]['thread'].is_alive()):
                print("Restart Thread")
                my_thread = MqttListener(mqtt_pr[i], i)
                my_thread.start()
                mqtt_pr[i]['thread'] = my_thread
        time.sleep(60)
