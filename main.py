import unicodedata
from traceback import print_exception
import paho.mqtt.client as mqtt
from meshtastic import BROADCAST_ADDR, BROADCAST_NUM
from meshtastic import mqtt_pb2 as mqtt_pb2
from meshtastic import portnums_pb2 as PortNum
import google.protobuf.json_format as json_format
import threading
from threading import Thread
import calendar
import datetime
import time


#Time between packets when forwarding is blocked
PACKET_BLOCK_TIME = 1800
#Queue text packets to check duplicates
PACKET_BLOCK_QUEUE = 10

#Node and packets main database
storage_msg = {}

#Connection parametrs
mqtt_pr = {'kyiv': {'server':'','user':'','passwd':'', 'topic':"msh/2/c/LongFast/", 'id':'!ffffff01'},
            'odessa': {'server':'','user':'','passwd':'','topic':"msh/2/c/LongFast/", 'id':'!ffffff02'}
            }



class MqttListener(Thread):

    def __init__(self, mqtt_param, serv_name):
        Thread.__init__(self)
        self.mqtt_param = mqtt_param
        self.serv_name = serv_name
    
    #Publish to MQTT function
    def publish(self, msg):
        for s in mqtt_pr:
            if s != self.serv_name:
                client = mqtt_pr[s]['client']
                result = client.publish(mqtt_pr[s]['topic']+mqtt_pr[s]['id'], msg)
                status = result[0]
                if status != 0:
                    print("%s send status %s"%(s,status))

    #Check recived packet function
    def check_recived_pack(self, client, userdata, msg):
        date = datetime.datetime.utcnow()
        utc_time = calendar.timegm(date.utctimetuple())
        ma = {}
        try:
            m = mqtt_pb2.ServiceEnvelope().FromString(msg.payload)
            asDict = json_format.MessageToDict(m)
            ma = asDict
            portnum = asDict['packet']['decoded']['portnum']
            id = asDict['packet']['id']
            from_node = asDict['packet']['from']
            if not (from_node in storage_msg.keys()):
                storage_msg[from_node] = {portnum:{'id':[id], 'time': utc_time}}
                self.publish(msg.payload)
            else:
                if (portnum in storage_msg[from_node].keys()):
                    node_base = storage_msg[from_node][portnum]
                    if portnum in ['TEXT_MESSAGE_APP', 'TRACEROUTE_APP', 'ROUTING_APP']:
                        if (id not in node_base['id']):
                            node_base['id'].append(id)
                            node_base['time'] = utc_time
                            print("text msg from "+self.serv_name)
                            self.publish(msg.payload)
                            if len(node_base['id']) > PACKET_BLOCK_QUEUE:
                                node_base['id'].pop(0)
                                print("pop")
                    else:
                        if ((utc_time-node_base['time'])>PACKET_BLOCK_TIME):
                            node_base['id'].append(id)
                            node_base['time'] = utc_time
                            self.publish(msg.payload)
                            if len(node_base['id']) > PACKET_BLOCK_QUEUE:
                                node_base['id'].pop(0)

                else:
                    storage_msg[from_node][portnum] = {'id':[id], 'time': utc_time}
                    self.publish(msg.payload)
        except Exception as exc:
            print_exception(exc)
            return

    # The callback function of connection
    def on_connect(self, client, userdata, flags, rc):
        print(f"Connected with result code {rc}")
        client.subscribe(mqtt_pr[self.serv_name]['topic']+"#")

    # The callback function for received message
    def on_message(self, client, userdata, msg):
        recived_thread = threading.Thread(target=self.check_recived_pack, args=(client, userdata, msg,))
        recived_thread.start()


    def run(self):
        client = mqtt.Client()
        client.on_connect = self.on_connect
        client.on_message = self.on_message
        client.username_pw_set(self.mqtt_param['user'], self.mqtt_param['passwd'])
        client.connect(self.mqtt_param['server'], 1883, 60)
        mqtt_pr[self.serv_name]['client'] = client
        client.loop_forever()



if __name__ == '__main__':
    for i in mqtt_pr:
        print("Creating Thread for " +i)
        my_thread = MqttListener(mqtt_pr[i], i)
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
