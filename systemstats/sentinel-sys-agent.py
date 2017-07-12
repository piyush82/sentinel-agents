# -----------------------------------------------------------------------------
# Copyright (c) 2017. ZHAW - ICCLab
#  All Rights Reserved.
#
#     Licensed under the Apache License, Version 2.0 (the "License"); you may
#     not use this file except in compliance with the License. You may obtain
#     a copy of the License at
#
#          http://www.apache.org/licenses/LICENSE-2.0
#
#     Unless required by applicable law or agreed to in writing, software
#     distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#     WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#     License for the specific language governing permissions and limitations
#     under the License.
#
#
#
#     Author: Piyush Harsh,
#     URL: piyush-harsh.info
# ------------------------------------------------------------------------------

import configparser
import time
import psutil
from datetime import datetime
from kafka import KafkaProducer

config = configparser.RawConfigParser()
config.read("sentinel-agent.conf")


def get_section():
    return config.sections()


def get_elements(section_name):
    return config[section_name]


def get_element_value(section_name, element_name):
    return config[section_name][element_name]


def get_kafka_producer(endpoint, key_serializer, value_serializer):
    if key_serializer == "StringSerializer" and value_serializer == "StringSerializer":
        return KafkaProducer(linger_ms=1, acks='all', retries=0, key_serializer=str.encode,
                             value_serializer=str.encode, bootstrap_servers=[endpoint])


kafka_producer = get_kafka_producer(get_element_value("kafka-endpoint", "endpoint"),
                                    get_element_value("kafka-endpoint", "keySerializer"),
                                    get_element_value("kafka-endpoint", "valueSerializer"))


def send_msg(msg):
    kafka_producer.send(get_element_value("sentinel", "topic"), key=get_element_value("sentinel", "seriesName"), value=msg)


if __name__ == '__main__':
    while True:
        msg_to_send = "";
        cpu_data = psutil.cpu_times()
        cpu_percent = psutil.cpu_percent(interval=None)
        ram_data = psutil.virtual_memory()
        disk_data = psutil.disk_usage('/')

        msg_to_send += "unixtime:" + str(time.time()*1000) + " cpu_user:" + str(cpu_data.user) + " cpu_system:" + \
                       str(cpu_data.system) + " cpu_idle:" + str(cpu_data.idle) + " cpu_percent:" + \
                       str(cpu_percent) + " ram_percent:" + str(ram_data.percent) + " disk_percent:" + \
                       str(disk_data.percent)
        send_msg(msg_to_send)

        # print(psutil.cpu_times())
        # print(psutil.cpu_percent(interval=None))
        # print(psutil.virtual_memory())
        # print(psutil.disk_usage('/'))
        # print(psutil.net_io_counters())
        time.sleep(int(get_element_value("agent", "period")))