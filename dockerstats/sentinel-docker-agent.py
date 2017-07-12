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
import docker
from kafka import KafkaProducer
import time
import socket
import jsonpickle

config = configparser.RawConfigParser()
config.read("sentinel-agent.conf")
hostname = str(socket.gethostname())


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
    kafka_producer.send(get_element_value("sentinel", "topic"), key=get_element_value("sentinel", "seriesName"),
                        value=msg)


class SentinelElement:
    key = ""
    value = ""
    type = ""

    def get_json(self):
        value = "{\"key\":\"" + self.key + "\", \"value\":\"" + str(self.value) + "\", \"type\":\"" + self.type + "\"}"
        return value

    def __str__(self):
        return self.get_json()

    def __repr__(self):
        return str(self)


if __name__ == '__main__':
    base_url = get_element_value("docker", "socket")
    client = docker.DockerClient(base_url)
    container_cache = {}
    while True:
        container_collection = client.containers.list()
        msg_to_send = ""
        element_list = []
        for container in container_collection:
            container_data = {}
            sample_list = []
            container_data["id"] = container.id
            container_data["name"] = container.name
            stat = container.stats(decode=True, stream=False)
            # extracting relevant stats
            data = SentinelElement()
            data.key = "networks_eth0_rx_bytes"
            if (container.id + "_" + "networks-eth0-rx_bytes") in container_cache and \
                            container_cache[container.id + "_" + "networks-eth0-rx_bytes"] > 0:
                data.value = stat["networks"]["eth0"]["rx_bytes"] - \
                             container_cache[container.id + "_" + "networks-eth0-rx_bytes"]
                container_cache[container.id + "_" + "networks-eth0-rx_bytes"] = stat["networks"]["eth0"]["rx_bytes"]
                if data.value < 0:
                    data.value = 0
            else:
                container_cache[container.id + "_" + "networks-eth0-rx_bytes"] = stat["networks"]["eth0"]["rx_bytes"]
                data.value = 0
            data.type = "long"
            sample_list.append(data)

            data = SentinelElement()
            data.key = "networks_eth0_tx_bytes"
            if (container.id + "_" + "networks-eth0-tx_bytes") in container_cache and \
                            container_cache[container.id + "_" + "networks-eth0-tx_bytes"] > 0:
                data.value = stat["networks"]["eth0"]["tx_bytes"] - \
                             container_cache[container.id + "_" + "networks-eth0-tx_bytes"]
                container_cache[container.id + "_" + "networks-eth0-tx_bytes"] = stat["networks"]["eth0"]["tx_bytes"]
                if data.value < 0:
                    data.value = 0
            else:
                container_cache[container.id + "_" + "networks-eth0-tx_bytes"] = stat["networks"]["eth0"]["tx_bytes"]
                data.value = 0
            data.type = "long"
            sample_list.append(data)

            data = SentinelElement()
            data.key = "memory_stats_usage"
            data.value = stat["memory_stats"]["usage"]
            data.type = "long"
            sample_list.append(data)

            data = SentinelElement()
            data.key = "cpu_usage_total"
            if (container.id + "_" + "cpu_stats-cpu_usage-total_usage") in container_cache and \
                            container_cache[container.id + "_" + "cpu_stats-cpu_usage-total_usage"] > 0:
                data.value = stat["cpu_stats"]["cpu_usage"]["total_usage"] - \
                             container_cache[container.id + "_" + "cpu_stats-cpu_usage-total_usage"]
                container_cache[container.id + "_" + "cpu_stats-cpu_usage-total_usage"] = \
                    stat["cpu_stats"]["cpu_usage"]["total_usage"]
                if data.value < 0:
                    data.value = 0
            else:
                container_cache[container.id + "_" + "cpu_stats-cpu_usage-total_usage"] = \
                    stat["cpu_stats"]["cpu_usage"]["total_usage"]
                data.value = 0
            data.type = "long"
            sample_list.append(data)

            container_data["metrics"] = sample_list
            element_list.append(container_data)
        msg_dict = {}
        msg_dict["host"] = hostname
        msg_dict["unixtime"] = str(time.time()) # unix time in seconds
        msg_dict["agent"] = "sentinel-docker-agent"
        msg_dict["values"] = element_list
        msg_to_send = jsonpickle.encode(msg_dict)
        # print(msg_to_send)
        send_msg(msg_to_send)
        time.sleep(int(get_element_value("agent", "period")))