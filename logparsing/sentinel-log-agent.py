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
from datetime import datetime
from kafka import KafkaProducer
from pygtail import Pygtail
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


def send_msg(target_key, msg):
    kafka_producer.send(get_element_value("sentinel", "topic"), key=target_key, value=msg)


def parse_elements(log_pattern, line):
    values = dict()
    map_keys = get_elements("mapping")
    for key in map_keys:
        values[key] = []

    pattern_elements = str.split(log_pattern)
    line_elements = str.split(line)

    pos = 0;
    for pattern in pattern_elements:
        if pos < len(line_elements):
            for key in map_keys:
                conf_value = get_element_value("mapping", key)
                if conf_value in pattern or pattern in conf_value:
                    if "msg" in pattern:
                        # go till end of the line
                        temp = str(line_elements[pos]).strip()
                        if len(temp) > 0:
                            values[key].append(temp)
                        while pos < len(line_elements) - 1 :
                            pos += 1
                            temp = str(line_elements[pos]).strip()
                            if len(temp) > 0:
                                values[key].append(temp)
                    else:
                        temp = str(line_elements[pos]).strip()
                        if len(temp) > 0:
                            values[key].append(temp)
        pos += 1

    return values


if __name__ == '__main__':
    log_pattern = get_element_value("target-file", "logpattern")
    while True:
        for line in Pygtail(get_element_value("target-file", "filepath")):
            values = parse_elements(log_pattern, line)
            aggregated_values = dict()
            map_keys = get_elements("mapping")
            for key in map_keys:
                aggregated_values[key] = ""
                for element in values[key]:
                    if key == "unixtime":
                        if element != "-":
                            aggregated_values[key] += element + " "
                    else:
                        aggregated_values[key] += element + " "
            unix_timestamp = datetime.strptime(str.rstrip(aggregated_values["unixtime"]), '%Y-%m-%d %H:%M:%S')
            msg_to_send = ""
            if "unixtime:ms" in get_element_value("target-file", "seriesPattern"):
                msg_to_send += "unixtime:" + str(unix_timestamp.timestamp() * 1000)
            elif "unixtime:s" in get_element_value("target-file", "seriesPattern"):
                msg_to_send += "unixtime:" + str(unix_timestamp.timestamp())
            elif "unixtime:ns" in get_element_value("target-file", "seriesPattern"):
                msg_to_send += "unixtime:" + str(unix_timestamp.timestamp() * 1000000)
            elif "unixtime:us" in get_element_value("target-file", "seriesPattern"):
                msg_to_send += "unixtime:" + str(unix_timestamp.timestamp() * 1000000000)
            for key in map_keys:
                if key == "unixtime":
                    continue
                msg_to_send += " " + key + ":" + str(aggregated_values[key]).strip().replace(" ", "_").replace(":", "@")
            send_msg(get_element_value("target-file", "seriesName"), msg_to_send)
            # print(aggregated_values)
        time.sleep(1)
