################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################
import socket
import threading
import time
from org.apache.flink.api.common.functions import FlatMapFunction, ReduceFunction
from org.apache.flink.api.java.functions import KeySelector
from org.apache.flink.streaming.api.windowing.time.Time import milliseconds
from org.apache.flink.streaming.util.serialization import SerializationSchema

from utils import constants
from utils import utils


class Tokenizer(FlatMapFunction):
    def flatMap(self, value, collector):
        collector.collect((1, value))


class Sum(ReduceFunction):
    def reduce(self, input1, input2):
        count1, val1 = input1
        count2, val2 = input2
        return (count1 + count2, val1)


class Selector(KeySelector):
    def getKey(self, input):
        return input[1]


class ToStringSchema(SerializationSchema):
    def serialize(self, value):
        return "{}, {}|".format(value[0], value[1])


class SocketStringReader(threading.Thread):
    def __init__(self, host, port, expected_num_values):
        threading.Thread.__init__(self)
        self._host = host
        self._port = port
        self._expected_num_values = expected_num_values

    def run(self):
        serversocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        serversocket.bind((self._host, self._port))
        serversocket.listen(5)
        (clientsocket, address) = serversocket.accept()
        while True:
            msg = clientsocket.recv(1024)
            if not msg:
                break
            for v in msg.split('|')[:-1]:
                print(v)

        print("*** Done receiving ***")
        clientsocket.close()
        serversocket.close()


class Main:
    def run(self, flink):
        port = utils.gen_free_port()
        SocketStringReader('', port, constants.NUM_ITERATIONS_IN_TEST).start()
        time.sleep(0.5)

        elements = ["aa" if iii % 2 == 0 else "bbb" for iii in range(constants.NUM_ITERATIONS_IN_TEST)]

        env = flink.get_execution_environment()
        env.from_collection(elements) \
            .flat_map(Tokenizer()) \
            .key_by(Selector()) \
            .time_window(milliseconds(50)) \
            .reduce(Sum()) \
            .write_to_socket('localhost', port, ToStringSchema())

        env.execute()


def main(flink):
    Main().run(flink)
