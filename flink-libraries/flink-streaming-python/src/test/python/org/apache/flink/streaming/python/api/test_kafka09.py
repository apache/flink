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
import sys
import threading
from org.apache.flink.api.common.functions import FlatMapFunction, ReduceFunction
from org.apache.flink.api.java.functions import KeySelector
from org.apache.flink.api.java.utils import ParameterTool
from org.apache.flink.streaming.python.connectors import PythonFlinkKafkaProducer09, PythonFlinkKafkaConsumer09
from org.apache.flink.streaming.api.functions.source import SourceFunction
from org.apache.flink.streaming.api.windowing.time.Time import milliseconds
from org.apache.flink.streaming.util.serialization import DeserializationSchema
from org.apache.flink.streaming.util.serialization import SerializationSchema

from utils import constants
from utils import utils


KAFKA_DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092"


class StringGenerator(SourceFunction):
    def __init__(self, msg, end_msg, num_iters=7000):
        self._running = True
        self._msg = msg
        self._end_msg = end_msg
        self._num_iters = num_iters
    def run(self, ctx):
        counter = 0
        while self._running and counter < self._num_iters - 1:
            counter += 1
            ctx.collect(self._msg)
        ctx.collect(self._end_msg)
    def cancel(self):
        self._running = False


class ToStringSchema(SerializationSchema):
    def serialize(self, value):
        return str(value)


class KafkaStringProducer(threading.Thread):
    def __init__(self, bootstrap_server, msg, end_msg, num_iters):
        threading.Thread.__init__(self)
        self._bootstrap_server = bootstrap_server
        self._msg = msg
        self._end_msg = end_msg
        # if self._msg[-1] != '\n': self._msg += '\n'
        # if self._end_msg[-1] != '\n': self._end_msg += '\n'
        self._num_iters = num_iters

    def run(self, flink):
        env = flink.get_execution_environment()

        stream = env.create_python_source(StringGenerator(self._msg, self._end_msg, num_iters=100))

        producer = PythonFlinkKafkaProducer09(KAFKA_DEFAULT_BOOTSTRAP_SERVERS, "kafka09-test", ToStringSchema())
        producer.set_log_failures_only(False);   # "False" by default
        producer.set_flush_on_checkpoint(True);  # "True" by default

        stream.add_sink(producer)

        env.execute()


class StringDeserializationSchema(DeserializationSchema):
    def deserialize(self, message):
        return ''.join(map(chr,message))

    def isEndOfStream(self, element):
        return str(element) == "quit"


class Tokenizer(FlatMapFunction):
    def flatMap(self, value, collector):
        for word in value.lower().split():
            collector.collect((1, word))


class Sum(ReduceFunction):
    def reduce(self, input1, input2):
        count1, val1 = input1
        count2, val2 = input2
        return (count1 + count2, val1)


class Selector(KeySelector):
    def getKey(self, input):
        return input[1]


class KafkaStringConsumer(threading.Thread):
    def __init__(self, bootstrap_server):
        threading.Thread.__init__(self)
        self._bootstrap_server = bootstrap_server

    def run(self, flink):
        parameterTool = ParameterTool.fromArgs(sys.argv[1:])
        props = parameterTool.getProperties()
        props.setProperty("bootstrap.servers", self._bootstrap_server)

        consumer = PythonFlinkKafkaConsumer09("kafka09-test", StringDeserializationSchema(), props)

        env = flink.get_execution_environment()
        env.add_java_source(consumer) \
            .flat_map(Tokenizer()) \
            .key_by(Selector()) \
            .time_window(milliseconds(100)) \
            .reduce(Sum()) \
            .output()

        env.execute()


class Main:
    def run(self, flink):
        host, port = KAFKA_DEFAULT_BOOTSTRAP_SERVERS.split(":")
        if not utils.is_reachable(host, int(port)):
            print("Kafka server is not reachable: [{}]".format(KAFKA_DEFAULT_BOOTSTRAP_SERVERS))
            return

        kafka_p = KafkaStringProducer(KAFKA_DEFAULT_BOOTSTRAP_SERVERS, "Hello World", "quit", constants.NUM_ITERATIONS_IN_TEST)
        kafka_c = KafkaStringConsumer(KAFKA_DEFAULT_BOOTSTRAP_SERVERS)

        kafka_p.start()
        kafka_c.start()

        kafka_p.join()
        kafka_c.join()


def main(flink):
    Main().run(flink)
