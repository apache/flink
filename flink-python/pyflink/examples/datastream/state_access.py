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
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import ValueStateDescriptor


class Sum(KeyedProcessFunction):

    def __init__(self):
        self.state = None

    def open(self, runtime_context: RuntimeContext):
        state_descriptor = ValueStateDescriptor("state", Types.PICKLED_BYTE_ARRAY())
        self.state = runtime_context.get_state(state_descriptor)

    def process_element(self, value, ctx: 'KeyedProcessFunction.Context'):
        # retrieve the current count
        current = self.state.value()
        if current is None:
            current = 0

        # update the state's count
        current += value[1]
        self.state.update(current)

        yield value[0], current


def state_access_demo():
    env = StreamExecutionEnvironment.get_execution_environment()

    ds = env.from_collection(
        collection=[
            ('Alice', 110.1),
            ('Bob', 30.2),
            ('Alice', 20.0),
            ('Bob', 53.1),
            ('Alice', 13.1),
            ('Bob', 3.1),
            ('Bob', 16.1),
            ('Alice', 20.1)
        ])

    # apply the process function onto a keyed stream
    ds.key_by(lambda value: value[0]) \
      .process(Sum()) \
      .print()

    # submit for execution
    env.execute()


if __name__ == '__main__':
    state_access_demo()
