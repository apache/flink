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
import json
import logging
import sys

from pyflink.common import Types
from pyflink.datastream import StreamExecutionEnvironment


def show(ds, env):
    ds.print()
    env.execute()


def basic_operations():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    # define the source
    ds = env.from_collection(
        collection=[
            (1, '{"name": "Flink", "tel": 123, "addr": {"country": "Germany", "city": "Berlin"}}'),
            (2, '{"name": "hello", "tel": 135, "addr": {"country": "China", "city": "Shanghai"}}'),
            (3, '{"name": "world", "tel": 124, "addr": {"country": "USA", "city": "NewYork"}}'),
            (4, '{"name": "PyFlink", "tel": 32, "addr": {"country": "China", "city": "Hangzhou"}}')
        ],
        type_info=Types.ROW_NAMED(["id", "info"], [Types.INT(), Types.STRING()])
    )

    # map
    def update_tel(data):
        # parse the json
        json_data = json.loads(data.info)
        json_data['tel'] += 1
        return data.id, json.dumps(json_data)

    show(ds.map(update_tel), env)
    # (1, '{"name": "Flink", "tel": 124, "addr": {"country": "Germany", "city": "Berlin"}}')
    # (2, '{"name": "hello", "tel": 136, "addr": {"country": "China", "city": "Shanghai"}}')
    # (3, '{"name": "world", "tel": 125, "addr": {"country": "USA", "city": "NewYork"}}')
    # (4, '{"name": "PyFlink", "tel": 33, "addr": {"country": "China", "city": "Hangzhou"}}')

    # filter
    show(ds.filter(lambda data: data.id == 1).map(update_tel), env)
    # (1, '{"name": "Flink", "tel": 124, "addr": {"country": "Germany", "city": "Berlin"}}')

    # key by
    show(ds.map(lambda data: (json.loads(data.info)['addr']['country'],
                              json.loads(data.info)['tel']))
           .key_by(lambda data: data[0]).sum(1), env)
    # ('Germany', 123)
    # ('China', 135)
    # ('USA', 124)
    # ('China', 167)


if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")

    basic_operations()
