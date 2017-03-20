
# ###############################################################################
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
################################################################################
from flink.plan.Environment import get_environment
from flink.functions.MapFunction import MapFunction
from flink.functions.CrossFunction import CrossFunction
from flink.functions.JoinFunction import JoinFunction
from flink.functions.CoGroupFunction import CoGroupFunction
from flink.functions.Aggregation import Max, Min, Sum
from utils import Verify, Verify2, Id

# Test multiple jobs in one Python plan file
if __name__ == "__main__":
    env = get_environment()
    env.set_parallelism(1)

    d1 = env.from_elements(1, 6, 12)
    d1 \
        .first(1) \
        .map_partition(Verify([1], "First with multiple jobs in one Python plan file")).output()

    env.execute(local=True)

    env2 = get_environment()
    env2.set_parallelism(1)

    d2 = env2.from_elements(1, 1, 12)
    d2 \
        .map(lambda x: x * 2) \
        .map_partition(Verify([2, 2, 24], "Lambda Map with multiple jobs in one Python plan file")).output()

    env2.execute(local=True)
