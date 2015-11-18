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
from flink.plan.Environment import get_environment
from flink.plan.Constants import BOOL, STRING
from flink.functions.MapPartitionFunction import MapPartitionFunction


class Verify(MapPartitionFunction):
    def __init__(self, msg):
        super(Verify, self).__init__()
        self.msg = msg

    def map_partition(self, iterator, collector):
        if self.msg is None:
            return
        else:
            raise Exception("Type Deduction failed: " + self.msg)

if __name__ == "__main__":
    env = get_environment()

    d1 = env.from_elements(("hello", 4, 3.2, True))

    d2 = env.from_elements("world")

    direct_from_source = d1.filter(lambda x:True)

    msg = None

    if direct_from_source._info.types != ("hello", 4, 3.2, True):
        msg = "Error deducting type directly from source."

    from_common_udf = d1.map(lambda x: x[3], BOOL).filter(lambda x:True)

    if from_common_udf._info.types != BOOL:
        msg = "Error deducting type from common udf."

    through_projection = d1.project(3, 2).filter(lambda x:True)

    if through_projection._info.types != (True, 3.2):
        msg = "Error deducting type through projection."

    through_default_op = d1.cross(d2).filter(lambda x:True)

    if through_default_op._info.types != (("hello", 4, 3.2, True), "world"):
        msg = "Error deducting type through default J/C." +str(through_default_op._info.types)

    through_prj_op = d1.cross(d2).project_first(1, 0).project_second().project_first(3, 2).filter(lambda x:True)

    if through_prj_op._info.types != (4, "hello", "world", True, 3.2):
        msg = "Error deducting type through projection J/C. "+str(through_prj_op._info.types)


    env = get_environment()

    env.from_elements("dummy").map_partition(Verify(msg), STRING).output()

    env.execute(local=True)
