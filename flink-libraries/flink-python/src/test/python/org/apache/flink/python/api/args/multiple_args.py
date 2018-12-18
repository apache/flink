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
import sys
from utils import Verify

if __name__ == "__main__":
    env = get_environment()

    d1 = env.from_elements(len(sys.argv))

    d1.map_partition(Verify([3], "MultipleArguments")).output()

    #Execution
    env.set_parallelism(1)

    env.execute(local=True)
