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

from datetime import datetime
from flink.plan.Environment import get_environment
from flink.plan.Constants import INT, STRING, FLOAT, WriteMode
from flink.functions.FilterFunction import FilterFunction
from flink.functions.ReduceFunction import ReduceFunction
from flink.functions.JoinFunction import JoinFunction

class CustomerFilter(FilterFunction):
    def filter(self, value):
        if value[1] == "AUTOMOBILE":
            return True
        else:
            return False

class LineitemFilter(FilterFunction):
    def filter(self, value):
        if (datetime.strptime(value[3], "%Y-%m-%d") > datetime.strptime("1995-03-12", "%Y-%m-%d")):
            return True
        else:
            return False

class OrderFilter(FilterFunction):
    def filter(self, value):
        if (datetime.strptime(value[2], "%Y-%m-%d") < datetime.strptime("1995-03-12", "%Y-%m-%d")):
            return True
        else:
            return False

class SumReducer(ReduceFunction):
    def reduce(self, value1, value2):
        return (value1[0], value1[1] + value2[1], value1[2], value1[3])

class CustomerOrderJoin(JoinFunction):
    def join(self, value1, value2):
        return (value2[0], 0.0, value2[2], value2[3])

class CustomerOrderLineitemJoin(JoinFunction):
    def join(self, value1, value2):
        return (value1[0], value2[1] * (1 - value2[2]), value1[2], value1[3])



if __name__ == "__main__":
    env = get_environment()

    if len(sys.argv) != 5:
        sys.exit("Usage: ./bin/pyflink.sh TPCHQuery3 <lineitem path> <customer path> <order path> <output path>")
    lineitem = env \
        .read_csv(sys.argv[1], [INT, INT, INT, INT, INT, FLOAT, FLOAT, FLOAT,
             STRING, STRING, STRING, STRING, STRING, STRING, STRING, STRING], '\n', '|') \
        .project(0,5,6,10) \
        .filter(LineitemFilter())


    customer = env \
        .read_csv(sys.argv[2], 
            [INT, STRING, STRING, INT, STRING, FLOAT, STRING, STRING], '\n', '|') \
        .project(0,6) \
        .filter(CustomerFilter())

    order = env \
        .read_csv(sys.argv[3], 
            [INT, INT, STRING, FLOAT, STRING, STRING, STRING, INT, STRING], '\n', '|') \
        .project(0,1,4,7) \
        .filter(OrderFilter())

    customerWithOrder = customer \
        .join(order) \
        .where(0) \
        .equal_to(1) \
        .using(CustomerOrderJoin())

    result = customerWithOrder \
        .join(lineitem) \
        .where(0) \
        .equal_to(0) \
        .using(CustomerOrderLineitemJoin()) \
        .group_by(0, 2, 3) \
        .reduce(SumReducer())

    result.write_csv(sys.argv[4], '\n', '|', WriteMode.OVERWRITE)

    env.set_parallelism(1)

    env.execute(local=True)


