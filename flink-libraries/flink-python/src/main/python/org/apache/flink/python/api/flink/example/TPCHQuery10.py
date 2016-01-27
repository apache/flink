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
from flink.functions.MapFunction import MapFunction

class OrderFilter(FilterFunction):
    def filter(self, value):
        if (datetime.strptime(value[2], "%Y-%m-%d") > datetime.strptime("1990-12-31", "%Y-%m-%d")):
            return True
        else:
            return False

class LineitemFilter(FilterFunction):
    def filter(self, value):
    	if value[3] == "R":
    		return True
    	else:
    		return False

class ComputeRevenue(MapFunction):
	def map(self, value):
		return (value[0], value[1] * (1 - value[2]))

class SumReducer(ReduceFunction):
    def reduce(self, value1, value2):
        return (value1[0], value1[1] + value2[1])

class test(JoinFunction):
	def join(self, value1, value2):
		return (value1[1], value2[1])


if __name__ == "__main__":

	env = get_environment()
	
	if len(sys.argv) != 6:
		sys.exit("Usage: ./bin/pyflink.sh TPCHQuery10 <customer path> <order path> <lineitem path> <nation path> <output path>")
	
	customer = env \
        .read_csv(sys.argv[1], 
            [INT, STRING, STRING, INT, STRING, FLOAT, STRING, STRING], '\n', '|') \
        .project(0,1,2,3,5) 

	order = env \
        .read_csv(sys.argv[2], 
            [INT, INT, STRING, FLOAT, STRING, STRING, STRING, INT, STRING], '\n', '|') \
        .project(0,1,4) \
   		.filter(OrderFilter()) \
   		.project(0,1)

	lineitem = env \
        .read_csv(sys.argv[3], [INT, INT, INT, INT, INT, FLOAT, FLOAT, FLOAT,
             STRING, STRING, STRING, STRING, STRING, STRING, STRING, STRING], '\n', '|') \
        .project(0,5,6,8) \
        .filter(LineitemFilter()) \
        .map(ComputeRevenue())

	nation = env \
    	.read_csv(sys.argv[4], [INT, STRING, INT, STRING], '\n', '|') \
    	.project(0,1)

	revenueByCustomer = order \
    	.join_with_huge(lineitem) \
    	.where(0) \
    	.equal_to(0) \
    	.project_first(1) \
    	.project_second(1)

	revenueByCustomer = revenueByCustomer \
    	.group_by(0) \
    	.reduce(SumReducer())

	customerWithNation = customer \
    	.join_with_tiny(nation) \
    	.where(3) \
    	.equal_to(0) \
    	.project_first(0,1,2) \
    	.project_second(1) \
    	.project_first(4)

	result = customerWithNation \
    	.join(revenueByCustomer) \
    	.where(0) \
    	.equal_to(0) \
    	.project_first(0,1,2,3,4) \
    	.project_second(1)

	result.write_csv(sys.argv[5], '\n', '|', WriteMode.OVERWRITE)

	env.set_parallelism(1)

	env.execute(local=True)