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
from flink.plan.Constants import WriteMode
from flink.functions.CoGroupFunction import CoGroupFunction
from flink.functions.FilterFunction import FilterFunction


class DocumentFilter(FilterFunction):
    def filter(self, value):
        keywords = [" editors ", " oscillations "]
        for kw in keywords:
            if kw not in value[1]:
                return False
        return True

class RankFilter(FilterFunction):
    def filter(self, value):
        return value[0] > 40

class VisitFilter(FilterFunction):
    def filter(self, value):
        return datetime.strptime(value[1], "%Y-%m-%d").year == 2007

class AntiJoinVisits(CoGroupFunction):
    def co_group(self, iterator1, iterator2, collector):
        if not iterator2.has_next():
            while iterator1.has_next():
                collector.collect(iterator1.next())

if __name__ == "__main__":

    env = get_environment()

    if len(sys.argv) != 5:
    	sys.exit("Usage: ./bin/pyflink.sh WebLogAnalysis <docments path> <ranks path> <visits path> <output path>")

    documents = env \
        .read_csv(sys.argv[1], "\n", "|") \
        .filter(DocumentFilter()) \
        .project(0)

    ranks = env \
        .read_csv(sys.argv[2], "\n", "|") \
        .filter(RankFilter())

    visits = env \
        .read_csv(sys.argv[3], "\n", "|") \
        .project(1,2) \
        .filter(VisitFilter()) \
        .project(0)

    docWithRanks = documents \
        .join(ranks) \
        .where(0) \
        .equal_to(1) \
        .project_second(0,1,2)

    result = docWithRanks \
        .co_group(visits) \
        .where(1) \
        .equal_to(0) \
        .using(AntiJoinVisits())

    result.write_csv(sys.argv[4], '\n', '|', WriteMode.OVERWRITE)

    env.set_parallelism(1)

    env.execute(local=True)