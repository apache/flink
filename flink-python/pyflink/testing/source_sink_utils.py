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
#################################################################################

from pyflink.java_gateway import get_gateway


def results():
    """
    Retrieves the results from an append table sink.
    """
    return retract_results()


def retract_results():
    """
    Retrieves the results from a retract table sink.
    """
    gateway = get_gateway()
    results = gateway.jvm.org.apache.flink.table.utils.TestingSinks.RowCollector.getAndClearValues()
    return gateway.jvm\
        .org.apache.flink.table.utils.TestingSinks.RowCollector.retractResults(results)


def upsert_results(keys):
    """
    Retrieves the results from an upsert table sink.
    """
    gateway = get_gateway()
    j_keys = gateway.new_array(gateway.jvm.int, len(keys))
    for i in range(0, len(keys)):
        j_keys[i] = keys[i]

    results = gateway.jvm.RowCollector.getAndClearValues()
    return gateway.jvm.RowCollector.upsertResults(results, j_keys)
