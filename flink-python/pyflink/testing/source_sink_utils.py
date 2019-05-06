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

import glob
import os
import unittest
from py4j.java_gateway import java_import

from pyflink.find_flink_home import _find_flink_source_root
from pyflink.java_gateway import get_gateway
from pyflink.table import TableSink


class TestTableSink(TableSink):
    """
    Base class for test table sink.
    """

    def __init__(self, j_table_sink_name, j_table_sink):
        FLINK_SOURCE_ROOT_DIR = _find_flink_source_root()
        filename_pattern = (
            "flink-table/flink-table-planner/target/"
            "flink-table-planner*-tests.jar")
        if not glob.glob(os.path.join(FLINK_SOURCE_ROOT_DIR, filename_pattern)):
            raise unittest.SkipTest(
                "'" + j_table_sink_name + "' is not available. Will skip the related tests.")

        super(TestTableSink, self).__init__(j_table_sink)


class TestAppendSink(TestTableSink):
    """
    A test append table sink.
    """

    def __init__(self):
        gateway = get_gateway()
        j_table_sink_name = "org.apache.flink.table.runtime.stream.table.TestAppendSink"
        java_import(gateway.jvm, j_table_sink_name)

        j_test_append_sink = gateway.jvm.TestAppendSink()
        super(TestAppendSink, self).__init__(j_table_sink_name, j_test_append_sink)


class TestRetractSink(TestTableSink):
    """
    A test retract table sink.
    """

    def __init__(self):
        gateway = get_gateway()
        j_table_sink_name = "org.apache.flink.table.runtime.stream.table.TestRetractSink"
        java_import(gateway.jvm, j_table_sink_name)

        j_test_retractsink = gateway.jvm.TestRetractSink()
        super(TestRetractSink, self).__init__(j_table_sink_name, j_test_retractsink)


class TestUpsertSink(TestTableSink):
    """
    A test upsert table sink.
    """

    def __init__(self, keys, is_append_only):
        gateway = get_gateway()
        j_table_sink_name = "org.apache.flink.table.runtime.stream.table.TestUpsertSink"
        java_import(gateway.jvm, j_table_sink_name)

        j_keys = gateway.new_array(gateway.jvm.String, len(keys))
        for i in xrange(0, len(keys)):
            j_keys[i] = keys[i]

        j_test_upsert_sink = gateway.jvm.TestUpsertSink(j_keys, is_append_only)
        super(TestUpsertSink, self).__init__(j_table_sink_name, j_test_upsert_sink)


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
    java_import(gateway.jvm, "org.apache.flink.table.runtime.stream.table.RowCollector")

    results = gateway.jvm.RowCollector.getAndClearValues()
    return gateway.jvm.RowCollector.retractResults(results)


def upsert_results(keys):
    """
    Retrieves the results from an upsert table sink.
    """
    gateway = get_gateway()
    java_import(gateway.jvm, "org.apache.flink.table.runtime.stream.table.RowCollector")

    j_keys = gateway.new_array(gateway.jvm.int, len(keys))
    for i in xrange(0, len(keys)):
        j_keys[i] = keys[i]

    results = gateway.jvm.RowCollector.getAndClearValues()
    return gateway.jvm.RowCollector.upsertResults(results, j_keys)
