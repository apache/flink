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
import os
import tempfile

from pyflink.find_flink_home import _find_flink_home
from pyflink.table import TableEnvironment, TableConfig
from pyflink.table.table_sink import CsvTableSink
from pyflink.table.table_source import CsvTableSource
from pyflink.table.types import DataTypes


def test_end_to_end():
    tmp_dir = tempfile.gettempdir()
    source_path = tmp_dir + '/streaming.csv'
    if os.path.isfile(source_path):
        os.remove(source_path)
    with open(source_path, 'w') as f:
        lines = '1,hi,hello\n' + '2,hi,hello\n'
        f.write(lines)
        f.close()
    _find_flink_home()
    print("using %s as FLINK_HOME..." % os.environ["FLINK_HOME"])

    t_config = TableConfig.Builder().as_streaming_execution().set_parallelism(1).build()
    t_env = TableEnvironment.get_table_environment(t_config)

    field_names = ["a", "b", "c"]
    field_types = [DataTypes.INT, DataTypes.STRING, DataTypes.STRING]

    # register Orders table in table environment
    t_env.register_table_source(
        "Orders",
        CsvTableSource(source_path, field_names, field_types))

    # register Results table in table environment
    tmp_dir = tempfile.gettempdir()
    tmp_csv = tmp_dir + '/streaming2.csv'
    if os.path.isfile(tmp_csv):
        os.remove(tmp_csv)

    t_env.register_table_sink(
        "Results",
        field_names, field_types, CsvTableSink(tmp_csv))

    t_env.scan("Orders") \
         .where("a > 0") \
         .select("a + 1, b, c") \
         .insert_into("Results")

    t_env.execute()
    with open(tmp_csv, 'r') as f:
        lines = f.read()
        assert lines == '2,hi,hello\n' + '3,hi,hello\n'
    print("test passed, the log file is under this directory: %s/log" % os.environ["FLINK_HOME"])

if __name__ == '__main__':
    test_end_to_end()
