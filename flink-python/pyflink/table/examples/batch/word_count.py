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

from pyflink.table import TableEnvironment, TableConfig
from pyflink.table.table_sink import CsvTableSink
from pyflink.table.table_source import CsvTableSource
from pyflink.table.types import DataTypes


# TODO: the word_count.py is just a test example for CLI.
#  After pyflink have aligned Java Table API Connectors, this example will be improved.
def word_count():
    tmp_dir = tempfile.gettempdir()
    source_path = tmp_dir + '/streaming.csv'
    if os.path.isfile(source_path):
        os.remove(source_path)
    content = "line Licensed to the Apache Software Foundation ASF under one " \
              "line or more contributor license agreements See the NOTICE file " \
              "line distributed with this work for additional information " \
              "line regarding copyright ownership The ASF licenses this file " \
              "to you under the Apache License Version the " \
              "License you may not use this file except in compliance " \
              "with the License"

    with open(source_path, 'w') as f:
        for word in content.split(" "):
            f.write(",".join([word, "1"]))
            f.write("\n")
            f.flush()
        f.close()

    t_config = TableConfig.Builder().as_batch_execution().build()
    t_env = TableEnvironment.create(t_config)

    field_names = ["word", "cout"]
    field_types = [DataTypes.STRING(), DataTypes.BIGINT()]

    # register Orders table in table environment
    t_env.register_table_source(
        "Word",
        CsvTableSource(source_path, field_names, field_types))

    # register Results table in table environment
    tmp_dir = tempfile.gettempdir()
    tmp_csv = tmp_dir + '/streaming2.csv'
    if os.path.isfile(tmp_csv):
        os.remove(tmp_csv)

    t_env.register_table_sink(
        "Results",
        field_names, field_types, CsvTableSink(tmp_csv))

    t_env.scan("Word") \
        .group_by("word") \
        .select("word, count(1) as count") \
        .insert_into("Results")

    t_env.execute()


if __name__ == '__main__':
    word_count()
