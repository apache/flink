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
import logging
import os
import shutil
import sys
import tempfile

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, TableConfig
from pyflink.table.descriptors import Bucket, Json, Schema
from pyflink.table.types import DataTypes


def word_count():
    t_config = TableConfig()
    env = StreamExecutionEnvironment.get_execution_environment()
    t_env = StreamTableEnvironment.create(env, t_config)

    # register Results table in table environment
    tmp_dir = tempfile.gettempdir()
    result_path = tmp_dir + '/result'
    if os.path.exists(result_path):
        try:
            if os.path.isfile(result_path):
                os.remove(result_path)
            else:
                shutil.rmtree(result_path)
        except OSError as e:
            logging.error("Error removing directory: %s - %s.", e.filename, e.strerror)

    logging.info("Results directory: %s", result_path)

    t_env.connect(Bucket().base_path(result_path)) \
        .with_format(Json()
                     .derive_schema()
                     .field("name", DataTypes.STRING())
                     .field("age", DataTypes.BIGINT())) \
        .with_schema(Schema()
                     .field("name", DataTypes.STRING())
                     .field("age", DataTypes.BIGINT())) \
        .register_table_sink("Results")

    elements = [('tom', 10),('cat', 20)]
    t_env.from_elements(elements, ["name", "age"]) \
         .select("name, age") \
         .insert_into("Results")

    t_env.execute("word_count")


if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")

    word_count()
