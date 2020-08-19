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
# test pyflink shell environment
from pyflink.shell import b_env, bt_env, FileSystem, OldCsv, DataTypes, Schema

import tempfile
import os
import shutil

sink_path = tempfile.gettempdir() + '/batch.csv'
if os.path.exists(sink_path):
    if os.path.isfile(sink_path):
        os.remove(sink_path)
    else:
        shutil.rmtree(sink_path)
b_env.set_parallelism(1)
t = bt_env.from_elements([(1, 'hi', 'hello'), (2, 'hi', 'hello')], ['a', 'b', 'c'])
bt_env.connect(FileSystem().path(sink_path)) \
    .with_format(OldCsv()
                 .field_delimiter(',')
                 .field("a", DataTypes.BIGINT())
                 .field("b", DataTypes.STRING())
                 .field("c", DataTypes.STRING())) \
    .with_schema(Schema()
                 .field("a", DataTypes.BIGINT())
                 .field("b", DataTypes.STRING())
                 .field("c", DataTypes.STRING())) \
    .create_temporary_table("batch_sink")

t.select("a + 1, b, c").execute_insert("batch_sink").get_job_client().get_job_execution_result().result()

with open(sink_path, 'r') as f:
    lines = f.read()
    assert lines == '2,hi,hello\n' + '3,hi,hello\n'

print('pip_test_code.py success!')
