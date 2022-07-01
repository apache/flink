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
from pyflink.java_gateway import get_gateway


class Schema(object):
    """
    Avro Schema class contains Java org.apache.avro.Schema.

    .. versionadded:: 1.16.0
    """

    def __init__(self, j_schema):
        self._j_schema = j_schema

    @staticmethod
    def parse_string(json_schema: str) -> 'Schema':
        """
        Parse JSON string as Avro Schema.

        :param json_schema: JSON represented schema string.
        :return: the Avro Schema.
        """
        JSchema = get_gateway().jvm.org.apache.flink.avro.shaded.org.apache.avro.Schema
        return Schema(JSchema.Parser().parse(json_schema))

    @staticmethod
    def parse_file(file_path: str) -> 'Schema':
        """
        Parse a schema definition file as Avro Schema.

        :param file_path: path to schema definition file.
        :return: the Avro Schema.
        """
        jvm = get_gateway().jvm
        j_file = jvm.java.io.File(file_path)
        JSchema = jvm.org.apache.flink.avro.shaded.org.apache.avro.Schema
        return Schema(JSchema.Parser().parse(j_file))
