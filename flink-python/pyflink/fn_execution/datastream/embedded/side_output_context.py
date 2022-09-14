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
from typing import Dict

from pyflink.fn_execution.embedded.converters import from_type_info_proto, DataConverter


class SideOutputContext(object):
    def __init__(self, j_side_output_context):
        self._j_side_output_context = j_side_output_context
        self._side_output_converters = (
            {tag_id: from_type_info_proto(_parse_type_info_proto(payload))
             for tag_id, payload in
             j_side_output_context.getAllSideOutputTypeInfoPayloads().items()}
        )  # type: Dict[str, DataConverter]

    def collect(self, tag_id: str, record):
        try:
            self._j_side_output_context.collectSideOutputById(
                tag_id,
                self._side_output_converters[tag_id].to_external(record))
        except KeyError:
            raise Exception("Unknown OutputTag id {0}, supported OutputTag ids are {1}".format(
                tag_id, list(self._side_output_converters.keys())))


def _parse_type_info_proto(type_info_payload):
    from pyflink.fn_execution import flink_fn_execution_pb2

    type_info = flink_fn_execution_pb2.TypeInfo()
    type_info.ParseFromString(type_info_payload)
    return type_info
