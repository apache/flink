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
from pyflink.common.typeinfo import TypeInformation, Types, RowTypeInfo
from pyflink.java_gateway import get_gateway


class OutputTag(object):

    def __init__(self, tag_id: str, type_info: TypeInformation = None):
        if tag_id == "":
            raise ValueError("tag_id cannot be empty string")
        self.tag_id = tag_id
        if type_info is None:
            self.type_info = Types.PICKLED_BYTE_ARRAY()
        elif isinstance(type_info, list):
            self.type_info = RowTypeInfo(type_info)
        else:
            self.type_info = type_info

    def get_java_output_tag(self):
        gateway = get_gateway()
        j_obj = gateway.jvm.org.apache.flink.util.OutputTag(self.tag_id,
                                                            self.type_info.get_java_type_info())
        self.type_info._j_typeinfo = None
        return j_obj
