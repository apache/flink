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
from typing import Optional, Union

from pyflink.common.typeinfo import TypeInformation, Types, RowTypeInfo
from pyflink.java_gateway import get_gateway


class OutputTag(object):
    """
    An :class:`OutputTag` is a typed and named tag to use for tagging side outputs of an operator.

    Example:
    ::

        # Explicitly specify output type
        >>> info = OutputTag("late-data", Types.TUPLE([Types.STRING(), Types.LONG()]))
        # Implicitly wrap list to Types.ROW
        >>> info_row = OutputTag("row", [Types.STRING(), Types.LONG()])
        # Implicitly use pickle serialization
        >>> info_side = OutputTag("side")
        # ERROR: tag id cannot be empty string (extra requirement for Python API)
        >>> info_error = OutputTag("")

    """

    def __init__(self, tag_id: str, type_info: Optional[Union[TypeInformation, list]] = None):
        if not tag_id:
            raise ValueError("OutputTag tag_id cannot be None or empty string")
        self.tag_id = tag_id

        if type_info is None:
            self.type_info = Types.PICKLED_BYTE_ARRAY()
        elif isinstance(type_info, list):
            self.type_info = RowTypeInfo(type_info)
        elif not isinstance(type_info, TypeInformation):
            raise TypeError("OutputTag type_info must be None, list or TypeInformation")
        else:
            self.type_info = type_info

        self._j_output_tag = None

    def __getstate__(self):
        # prevent java object to be pickled
        self.type_info._j_typeinfo = None
        return self.tag_id, self.type_info

    def __setstate__(self, state):
        tag_id, type_info = state
        self.tag_id = tag_id
        self.type_info = type_info
        self._j_output_tag = None

    def get_java_output_tag(self):
        gateway = get_gateway()
        if self._j_output_tag is None:
            self._j_output_tag = gateway.jvm.org.apache.flink.util.OutputTag(
                self.tag_id, self.type_info.get_java_type_info()
            )
        return self._j_output_tag
