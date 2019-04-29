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

import sys
from threading import RLock

from pyflink.java_gateway import get_gateway
from pyflink.table.types import DataTypes

if sys.version > '3':
    xrange = range

_data_types_mapping = None
_lock = RLock()


def to_java_type(py_type):
    global _data_types_mapping
    global _lock

    if _data_types_mapping is None:
        with _lock:
            gateway = get_gateway()
            TYPES = gateway.jvm.org.apache.flink.api.common.typeinfo.Types
            _data_types_mapping = {
                DataTypes.STRING: TYPES.STRING,
                DataTypes.BOOLEAN: TYPES.BOOLEAN,
                DataTypes.BYTE: TYPES.BYTE,
                DataTypes.CHAR: TYPES.CHAR,
                DataTypes.SHORT: TYPES.SHORT,
                DataTypes.INT: TYPES.INT,
                DataTypes.LONG: TYPES.LONG,
                DataTypes.FLOAT: TYPES.FLOAT,
                DataTypes.DOUBLE: TYPES.DOUBLE,
                DataTypes.DATE: TYPES.SQL_DATE,
                DataTypes.TIME: TYPES.SQL_TIME,
                DataTypes.TIMESTAMP: TYPES.SQL_TIMESTAMP
            }

    return _data_types_mapping[py_type]
