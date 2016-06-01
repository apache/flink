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


class _Identifier(object):
    """
    Gotta be kept in sync with java constants!
    """
    SORT = "sort"
    GROUP = "groupby"
    COGROUP = "cogroup"
    CROSS = "cross"
    CROSSH = "cross_h"
    CROSST = "cross_t"
    FLATMAP = "flatmap"
    FILTER = "filter"
    MAPPARTITION = "mappartition"
    GROUPREDUCE = "groupreduce"
    JOIN = "join"
    JOINH = "join_h"
    JOINT = "join_t"
    MAP = "map"
    PROJECTION = "projection"
    REDUCE = "reduce"
    UNION = "union"
    SOURCE_CSV = "source_csv"
    SOURCE_TEXT = "source_text"
    SOURCE_VALUE = "source_value"
    SOURCE_SEQ = "source_seq"
    SINK_CSV = "sink_csv"
    SINK_TEXT = "sink_text"
    SINK_PRINT = "sink_print"
    BROADCAST = "broadcast"
    FIRST = "first"
    DISTINCT = "distinct"
    PARTITION_HASH = "partition_hash"
    REBALANCE = "rebalance"


class WriteMode(object):
    NO_OVERWRITE = 0
    OVERWRITE = 1


class Order(object):
    NONE = 0
    ASCENDING = 1
    DESCENDING = 2
    ANY = 3

import sys

PY2 = sys.version_info[0] == 2
PY3 = sys.version_info[0] == 3

if PY2:
    BOOL = True
    INT = 1
    LONG = long(1)
    FLOAT = 2.5
    STRING = "type"
    BYTES = bytearray(b"byte")
elif PY3:
    BOOL = True
    INT = 1
    FLOAT = 2.5
    STRING = "type"
    BYTES = bytearray(b"byte")


def _createKeyValueTypeInfo(keyCount):
    return (tuple([BYTES for _ in range(keyCount)]), BYTES)


def _createArrayTypeInfo():
    return BYTES
