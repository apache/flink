.. ################################################################################
     Licensed to the Apache Software Foundation (ASF) under one
     or more contributor license agreements.  See the NOTICE file
     distributed with this work for additional information
     regarding copyright ownership.  The ASF licenses this file
     to you under the Apache License, Version 2.0 (the
     "License"); you may not use this file except in compliance
     with the License.  You may obtain a copy of the License at

         http://www.apache.org/licenses/LICENSE-2.0

     Unless required by applicable law or agreed to in writing, software
     distributed under the License is distributed on an "AS IS" BASIS,
     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
     See the License for the specific language governing permissions and
    limitations under the License.
   ################################################################################


===========
Expressions
===========

.. currentmodule:: pyflink.table.expressions

.. autosummary::
    :toctree: api/

    col
    lit
    range_
    and_
    or_
    not_
    current_database
    current_date
    current_time
    current_timestamp
    current_watermark
    local_time
    local_timestamp
    to_date
    to_timestamp
    to_timestamp_ltz
    temporal_overlaps
    date_format
    timestamp_diff
    convert_tz
    from_unixtime
    unix_timestamp
    array
    row
    map_
    row_interval
    pi
    e
    rand
    rand_integer
    atan2
    negative
    concat
    concat_ws
    uuid
    null_of
    log
    source_watermark
    if_then_else
    coalesce
    with_columns
    without_columns
    json_string
    json_object
    json_object_agg
    json_array
    json_array_agg
    call
    call_sql