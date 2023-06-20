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
    map_from_arrays
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


Expression
==========

arithmetic functions
--------------------

.. currentmodule:: pyflink.table.expression

.. autosummary::
    :toctree: api/

    Expression.exp
    Expression.log10
    Expression.log2
    Expression.ln
    Expression.log
    Expression.cosh
    Expression.sinh
    Expression.sin
    Expression.cos
    Expression.tan
    Expression.cot
    Expression.asin
    Expression.acos
    Expression.atan
    Expression.tanh
    Expression.degrees
    Expression.radians
    Expression.sqrt
    Expression.abs
    Expression.sign
    Expression.round
    Expression.between
    Expression.not_between
    Expression.then
    Expression.if_null
    Expression.is_null
    Expression.is_not_null
    Expression.is_true
    Expression.is_false
    Expression.is_not_true
    Expression.is_not_false
    Expression.distinct
    Expression.sum
    Expression.sum0
    Expression.min
    Expression.max
    Expression.count
    Expression.avg
    Expression.first_value
    Expression.last_value
    Expression.list_agg
    Expression.stddev_pop
    Expression.stddev_samp
    Expression.var_pop
    Expression.var_samp
    Expression.collect
    Expression.alias
    Expression.cast
    Expression.try_cast
    Expression.asc
    Expression.desc
    Expression.in_
    Expression.start
    Expression.end
    Expression.bin
    Expression.hex
    Expression.truncate

string functions
----------------

.. currentmodule:: pyflink.table.expression

.. autosummary::
    :toctree: api/

    Expression.substring
    Expression.substr
    Expression.trim_leading
    Expression.trim_trailing
    Expression.trim
    Expression.replace
    Expression.char_length
    Expression.upper_case
    Expression.lower_case
    Expression.init_cap
    Expression.like
    Expression.similar
    Expression.position
    Expression.lpad
    Expression.rpad
    Expression.overlay
    Expression.regexp
    Expression.regexp_replace
    Expression.regexp_extract
    Expression.from_base64
    Expression.to_base64
    Expression.ascii
    Expression.chr
    Expression.decode
    Expression.encode
    Expression.left
    Expression.right
    Expression.instr
    Expression.locate
    Expression.parse_url
    Expression.ltrim
    Expression.rtrim
    Expression.repeat
    Expression.over
    Expression.reverse
    Expression.split_index
    Expression.str_to_map

temporal functions
------------------

.. currentmodule:: pyflink.table.expression

.. autosummary::
    :toctree: api/

    Expression.to_date
    Expression.to_time
    Expression.to_timestamp
    Expression.extract
    Expression.floor
    Expression.ceil


advanced type helper functions
------------------------------

.. currentmodule:: pyflink.table.expression

.. autosummary::
    :toctree: api/

    Expression.get
    Expression.flatten
    Expression.at
    Expression.cardinality
    Expression.element
    Expression.array_concat
    Expression.array_contains
    Expression.array_distinct
    Expression.array_join
    Expression.array_position
    Expression.array_remove
    Expression.array_reverse
    Expression.array_max
    Expression.array_slice
    Expression.array_union
    Expression.map_entries
    Expression.map_keys
    Expression.map_values


time definition functions
-------------------------

.. currentmodule:: pyflink.table.expression

.. autosummary::
    :toctree: api/

    Expression.rowtime
    Expression.proctime
    Expression.year
    Expression.years
    Expression.quarter
    Expression.quarters
    Expression.month
    Expression.months
    Expression.week
    Expression.weeks
    Expression.day
    Expression.days
    Expression.hour
    Expression.hours
    Expression.minute
    Expression.minutes
    Expression.second
    Expression.seconds
    Expression.milli
    Expression.millis


hash functions
--------------

.. currentmodule:: pyflink.table.expression

.. autosummary::
    :toctree: api/

    Expression.md5
    Expression.sha1
    Expression.sha224
    Expression.sha256
    Expression.sha384
    Expression.sha512
    Expression.sha2


JSON functions
--------------

.. currentmodule:: pyflink.table.expression

.. autosummary::
    :toctree: api/

    Expression.is_json
    Expression.json_exists
    Expression.json_value
    Expression.json_query
