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
from pyflink.table.types import DataType, LocalZonedTimestampType
from pyflink.util.utils import to_jarray


def pandas_to_arrow(schema, timezone, field_types, series):
    import pyarrow as pa

    def create_array(s, t):
        try:
            return pa.Array.from_pandas(s, mask=s.isnull(), type=t)
        except pa.ArrowException as e:
            error_msg = "Exception thrown when converting pandas.Series (%s) to " \
                        "pyarrow.Array (%s)."
            raise RuntimeError(error_msg % (s.dtype, t), e)

    arrays = [create_array(
        tz_convert_to_internal(series[i], field_types[i], timezone),
        schema.types[i]) for i in range(0, len(schema))]
    return pa.RecordBatch.from_arrays(arrays, schema)


def arrow_to_pandas(timezone, field_types, batches):
    import pyarrow as pa
    table = pa.Table.from_batches(batches)
    return [tz_convert_from_internal(c.to_pandas(date_as_object=True), t, timezone)
            for c, t in zip(table.itercolumns(), field_types)]


def tz_convert_from_internal(s, t: DataType, local_tz):
    """
    Converts the timestamp series from internal according to the specified local timezone.

    Returns the same series if the series is not a timestamp series. Otherwise,
    returns a converted series.
    """
    if type(t) == LocalZonedTimestampType:
        return s.dt.tz_localize(local_tz)
    else:
        return s


def tz_convert_to_internal(s, t: DataType, local_tz):
    """
    Converts the timestamp series to internal according to the specified local timezone.
    """
    if type(t) == LocalZonedTimestampType:
        from pandas.api.types import is_datetime64_dtype, is_datetime64tz_dtype
        if is_datetime64_dtype(s.dtype):
            return s.dt.tz_localize(None)
        elif is_datetime64tz_dtype(s.dtype):
            return s.dt.tz_convert(local_tz).dt.tz_localize(None)
    return s


def to_expression_jarray(exprs):
    """
    Convert python list of Expression to java array of Expression.
    """
    gateway = get_gateway()
    return to_jarray(gateway.jvm.Expression, [expr._j_expr for expr in exprs])
