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
import ast

from pyflink.common.types import RowKind

from pyflink.java_gateway import get_gateway
from pyflink.table.types import DataType, LocalZonedTimestampType, Row, RowType, \
    TimeType, DateType, ArrayType, MapType, TimestampType, FloatType, RawType
from pyflink.util.java_utils import to_jarray
import datetime
import pickle


def pandas_to_arrow(schema, timezone, field_types, series):
    import pyarrow as pa
    import pandas as pd

    def create_array(s, t):
        try:
            return pa.Array.from_pandas(s, mask=s.isnull(), type=t)
        except pa.ArrowException as e:
            error_msg = "Exception thrown when converting pandas.Series (%s) to " \
                        "pyarrow.Array (%s)."
            raise RuntimeError(error_msg % (s.dtype, t), e)

    arrays = []
    for i in range(len(schema)):
        s = series[i]
        field_type = field_types[i]
        schema_type = schema.types[i]
        if type(s) == pd.DataFrame:
            array_names = [(create_array(s[s.columns[j]], field.type), field.name)
                           for j, field in enumerate(schema_type)]
            struct_arrays, struct_names = zip(*array_names)
            arrays.append(pa.StructArray.from_arrays(struct_arrays, struct_names))
        else:
            arrays.append(create_array(
                tz_convert_to_internal(s, field_type, timezone), schema_type))
    return pa.RecordBatch.from_arrays(arrays, schema)


def arrow_to_pandas(timezone, field_types, batches):
    def arrow_column_to_pandas(arrow_column, t: DataType):
        if type(t) == RowType:
            import pandas as pd
            series = [column.to_pandas(date_as_object=True).rename(field.name)
                      for column, field in zip(arrow_column.flatten(), arrow_column.type)]
            return pd.concat(series, axis=1)
        else:
            return arrow_column.to_pandas(date_as_object=True)
    import pyarrow as pa
    table = pa.Table.from_batches(batches)
    return [tz_convert_from_internal(arrow_column_to_pandas(c, t), t, timezone)
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


def pickled_bytes_to_python_converter(data, field_type: DataType):
    if isinstance(field_type, RowType):
        row_kind = RowKind(int.from_bytes(data[0], byteorder='big', signed=False))
        data = zip(list(data[1:]), field_type.field_types())
        fields = []
        for d, d_type in data:
            fields.append(pickled_bytes_to_python_converter(d, d_type))
        result_row = Row(fields)
        result_row.set_row_kind(row_kind)
        return result_row
    else:
        data = pickle.loads(data)
        if isinstance(field_type, TimeType):
            seconds, microseconds = divmod(data, 10 ** 6)
            minutes, seconds = divmod(seconds, 60)
            hours, minutes = divmod(minutes, 60)
            return datetime.time(hours, minutes, seconds, microseconds)
        elif isinstance(field_type, DateType):
            return field_type.from_sql_type(data)
        elif isinstance(field_type, TimestampType):
            return field_type.from_sql_type(int(data.timestamp() * 10**6))
        elif isinstance(field_type, MapType):
            key_type = field_type.key_type
            value_type = field_type.value_type
            zip_kv = zip(data[0], data[1])
            return dict((pickled_bytes_to_python_converter(k, key_type),
                         pickled_bytes_to_python_converter(v, value_type))
                        for k, v in zip_kv)
        elif isinstance(field_type, FloatType):
            return field_type.from_sql_type(ast.literal_eval(data))
        elif isinstance(field_type, ArrayType):
            element_type = field_type.element_type
            elements = []
            for element_bytes in data:
                elements.append(pickled_bytes_to_python_converter(element_bytes, element_type))
            return elements
        elif isinstance(field_type, RawType):
            return field_type.from_sql_type(data)
        else:
            return field_type.from_sql_type(data)
