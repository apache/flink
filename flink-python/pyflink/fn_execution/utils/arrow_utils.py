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

"""Arrow-native scalar UDF result contracts shared by the Python and compiled coders."""


def to_arrow_schema(row_type):
    import pyarrow as pa
    from pyflink.table.types import ArrayType, MapType, RowType, to_arrow_type

    def field(name, data_type):
        if isinstance(data_type, RowType):
            arrow_type = pa.struct([field(f.name, f.data_type) for f in data_type.fields])
        elif isinstance(data_type, ArrayType):
            arrow_type = pa.list_(field("element", data_type.element_type))
        elif isinstance(data_type, MapType):
            arrow_type = pa.map_(field("key", data_type.key_type).with_nullable(False),
                                 field("value", data_type.value_type))
        else:
            arrow_type = to_arrow_type(data_type)
        return pa.field(name, arrow_type, nullable=data_type._nullable)

    return pa.schema([field(f.name, f.data_type) for f in row_type.fields])


def validate_arrow_batch(batch, schema, field_types):
    import pyarrow as pa

    if not isinstance(batch, pa.RecordBatch):
        raise TypeError("Arrow transport expects a pyarrow.RecordBatch.")
    if batch.num_columns != len(schema):
        raise ValueError(f"Arrow result has {batch.num_columns} columns, expected {len(schema)}.")
    for column, field, data_type in zip(batch.columns, schema, field_types):
        _validate_array(column, field.type, data_type, field.name)
    return pa.RecordBatch.from_arrays(batch.columns, schema=schema)


def _validate_array(column, expected_type, data_type, path):
    import pyarrow as pa
    from pyflink.table.types import ArrayType, MapType, RowType

    def wrong_type():
        raise TypeError(
            f"Arrow result field '{path}' has type {column.type}, expected {expected_type}.")

    if not data_type._nullable and column.null_count:
        raise ValueError(f"Arrow result field '{path}' is not nullable.")
    if isinstance(data_type, RowType):
        if not pa.types.is_struct(column.type) or column.type.num_fields != len(data_type.fields):
            wrong_type()
        # Children hidden by a null parent are not logical values and may contain nulls.
        visible = column.filter(column.is_valid()) if column.null_count else column
        for index, field in enumerate(data_type.fields):
            if column.type[index].name != field.name:
                wrong_type()
            _validate_array(visible.field(index), expected_type[index].type,
                            field.data_type, f"{path}.{field.name}")
    elif isinstance(data_type, ArrayType):
        if not pa.types.is_list(column.type):
            wrong_type()
        # flatten respects the slice offsets and excludes values under null lists.
        _validate_array(column.flatten(), expected_type.value_type,
                        data_type.element_type, f"{path}[]")
    elif isinstance(data_type, MapType):
        if not pa.types.is_map(column.type):
            wrong_type()
        visible = column.filter(column.is_valid()) if column.null_count else column
        start, end = visible.offsets[0].as_py(), visible.offsets[-1].as_py()
        _validate_array(visible.keys.slice(start, end - start), expected_type.key_type,
                        data_type.key_type.not_null(), f"{path}.key")
        _validate_array(visible.items.slice(start, end - start), expected_type.item_type,
                        data_type.value_type, f"{path}.value")
    elif column.type != expected_type:
        wrong_type()


def check_arrow_udf_result(func, *args, result_type=None, arrow_type=None):
    import pyarrow as pa

    result = func(*args)
    name = getattr(func, "__qualname__", type(func).__name__)
    if not isinstance(result, (pa.Array, pa.ChunkedArray)):
        raise TypeError(
            f"Arrow UDF '{name}' must return a pyarrow.Array or pyarrow.ChunkedArray, "
            f"got {type(result).__name__}.")
    for arg in args:
        if isinstance(arg, (pa.Array, pa.ChunkedArray)) and len(result) != len(arg):
            raise ValueError(
                f"Arrow UDF '{name}' returned {len(result)} rows, expected {len(arg)}.")
    if result_type is not None:
        chunks = result.chunks if isinstance(result, pa.ChunkedArray) else [result]
        # An empty ChunkedArray still has an element type that must match the declaration.
        for chunk in chunks or [pa.array([], type=result.type)]:
            _validate_array(chunk, arrow_type, result_type, name)
    return result


def create_arrow_batch(results, row_count):
    import pyarrow as pa

    columns = []
    for result in results:
        if len(result) != row_count:
            raise ValueError(f"Arrow UDF returned {len(result)} rows, expected {row_count}.")
        columns.append(result.combine_chunks() if isinstance(result, pa.ChunkedArray) else result)
    return pa.RecordBatch.from_arrays(columns, names=[f"f{i}" for i in range(len(columns))])
