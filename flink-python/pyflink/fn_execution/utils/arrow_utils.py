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


def validate_arrow_batch(batch, schema, field_types):
    import pyarrow as pa

    if not isinstance(batch, pa.RecordBatch):
        raise TypeError("Arrow transport expects a pyarrow.RecordBatch.")
    if batch.num_columns != len(schema):
        raise ValueError(f"Arrow result has {batch.num_columns} columns, expected {len(schema)}.")
    for column, field, data_type in zip(batch.columns, schema, field_types):
        _validate_array(column, field.type, data_type, field.name)
    return pa.RecordBatch.from_arrays(batch.columns, schema=schema)


def _validate_array(column, expected_type, data_type, path, parent_validity=None):
    import pyarrow as pa
    import pyarrow.compute as pc
    from pyflink.table.types import ArrayType, MapType, RowType

    def wrong_type():
        raise TypeError(
            f"Arrow result field '{path}' has type {column.type}, expected {expected_type}.")

    if not data_type._nullable and column.null_count:
        if parent_validity is None or pc.any(
                pc.and_(parent_validity, column.is_null())).as_py():
            raise ValueError(f"Arrow result field '{path}' is not nullable.")
    if isinstance(data_type, RowType):
        if not pa.types.is_struct(column.type) or column.type.num_fields != len(data_type.fields):
            wrong_type()
        validity = _get_validity(column, parent_validity)
        for index, field in enumerate(data_type.fields):
            if column.type[index].name != field.name:
                wrong_type()
            _validate_array(column.field(index), expected_type[index].type,
                            field.data_type, f"{path}.{field.name}", validity)
    elif isinstance(data_type, ArrayType):
        if not pa.types.is_list(column.type):
            wrong_type()
        start, end = column.offsets[0].as_py(), column.offsets[-1].as_py()
        _validate_array(column.values.slice(start, end - start), expected_type.value_type,
                        data_type.element_type, f"{path}[]",
                        _get_child_validity(column, parent_validity))
    elif isinstance(data_type, MapType):
        if not pa.types.is_map(column.type):
            wrong_type()
        validity = _get_child_validity(column, parent_validity)
        start, end = column.offsets[0].as_py(), column.offsets[-1].as_py()
        _validate_array(column.keys.slice(start, end - start), expected_type.key_type,
                        data_type.key_type.not_null(), f"{path}.key", validity)
        _validate_array(column.items.slice(start, end - start), expected_type.item_type,
                        data_type.value_type, f"{path}.value", validity)
    elif column.type != expected_type:
        wrong_type()


def _get_validity(column, parent_validity):
    import pyarrow.compute as pc

    # Hidden child nulls are valid; propagate visibility instead of filtering the payload.
    if column.null_count:
        validity = column.is_valid()
        return validity if parent_validity is None else pc.and_(parent_validity, validity)
    return parent_validity


def _get_child_validity(column, parent_validity):
    import pyarrow as pa
    import pyarrow.compute as pc

    validity = _get_validity(column, parent_validity)
    if validity is None:
        return None
    # A list view also handles map entries and keeps the original offsets and value buffers.
    entries = pa.ListArray.from_arrays(column.offsets, column.values)
    return pc.take(validity, pc.list_parent_indices(entries))


def check_arrow_udf_result(func, *args):
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
    if isinstance(result, pa.ChunkedArray):
        result = result.chunk(0) if result.num_chunks == 1 else result.combine_chunks()
    return result


def create_record_batch(results, row_count):
    import pyarrow as pa

    columns = []
    for result in results:
        if len(result) != row_count:
            raise ValueError(f"Arrow UDF returned {len(result)} rows, expected {row_count}.")
        columns.append(result)
    return pa.RecordBatch.from_arrays(columns, names=[f"f{i}" for i in range(len(columns))])
