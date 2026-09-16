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

from functools import cache


def validate_arrow_batch(batch, schema, field_types):
    import pyarrow as pa

    if not isinstance(batch, pa.RecordBatch):
        raise TypeError("Arrow transport expects a pyarrow.RecordBatch.")
    if batch.num_columns != len(schema):
        raise ValueError(f"Arrow result has {batch.num_columns} columns, expected {len(schema)}.")
    # PyArrow 5 cannot express map-value nullability, so retain the logical constraint there.
    legacy_map_fields = not hasattr(pa.MapType, 'item_field')
    columns = []
    for index, (column, field) in enumerate(zip(batch.columns, schema)):
        _validate_array(column, field, field.name,
                        data_type=field_types[index] if legacy_map_fields else None)
        columns.append(_apply_arrow_type(column, field.type))
    return pa.RecordBatch.from_arrays(columns, schema=schema)


def _apply_arrow_type(column, expected_type):
    import pyarrow as pa

    if column.type == expected_type:
        return column
    # Old Arrow casts reject valid nullability changes, and views inspect unsliced children.
    # After validation, rebuild only container metadata while keeping the payload buffers.
    if pa.types.is_struct(expected_type):
        return pa.StructArray.from_arrays(
            [_apply_arrow_type(column.field(index), field.type)
             for index, field in enumerate(expected_type)],
            fields=list(expected_type), mask=column.is_null() if column.null_count else None)
    if pa.types.is_list(expected_type):
        children = [_apply_arrow_type(column.values, expected_type.value_type)]
    else:
        value_field = (expected_type.item_field if hasattr(expected_type, 'item_field')
                       else pa.field("value", expected_type.item_type))
        children = [pa.StructArray.from_arrays(
            [_apply_arrow_type(column.keys, expected_type.key_type),
             _apply_arrow_type(column.items, expected_type.item_type)],
            fields=[pa.field("key", expected_type.key_type, nullable=False), value_field])]
    return pa.Array.from_buffers(
        expected_type, len(column), column.buffers()[:expected_type.num_buffers],
        null_count=column.null_count, offset=column.offset, children=children)


def _validate_array(column, field, path, parent_validity=None, data_type=None):
    import pyarrow as pa
    import pyarrow.compute as pc
    expected_type = field.type

    def wrong_type():
        raise TypeError(
            f"Arrow result field '{path}' has type {column.type}, expected {expected_type}.")

    if not field.nullable and column.null_count:
        validity = parent_validity() if parent_validity is not None else None
        if validity is None or pc.any(pc.and_(validity, column.is_null())).as_py():
            raise ValueError(f"Arrow result field '{path}' is not nullable.")
    if pa.types.is_struct(expected_type):
        if (not pa.types.is_struct(column.type)
                or column.type.num_fields != expected_type.num_fields):
            wrong_type()
        validity = cache(lambda: _get_validity(column, parent_validity))
        for index, child_field in enumerate(expected_type):
            if column.type[index].name != child_field.name:
                wrong_type()
            _validate_array(column.field(index), child_field,
                            f"{path}.{child_field.name}", validity,
                            data_type.fields[index].data_type if data_type is not None else None)
    elif pa.types.is_list(expected_type):
        if not pa.types.is_list(column.type):
            wrong_type()
        start, end = column.offsets[0].as_py(), column.offsets[-1].as_py()
        _validate_array(column.values.slice(start, end - start),
                        expected_type.value_field, f"{path}[]",
                        cache(lambda: _get_child_validity(column, parent_validity)),
                        data_type.element_type if data_type is not None else None)
    elif pa.types.is_map(expected_type):
        if not pa.types.is_map(column.type):
            wrong_type()
        validity = cache(lambda: _get_child_validity(column, parent_validity))
        offsets = _get_offsets(column)
        start, end = offsets[0].as_py(), offsets[-1].as_py()
        key_field = (expected_type.key_field if data_type is None
                     else pa.field("key", expected_type.key_type, nullable=False))
        value_field = (expected_type.item_field if data_type is None
                       else pa.field("value", expected_type.item_type,
                                     nullable=data_type.value_type._nullable))
        _validate_array(column.keys.slice(start, end - start), key_field,
                        f"{path}.key", validity,
                        data_type.key_type if data_type is not None else None)
        _validate_array(column.items.slice(start, end - start), value_field,
                        f"{path}.value", validity,
                        data_type.value_type if data_type is not None else None)
    elif column.type != expected_type:
        wrong_type()


def _get_validity(column, parent_validity):
    import pyarrow.compute as pc

    # Resolve ancestor visibility only when a NOT NULL descendant contains physical nulls.
    parent_validity = parent_validity() if parent_validity is not None else None
    if column.null_count:
        validity = column.is_valid()
        return validity if parent_validity is None else pc.and_(parent_validity, validity)
    return parent_validity


def _get_child_validity(column, parent_validity):
    import numpy as np
    import pyarrow as pa

    validity = _get_validity(column, parent_validity)
    if validity is None:
        return None
    # Expand visibility directly instead of allocating an integer parent index for every child.
    offsets = _get_offsets(column).to_numpy(zero_copy_only=True)
    visible = validity.to_numpy(zero_copy_only=False)
    return pa.array(np.repeat(visible, np.diff(offsets)), type=pa.bool_())


def _get_offsets(column):
    import pyarrow as pa

    if hasattr(column, 'offsets'):
        return column.offsets
    # PyArrow 5 does not expose MapArray.offsets, but uses the same int32 offsets as lists.
    return pa.Array.from_buffers(pa.int32(), len(column) + 1,
                                 [None, column.buffers()[1]], offset=column.offset)


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
