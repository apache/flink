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

import os
from abc import ABC

import pytz

from pyflink.fn_execution import flink_fn_execution_pb2

try:
    from pyflink.fn_execution import coder_impl_fast as coder_impl
except:
    from pyflink.fn_execution.beam import beam_coder_impl_slow as coder_impl

__all__ = ['RowCoder', 'BigIntCoder', 'TinyIntCoder', 'BooleanCoder',
           'SmallIntCoder', 'IntCoder', 'FloatCoder', 'DoubleCoder',
           'BinaryCoder', 'CharCoder', 'DateCoder', 'TimeCoder',
           'TimestampCoder', 'ArrayCoder', 'MapCoder', 'DecimalCoder']

FLINK_SCALAR_FUNCTION_SCHEMA_CODER_URN = "flink:coder:schema:scalar_function:v1"
FLINK_TABLE_FUNCTION_SCHEMA_CODER_URN = "flink:coder:schema:table_function:v1"
FLINK_SCALAR_FUNCTION_SCHEMA_ARROW_CODER_URN = "flink:coder:schema:scalar_function:arrow:v1"


class BaseCoder(ABC):
    def get_impl(self):
        pass

    @staticmethod
    def from_schema_proto(schema_proto):
        pass


class TableFunctionRowCoder(BaseCoder):
    """
    Coder for Table Function Row.
    """

    def __init__(self, flatten_row_coder):
        self._flatten_row_coder = flatten_row_coder

    def get_impl(self):
        return coder_impl.TableFunctionRowCoderImpl(self._flatten_row_coder.get_impl())

    @staticmethod
    def from_schema_proto(schema_proto):
        return TableFunctionRowCoder(FlattenRowCoder.from_schema_proto(schema_proto))

    def __repr__(self):
        return 'TableFunctionRowCoder[%s]' % repr(self._flatten_row_coder)

    def __eq__(self, other):
        return (self.__class__ == other.__class__
                and self._flatten_row_coder == other._flatten_row_coder)

    def __ne__(self, other):
        return not self == other

    def __hash__(self):
        return hash(self._flatten_row_coder)


class FlattenRowCoder(BaseCoder):
    """
    Coder for Row. The decoded result will be flattened as a list of column values of a row instead
    of a row object.
    """

    def __init__(self, field_coders):
        self._field_coders = field_coders

    def get_impl(self):
        return coder_impl.FlattenRowCoderImpl([c.get_impl() for c in self._field_coders])

    @staticmethod
    def from_schema_proto(schema_proto):
        return FlattenRowCoder([from_proto(f.type) for f in schema_proto.fields])

    def __repr__(self):
        return 'FlattenRowCoder[%s]' % ', '.join(str(c) for c in self._field_coders)

    def __eq__(self, other):
        return (self.__class__ == other.__class__
                and len(self._field_coders) == len(other._field_coders)
                and [self._field_coders[i] == other._field_coders[i] for i in
                     range(len(self._field_coders))])

    def __ne__(self, other):
        return not self == other

    def __hash__(self):
        return hash(self._field_coders)


class FieldCoder(ABC):
    def get_impl(self):
        pass


class RowCoder(FieldCoder):
    """
    Coder for Row.
    """

    def __init__(self, field_coders):
        self._field_coders = field_coders

    def get_impl(self):
        return coder_impl.RowCoderImpl([c.get_impl() for c in self._field_coders])

    def __repr__(self):
        return 'RowCoder[%s]' % ', '.join(str(c) for c in self._field_coders)


class CollectionCoder(FieldCoder):
    """
    Base coder for collection.
    """

    def __init__(self, elem_coder):
        self._elem_coder = elem_coder

    def is_deterministic(self):
        return self._elem_coder.is_deterministic()

    def __eq__(self, other):
        return (self.__class__ == other.__class__
                and self._elem_coder == other._elem_coder)

    def __repr__(self):
        return '%s[%s]' % (self.__class__.__name__, repr(self._elem_coder))

    def __ne__(self, other):
        return not self == other

    def __hash__(self):
        return hash(self._elem_coder)


class ArrayCoder(CollectionCoder):
    """
    Coder for Array.
    """

    def __init__(self, elem_coder):
        self._elem_coder = elem_coder
        super(ArrayCoder, self).__init__(elem_coder)

    def get_impl(self):
        return coder_impl.ArrayCoderImpl(self._elem_coder.get_impl())


class MapCoder(FieldCoder):
    """
    Coder for Map.
    """

    def __init__(self, key_coder, value_coder):
        self._key_coder = key_coder
        self._value_coder = value_coder

    def get_impl(self):
        return coder_impl.MapCoderImpl(self._key_coder.get_impl(), self._value_coder.get_impl())

    def is_deterministic(self):
        return self._key_coder.is_deterministic() and self._value_coder.is_deterministic()

    def __repr__(self):
        return 'MapCoder[%s]' % ','.join([repr(self._key_coder), repr(self._value_coder)])

    def __eq__(self, other):
        return (self.__class__ == other.__class__
                and self._key_coder == other._key_coder
                and self._value_coder == other._value_coder)

    def __ne__(self, other):
        return not self == other

    def __hash__(self):
        return hash([self._key_coder, self._value_coder])


class BigIntCoder(FieldCoder):
    """
    Coder for 8 bytes long.
    """

    def get_impl(self):
        return coder_impl.BigIntCoderImpl()


class TinyIntCoder(FieldCoder):
    """
    Coder for Byte.
    """

    def get_impl(self):
        return coder_impl.TinyIntCoderImpl()


class BooleanCoder(FieldCoder):
    """
    Coder for Boolean.
    """

    def get_impl(self):
        return coder_impl.BooleanCoderImpl()


class SmallIntCoder(FieldCoder):
    """
    Coder for Short.
    """

    def get_impl(self):
        return coder_impl.SmallIntCoderImpl()


class IntCoder(FieldCoder):
    """
    Coder for 4 bytes int.
    """

    def get_impl(self):
        return coder_impl.IntCoderImpl()


class FloatCoder(FieldCoder):
    """
    Coder for Float.
    """

    def get_impl(self):
        return coder_impl.FloatCoderImpl()


class DoubleCoder(FieldCoder):
    """
    Coder for Double.
    """

    def get_impl(self):
        return coder_impl.DoubleCoderImpl()


class DecimalCoder(FieldCoder):
    """
    Coder for Decimal.
    """

    def __init__(self, precision, scale):
        self.precision = precision
        self.scale = scale

    def get_impl(self):
        return coder_impl.DecimalCoderImpl(self.precision, self.scale)


class BinaryCoder(FieldCoder):
    """
    Coder for Byte Array.
    """

    def get_impl(self):
        return coder_impl.BinaryCoderImpl()


class CharCoder(FieldCoder):
    """
    Coder for Character String.
    """

    def get_impl(self):
        return coder_impl.CharCoderImpl()


class DateCoder(FieldCoder):
    """
    Coder for Date
    """

    def get_impl(self):
        return coder_impl.DateCoderImpl()


class TimeCoder(FieldCoder):
    """
    Coder for Time.
    """

    def get_impl(self):
        return coder_impl.TimeCoderImpl()


class TimestampCoder(FieldCoder):
    """
    Coder for Timestamp.
    """

    def __init__(self, precision):
        self.precision = precision

    def get_impl(self):
        return coder_impl.TimestampCoderImpl(self.precision)


class LocalZonedTimestampCoder(FieldCoder):
    """
    Coder for LocalZonedTimestamp.
    """

    def __init__(self, precision, timezone):
        self.precision = precision
        self.timezone = timezone

    def get_impl(self):
        return coder_impl.LocalZonedTimestampCoderImpl(self.precision, self.timezone)


type_name = flink_fn_execution_pb2.Schema
_type_name_mappings = {
    type_name.TINYINT: TinyIntCoder(),
    type_name.SMALLINT: SmallIntCoder(),
    type_name.INT: IntCoder(),
    type_name.BIGINT: BigIntCoder(),
    type_name.BOOLEAN: BooleanCoder(),
    type_name.FLOAT: FloatCoder(),
    type_name.DOUBLE: DoubleCoder(),
    type_name.BINARY: BinaryCoder(),
    type_name.VARBINARY: BinaryCoder(),
    type_name.CHAR: CharCoder(),
    type_name.VARCHAR: CharCoder(),
    type_name.DATE: DateCoder(),
    type_name.TIME: TimeCoder(),
}


def from_proto(field_type):
    """
    Creates the corresponding :class:`Coder` given the protocol representation of the field type.

    :param field_type: the protocol representation of the field type
    :return: :class:`Coder`
    """
    field_type_name = field_type.type_name
    coder = _type_name_mappings.get(field_type_name)
    if coder is not None:
        return coder
    if field_type_name == type_name.ROW:
        return RowCoder([from_proto(f.type) for f in field_type.row_schema.fields])
    if field_type_name == type_name.TIMESTAMP:
        return TimestampCoder(field_type.timestamp_info.precision)
    if field_type_name == type_name.LOCAL_ZONED_TIMESTAMP:
        timezone = pytz.timezone(os.environ['table.exec.timezone'])
        return LocalZonedTimestampCoder(field_type.local_zoned_timestamp_info.precision, timezone)
    elif field_type_name == type_name.ARRAY:
        return ArrayCoder(from_proto(field_type.collection_element_type))
    elif field_type_name == type_name.MAP:
        return MapCoder(from_proto(field_type.map_info.key_type),
                        from_proto(field_type.map_info.value_type))
    elif field_type_name == type_name.DECIMAL:
        return DecimalCoder(field_type.decimal_info.precision,
                            field_type.decimal_info.scale)
    else:
        raise ValueError("field_type %s is not supported." % field_type)
