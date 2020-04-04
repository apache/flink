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

from abc import ABC


import datetime
import decimal
from apache_beam.coders import Coder
from apache_beam.coders.coders import FastCoder

from pyflink.fn_execution import coder_impl
from pyflink.fn_execution import flink_fn_execution_pb2

FLINK_SCHEMA_CODER_URN = "flink:coder:schema:v1"


__all__ = ['RowCoder', 'BigIntCoder', 'TinyIntCoder', 'BooleanCoder',
           'SmallIntCoder', 'IntCoder', 'FloatCoder', 'DoubleCoder',
           'BinaryCoder', 'CharCoder', 'DateCoder', 'TimeCoder',
           'TimestampCoder', 'ArrayCoder', 'MapCoder', 'DecimalCoder']


class RowCoder(FastCoder):
    """
    Coder for Row.
    """

    def __init__(self, field_coders):
        self._field_coders = field_coders

    def _create_impl(self):
        return coder_impl.RowCoderImpl([c.get_impl() for c in self._field_coders])

    def is_deterministic(self):
        return all(c.is_deterministic() for c in self._field_coders)

    def to_type_hint(self):
        from pyflink.table import Row
        return Row

    def __repr__(self):
        return 'RowCoder[%s]' % ', '.join(str(c) for c in self._field_coders)

    def __eq__(self, other):
        return (self.__class__ == other.__class__
                and len(self._field_coders) == len(other._field_coders)
                and [self._field_coders[i] == other._field_coders[i] for i in
                     range(len(self._field_coders))])

    def __ne__(self, other):
        return not self == other

    def __hash__(self):
        return hash(self._field_coders)


class CollectionCoder(FastCoder):
    """
    Base coder for collection.
    """
    def __init__(self, elem_coder):
        self._elem_coder = elem_coder

    def _create_impl(self):
        raise NotImplementedError

    def is_deterministic(self):
        return self._elem_coder.is_deterministic()

    def to_type_hint(self):
        return []

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

    def _create_impl(self):
        return coder_impl.ArrayCoderImpl(self._elem_coder.get_impl())


class MapCoder(FastCoder):
    """
    Coder for Map.
    """

    def __init__(self, key_coder, value_coder):
        self._key_coder = key_coder
        self._value_coder = value_coder

    def _create_impl(self):
        return coder_impl.MapCoderImpl(self._key_coder.get_impl(), self._value_coder.get_impl())

    def is_deterministic(self):
        return self._key_coder.is_deterministic() and self._value_coder.is_deterministic()

    def to_type_hint(self):
        return {}

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


class DeterministicCoder(FastCoder, ABC):
    """
    Base Coder for all deterministic Coders.
    """

    def is_deterministic(self):
        return True


class BigIntCoder(DeterministicCoder):
    """
    Coder for 8 bytes long.
    """

    def _create_impl(self):
        return coder_impl.BigIntCoderImpl()

    def to_type_hint(self):
        return int


class TinyIntCoder(DeterministicCoder):
    """
    Coder for Byte.
    """

    def _create_impl(self):
        return coder_impl.TinyIntCoderImpl()

    def to_type_hint(self):
        return int


class BooleanCoder(DeterministicCoder):
    """
    Coder for Boolean.
    """

    def _create_impl(self):
        return coder_impl.BooleanCoderImpl()

    def to_type_hint(self):
        return bool


class SmallIntCoder(DeterministicCoder):
    """
    Coder for Short.
    """

    def _create_impl(self):
        return coder_impl.SmallIntImpl()

    def to_type_hint(self):
        return int


class IntCoder(DeterministicCoder):
    """
    Coder for 4 bytes int.
    """

    def _create_impl(self):
        return coder_impl.IntCoderImpl()

    def to_type_hint(self):
        return int


class FloatCoder(DeterministicCoder):
    """
    Coder for Float.
    """

    def _create_impl(self):
        return coder_impl.FloatCoderImpl()

    def to_type_hint(self):
        return float


class DoubleCoder(DeterministicCoder):
    """
    Coder for Double.
    """

    def _create_impl(self):
        return coder_impl.DoubleCoderImpl()

    def to_type_hint(self):
        return float


class DecimalCoder(DeterministicCoder):
    """
    Coder for Decimal.
    """

    def __init__(self, precision, scale):
        self.precision = precision
        self.scale = scale

    def _create_impl(self):
        return coder_impl.DecimalCoderImpl(self.precision, self.scale)

    def to_type_hint(self):
        return decimal.Decimal


class BinaryCoder(DeterministicCoder):
    """
    Coder for Byte Array.
    """

    def _create_impl(self):
        return coder_impl.BinaryCoderImpl()

    def to_type_hint(self):
        return bytes


class CharCoder(DeterministicCoder):
    """
    Coder for Character String.
    """
    def _create_impl(self):
        return coder_impl.CharCoderImpl()

    def to_type_hint(self):
        return str


class DateCoder(DeterministicCoder):
    """
    Coder for Date
    """

    def _create_impl(self):
        return coder_impl.DateCoderImpl()

    def to_type_hint(self):
        return datetime.date


class TimeCoder(DeterministicCoder):
    """
    Coder for Time.
    """

    def _create_impl(self):
        return coder_impl.TimeCoderImpl()

    def to_type_hint(self):
        return datetime.time


class TimestampCoder(DeterministicCoder):
    """
    Coder for Timestamp.
    """

    def __init__(self, precision):
        self.precision = precision

    def _create_impl(self):
        return coder_impl.TimestampCoderImpl(self.precision)

    def to_type_hint(self):
        return datetime.datetime


@Coder.register_urn(FLINK_SCHEMA_CODER_URN, flink_fn_execution_pb2.Schema)
def _pickle_from_runner_api_parameter(schema_proto, unused_components, unused_context):
    return RowCoder([from_proto(f.type) for f in schema_proto.fields])


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
    if field_type_name == type_name.DATETIME:
        return TimestampCoder(field_type.date_time_type.precision)
    elif field_type_name == type_name.ARRAY:
        return ArrayCoder(from_proto(field_type.collection_element_type))
    elif field_type_name == type_name.MAP:
        return MapCoder(from_proto(field_type.map_type.key_type),
                        from_proto(field_type.map_type.value_type))
    elif field_type_name == type_name.DECIMAL:
        return DecimalCoder(field_type.decimal_type.precision,
                            field_type.decimal_type.scale)
    else:
        raise ValueError("field_type %s is not supported." % field_type)
