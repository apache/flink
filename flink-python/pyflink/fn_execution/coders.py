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

import datetime
from decimal import Decimal
from apache_beam.coders import Coder, VarIntCoder
from apache_beam.coders.coders import FastCoder

from pyflink.fn_execution import coder_impl
from pyflink.fn_execution import flink_fn_execution_pb2

FLINK_SCHEMA_CODER_URN = "flink:coder:schema:v1"


__all__ = ['RowCoder']


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


class ArrayCoder(FastCoder):
    """
    Coder for Array.
    """

    def __init__(self, elem_coder):
        self._elem_coder = elem_coder

    def _create_impl(self):
        return coder_impl.ArrayCoderImpl(self._elem_coder.get_impl())

    def is_deterministic(self):
        return self._elem_coder.is_deterministic()

    def to_type_hint(self):
        return []

    def __repr__(self):
        return 'ArrayCoder[%s]' % str(self._elem_coder)


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
        return 'MapCoder[%s]' % ','.join([str(self._key_coder), str(self._value_coder)])


class MultiSetCoder(FastCoder):
    """
    Coder for Multiset
    """

    def __init__(self, element_coder):
        self._element_coder = element_coder

    def _create_impl(self):
        return coder_impl.MultiSetCoderImpl(self._element_coder.get_impl())

    def is_deterministic(self):
        return self._element_coder.is_deterministic()

    def to_type_hint(self):
        return []

    def __repr__(self):
        return 'MultiSetCoder[%s]' % str(self._element_coder)


class BooleanCoder(FastCoder):
    """
    Coder for Boolean
    """

    def _create_impl(self):
        return coder_impl.BooleanCoderImpl()

    def is_deterministic(self):
        return True

    def to_type_hint(self):
        return bool

    def __repr__(self):
        return 'BooleanCoder'


class ByteCoder(FastCoder):
    """
    Coder for Byte.
    """

    def _create_impl(self):
        return coder_impl.ByteCoderImpl()

    def is_deterministic(self):
        return True

    def to_type_hint(self):
        return int

    def __repr__(self):
        return 'ByteCoder'


class ShortCoder(FastCoder):
    """
    Coder for Short.
    """

    def _create_impl(self):
        return coder_impl.ShortCoderImpl()

    def is_deterministic(self):
        return True

    def to_type_hint(self):
        return int

    def __repr__(self):
        return 'ShortCoder'


class FloatCoder(FastCoder):
    """
    Coder for Float.
    """

    def _create_impl(self):
        return coder_impl.FloatCoderImpl()

    def is_deterministic(self):
        return True

    def to_type_hint(self):
        return float

    def __repr__(self):
        return 'FloatCoder'


class DoubleCoder(FastCoder):
    """
    Coder for Double.
    """

    def _create_impl(self):
        return coder_impl.DoubleCoderImpl()

    def is_deterministic(self):
        return True

    def to_type_hint(self):
        return float

    def __repr__(self):
        return 'DoubleCoder'


class DecimalCoder(FastCoder):
    """
    Coder for Decimal.
    """

    def __init__(self, str_coder):
        self._str_coder = str_coder

    def _create_impl(self):
        return coder_impl.DecimalCoderImpl(self._str_coder.get_impl())

    def is_deterministic(self):
        return True

    def to_type_hint(self):
        return Decimal

    def __repr__(self):
        return "DecimalCoder"


class DateCoder(FastCoder):
    """
    Coder for Date
    """

    def _create_impl(self):
        return coder_impl.DateCoderImpl()

    def is_deterministic(self):
        return True

    def to_type_hint(self):
        return datetime.date

    def __repr__(self):
        return "DateCoder"


class TimeCoder(FastCoder):
    """
    Coder for Time
    """

    def _create_impl(self):
        return coder_impl.TimeCoderImpl()

    def is_deterministic(self):
        return True

    def to_type_hint(self):
        return datetime.time

    def __repr__(self):
        return "TimeCoder"


class DateTimeCoder(FastCoder):
    """
    Coder for DateTime
    """

    def _create_impl(self):
        return coder_impl.DateTimeCoderImpl()

    def is_deterministic(self):
        return True

    def to_type_hint(self):
        return datetime.datetime

    def __repr__(self):
        return "DateTimeCoder"


class BinaryCoder(FastCoder):
    """
    Coder for Binary String
    """

    def _create_impl(self):
        return coder_impl.BinaryCoderImpl()

    def is_deterministic(self):
        return True

    def to_type_hint(self):
        return bytearray

    def __repr__(self):
        return "BinaryCoder"


class StringCoder(FastCoder):
    """
    Coder for Character String
    """
    def _create_impl(self):
        return coder_impl.StringCoderImpl()

    def is_deterministic(self):
        return True

    def to_type_hint(self):
        return str

    def __repr__(self):
        return "StringCoder"


@Coder.register_urn(FLINK_SCHEMA_CODER_URN, flink_fn_execution_pb2.Schema)
def _pickle_from_runner_api_parameter(schema_proto, unused_components, unused_context):
    return RowCoder([from_proto(f.type) for f in schema_proto.fields])


type_name = flink_fn_execution_pb2.Schema.TypeName
BYTE_CODER = ByteCoder()
SHORT_CODER = ShortCoder()
BIGINT_CODER = VarIntCoder()
BOOLEAN_CODER = BooleanCoder()
BINARY_CODER = BinaryCoder()
CHARACTER_CODER = StringCoder()
FLOAT_CODER = FloatCoder()
DOUBLE_CODER = DoubleCoder()
DECIMAL_CODER = DecimalCoder(CHARACTER_CODER)
DATE_CODER = DateCoder()
TIME_CODER = TimeCoder()
DATETIME_CODER = DateTimeCoder()
_type_name_mappings = {
    type_name.BOOLEAN: BOOLEAN_CODER,
    type_name.TINYINT: BYTE_CODER,
    type_name.SMALLINT: SHORT_CODER,
    type_name.INT: BIGINT_CODER,
    type_name.BIGINT: BIGINT_CODER,
    type_name.BINARY: BINARY_CODER,
    type_name.VARBINARY: BINARY_CODER,
    type_name.CHAR: CHARACTER_CODER,
    type_name.VARCHAR: CHARACTER_CODER,
    type_name.FLOAT: FLOAT_CODER,
    type_name.DOUBLE: DOUBLE_CODER,
    type_name.DECIMAL: DECIMAL_CODER,
    type_name.DATE: DATE_CODER,
    type_name.TIME: TIME_CODER,
    type_name.DATETIME: DATETIME_CODER
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
    elif field_type_name == type_name.ARRAY:
        return ArrayCoder(from_proto(field_type.collection_element_type))
    elif field_type_name == type_name.MAP:
        return MapCoder(from_proto(field_type.map_type.key_type),
                        from_proto(field_type.map_type.value_type))
    elif field_type_name == type_name.MULTISET:
        return MultiSetCoder(from_proto(field_type.collection_element_type))
    else:
        raise ValueError("field_type %s is not supported." % field_type)
