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
from abc import ABC, abstractmethod
from typing import Union

import pytz

from pyflink import fn_execution

if fn_execution.PYFLINK_CYTHON_ENABLED:
    from pyflink.fn_execution import coder_impl_fast as coder_impl
else:
    from pyflink.fn_execution import coder_impl_slow as coder_impl

from pyflink.datastream.formats.avro import GenericRecordAvroTypeInfo, AvroSchema
from pyflink.common.typeinfo import TypeInformation, BasicTypeInfo, BasicType, DateTypeInfo, \
    TimeTypeInfo, TimestampTypeInfo, PrimitiveArrayTypeInfo, BasicArrayTypeInfo, TupleTypeInfo, \
    MapTypeInfo, ListTypeInfo, RowTypeInfo, PickledBytesTypeInfo, ObjectArrayTypeInfo, \
    ExternalTypeInfo
from pyflink.table.types import TinyIntType, SmallIntType, IntType, BigIntType, BooleanType, \
    FloatType, DoubleType, VarCharType, VarBinaryType, DecimalType, DateType, TimeType, \
    LocalZonedTimestampType, RowType, RowField, to_arrow_type, TimestampType, ArrayType, MapType, \
    BinaryType, NullType, CharType

__all__ = ['FlattenRowCoder', 'RowCoder', 'BigIntCoder', 'TinyIntCoder', 'BooleanCoder',
           'SmallIntCoder', 'IntCoder', 'FloatCoder', 'DoubleCoder', 'BinaryCoder', 'CharCoder',
           'DateCoder', 'TimeCoder', 'TimestampCoder', 'LocalZonedTimestampCoder', 'InstantCoder',
           'GenericArrayCoder', 'PrimitiveArrayCoder', 'MapCoder', 'DecimalCoder',
           'BigDecimalCoder', 'TupleCoder', 'TimeWindowCoder', 'CountWindowCoder',
           'PickleCoder', 'CloudPickleCoder', 'DataViewFilterCoder']


#########################################################################
#             Top-level coder: ValueCoder & IterableCoder
#########################################################################

# LengthPrefixBaseCoder is the top level coder and the other coders will be used as the field coder
class LengthPrefixBaseCoder(ABC):
    def __init__(self, field_coder: 'FieldCoder'):
        self._field_coder = field_coder

    @abstractmethod
    def get_impl(self):
        pass

    @classmethod
    def from_coder_info_descriptor_proto(cls, coder_info_descriptor_proto):
        from pyflink.fn_execution import flink_fn_execution_pb2

        field_coder = cls._to_field_coder(coder_info_descriptor_proto)
        mode = coder_info_descriptor_proto.mode
        separated_with_end_message = coder_info_descriptor_proto.separated_with_end_message
        if mode == flink_fn_execution_pb2.CoderInfoDescriptor.SINGLE:
            return ValueCoder(field_coder)
        else:
            return IterableCoder(field_coder, separated_with_end_message)

    @classmethod
    def _to_field_coder(cls, coder_info_descriptor_proto):
        if coder_info_descriptor_proto.HasField('flatten_row_type'):
            schema_proto = coder_info_descriptor_proto.flatten_row_type.schema
            field_coders = [from_proto(f.type) for f in schema_proto.fields]
            return FlattenRowCoder(field_coders)
        elif coder_info_descriptor_proto.HasField('row_type'):
            schema_proto = coder_info_descriptor_proto.row_type.schema
            field_coders = [from_proto(f.type) for f in schema_proto.fields]
            field_names = [f.name for f in schema_proto.fields]
            return RowCoder(field_coders, field_names)
        elif coder_info_descriptor_proto.HasField('arrow_type'):
            timezone = pytz.timezone(os.environ['TABLE_LOCAL_TIME_ZONE'])
            schema_proto = coder_info_descriptor_proto.arrow_type.schema
            row_type = cls._to_row_type(schema_proto)
            return ArrowCoder(cls._to_arrow_schema(row_type), row_type, timezone)
        elif coder_info_descriptor_proto.HasField('over_window_arrow_type'):
            timezone = pytz.timezone(os.environ['TABLE_LOCAL_TIME_ZONE'])
            schema_proto = coder_info_descriptor_proto.over_window_arrow_type.schema
            row_type = cls._to_row_type(schema_proto)
            return OverWindowArrowCoder(
                cls._to_arrow_schema(row_type), row_type, timezone)
        elif coder_info_descriptor_proto.HasField('raw_type'):
            type_info_proto = coder_info_descriptor_proto.raw_type.type_info
            field_coder = from_type_info_proto(type_info_proto)
            return field_coder
        else:
            raise ValueError("Unexpected coder type %s" % coder_info_descriptor_proto)

    @classmethod
    def _to_arrow_schema(cls, row_type):
        import pyarrow as pa

        return pa.schema([pa.field(n, to_arrow_type(t), t._nullable)
                          for n, t in zip(row_type.field_names(), row_type.field_types())])

    @classmethod
    def _to_data_type(cls, field_type):
        from pyflink.fn_execution import flink_fn_execution_pb2

        if field_type.type_name == flink_fn_execution_pb2.Schema.TINYINT:
            return TinyIntType(field_type.nullable)
        elif field_type.type_name == flink_fn_execution_pb2.Schema.SMALLINT:
            return SmallIntType(field_type.nullable)
        elif field_type.type_name == flink_fn_execution_pb2.Schema.INT:
            return IntType(field_type.nullable)
        elif field_type.type_name == flink_fn_execution_pb2.Schema.BIGINT:
            return BigIntType(field_type.nullable)
        elif field_type.type_name == flink_fn_execution_pb2.Schema.BOOLEAN:
            return BooleanType(field_type.nullable)
        elif field_type.type_name == flink_fn_execution_pb2.Schema.FLOAT:
            return FloatType(field_type.nullable)
        elif field_type.type_name == flink_fn_execution_pb2.Schema.DOUBLE:
            return DoubleType(field_type.nullable)
        elif field_type.type_name == flink_fn_execution_pb2.Schema.CHAR:
            return CharType(field_type.char_info.length, field_type.nullable)
        elif field_type.type_name == flink_fn_execution_pb2.Schema.VARCHAR:
            return VarCharType(field_type.var_char_info.length, field_type.nullable)
        elif field_type.type_name == flink_fn_execution_pb2.Schema.BINARY:
            return BinaryType(field_type.binary_info.length, field_type.nullable)
        elif field_type.type_name == flink_fn_execution_pb2.Schema.VARBINARY:
            return VarBinaryType(field_type.var_binary_info.length, field_type.nullable)
        elif field_type.type_name == flink_fn_execution_pb2.Schema.DECIMAL:
            return DecimalType(field_type.decimal_info.precision,
                               field_type.decimal_info.scale,
                               field_type.nullable)
        elif field_type.type_name == flink_fn_execution_pb2.Schema.DATE:
            return DateType(field_type.nullable)
        elif field_type.type_name == flink_fn_execution_pb2.Schema.TIME:
            return TimeType(field_type.time_info.precision, field_type.nullable)
        elif field_type.type_name == \
                flink_fn_execution_pb2.Schema.LOCAL_ZONED_TIMESTAMP:
            return LocalZonedTimestampType(field_type.local_zoned_timestamp_info.precision,
                                           field_type.nullable)
        elif field_type.type_name == flink_fn_execution_pb2.Schema.TIMESTAMP:
            return TimestampType(field_type.timestamp_info.precision, field_type.nullable)
        elif field_type.type_name == flink_fn_execution_pb2.Schema.BASIC_ARRAY:
            return ArrayType(cls._to_data_type(field_type.collection_element_type),
                             field_type.nullable)
        elif field_type.type_name == flink_fn_execution_pb2.Schema.TypeName.ROW:
            return RowType(
                [RowField(f.name, cls._to_data_type(f.type), f.description)
                 for f in field_type.row_schema.fields], field_type.nullable)
        elif field_type.type_name == flink_fn_execution_pb2.Schema.TypeName.MAP:
            return MapType(cls._to_data_type(field_type.map_info.key_type),
                           cls._to_data_type(field_type.map_info.value_type),
                           field_type.nullable)
        elif field_type.type_name == flink_fn_execution_pb2.Schema.TypeName.NULL:
            return NullType()
        else:
            raise ValueError("field_type %s is not supported." % field_type)

    @classmethod
    def _to_row_type(cls, row_schema):
        return RowType([RowField(f.name, cls._to_data_type(f.type)) for f in row_schema.fields])


class IterableCoder(LengthPrefixBaseCoder):
    """
    Coder for iterable data.
    """

    def __init__(self, field_coder: 'FieldCoder', separated_with_end_message):
        super(IterableCoder, self).__init__(field_coder)
        self._separated_with_end_message = separated_with_end_message

    def get_impl(self):
        return coder_impl.IterableCoderImpl(self._field_coder.get_impl(),
                                            self._separated_with_end_message)


class ValueCoder(LengthPrefixBaseCoder):
    """
    Coder for single data.
    """

    def __init__(self, field_coder: 'FieldCoder'):
        super(ValueCoder, self).__init__(field_coder)

    def get_impl(self):
        return coder_impl.ValueCoderImpl(self._field_coder.get_impl())


#########################################################################
#                         Low-level coder: FieldCoder
#########################################################################


class FieldCoder(ABC):

    def get_impl(self) -> coder_impl.FieldCoderImpl:
        pass

    def __eq__(self, other):
        return type(self) == type(other)


class FlattenRowCoder(FieldCoder):
    """
    Coder for Row. The decoded result will be flattened as a list of column values of a row instead
    of a row object.
    """

    def __init__(self, field_coders):
        self._field_coders = field_coders

    def get_impl(self):
        return coder_impl.FlattenRowCoderImpl([c.get_impl() for c in self._field_coders])

    def __repr__(self):
        return 'FlattenRowCoder[%s]' % ', '.join(str(c) for c in self._field_coders)

    def __eq__(self, other: 'FlattenRowCoder'):
        return (self.__class__ == other.__class__
                and len(self._field_coders) == len(other._field_coders)
                and [self._field_coders[i] == other._field_coders[i] for i in
                     range(len(self._field_coders))])

    def __ne__(self, other):
        return not self == other

    def __hash__(self):
        return hash(self._field_coders)


class ArrowCoder(FieldCoder):
    """
    Coder for Arrow.
    """

    def __init__(self, schema, row_type, timezone):
        self._schema = schema
        self._row_type = row_type
        self._timezone = timezone

    def get_impl(self):
        return coder_impl.ArrowCoderImpl(self._schema, self._row_type, self._timezone)

    def __repr__(self):
        return 'ArrowCoder[%s]' % self._schema


class OverWindowArrowCoder(FieldCoder):
    """
    Coder for batch pandas over window aggregation.
    """

    def __init__(self, schema, row_type, timezone):
        self._arrow_coder = ArrowCoder(schema, row_type, timezone)

    def get_impl(self):
        return coder_impl.OverWindowArrowCoderImpl(self._arrow_coder.get_impl())

    def __repr__(self):
        return 'OverWindowArrowCoder[%s]' % self._arrow_coder


class RowCoder(FieldCoder):
    """
    Coder for Row.
    """

    def __init__(self, field_coders, field_names):
        self._field_coders = field_coders
        self._field_names = field_names

    def get_impl(self):
        return coder_impl.RowCoderImpl([c.get_impl() for c in self._field_coders],
                                       self._field_names)

    def __repr__(self):
        return 'RowCoder[%s]' % ', '.join(str(c) for c in self._field_coders)

    def __eq__(self, other: 'RowCoder'):
        return (self.__class__ == other.__class__
                and self._field_names == other._field_names
                and [self._field_coders[i] == other._field_coders[i] for i in
                     range(len(self._field_coders))])

    def __ne__(self, other):
        return not self == other

    def __hash__(self):
        return hash(self._field_coders)


class CollectionCoder(FieldCoder):
    """
    Base coder for collection.
    """

    def __init__(self, elem_coder):
        self._elem_coder = elem_coder

    def is_deterministic(self):
        return self._elem_coder.is_deterministic()

    def __eq__(self, other: 'CollectionCoder'):
        return (self.__class__ == other.__class__
                and self._elem_coder == other._elem_coder)

    def __repr__(self):
        return '%s[%s]' % (self.__class__.__name__, repr(self._elem_coder))

    def __ne__(self, other):
        return not self == other

    def __hash__(self):
        return hash(self._elem_coder)


class GenericArrayCoder(CollectionCoder):
    """
    Coder for generic array such as basic array or object array.
    """

    def __init__(self, elem_coder):
        super(GenericArrayCoder, self).__init__(elem_coder)

    def get_impl(self):
        return coder_impl.GenericArrayCoderImpl(self._elem_coder.get_impl())


class PrimitiveArrayCoder(CollectionCoder):
    """
    Coder for Primitive Array.
    """

    def __init__(self, elem_coder):
        super(PrimitiveArrayCoder, self).__init__(elem_coder)

    def get_impl(self):
        return coder_impl.PrimitiveArrayCoderImpl(self._elem_coder.get_impl())


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

    def __eq__(self, other: 'MapCoder'):
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

    def __eq__(self, other: 'DecimalCoder'):
        return (self.__class__ == other.__class__ and
                self.precision == other.precision and
                self.scale == other.scale)


class BigDecimalCoder(FieldCoder):
    """
    Coder for Basic Decimal that no need to have precision and scale specified.
    """

    def get_impl(self):
        return coder_impl.BigDecimalCoderImpl()


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

    def __eq__(self, other: 'TimestampCoder'):
        return self.__class__ == other.__class__ and self.precision == other.precision


class LocalZonedTimestampCoder(FieldCoder):
    """
    Coder for LocalZonedTimestamp.
    """

    def __init__(self, precision, timezone):
        self.precision = precision
        self.timezone = timezone

    def get_impl(self):
        return coder_impl.LocalZonedTimestampCoderImpl(self.precision, self.timezone)

    def __eq__(self, other: 'LocalZonedTimestampCoder'):
        return (self.__class__ == other.__class__ and
                self.precision == other.precision and
                self.timezone == other.timezone)


class InstantCoder(FieldCoder):
    """
    Coder for Instant.
    """
    def get_impl(self) -> coder_impl.FieldCoderImpl:
        return coder_impl.InstantCoderImpl()


class CloudPickleCoder(FieldCoder):
    """
    Coder used with cloudpickle to encode python object.
    """

    def get_impl(self):
        return coder_impl.CloudPickleCoderImpl()


class PickleCoder(FieldCoder):
    """
    Coder used with pickle to encode python object.
    """

    def get_impl(self):
        return coder_impl.PickleCoderImpl()


class TupleCoder(FieldCoder):
    """
    Coder for Tuple.
    """

    def __init__(self, field_coders):
        self._field_coders = field_coders

    def get_impl(self):
        return coder_impl.TupleCoderImpl([c.get_impl() for c in self._field_coders])

    def __repr__(self):
        return 'TupleCoder[%s]' % ', '.join(str(c) for c in self._field_coders)

    def __eq__(self, other: 'TupleCoder'):
        return (self.__class__ == other.__class__ and
                [self._field_coders[i] == other._field_coders[i]
                 for i in range(len(self._field_coders))])


class TimeWindowCoder(FieldCoder):
    """
    Coder for TimeWindow.
    """

    def get_impl(self):
        return coder_impl.TimeWindowCoderImpl()


class CountWindowCoder(FieldCoder):
    """
    Coder for CountWindow.
    """

    def get_impl(self):
        return coder_impl.CountWindowCoderImpl()


class GlobalWindowCoder(FieldCoder):
    """
    Coder for GlobalWindow.
    """

    def get_impl(self):
        return coder_impl.GlobalWindowCoderImpl()


class DataViewFilterCoder(FieldCoder):
    """
    Coder for data view filter.
    """

    def __init__(self, udf_data_view_specs):
        self._udf_data_view_specs = udf_data_view_specs

    def get_impl(self):
        return coder_impl.DataViewFilterCoderImpl(self._udf_data_view_specs)


class AvroCoder(FieldCoder):

    def __init__(self, schema: Union[str, AvroSchema]):
        if isinstance(schema, str):
            self._schema_string = schema
        elif isinstance(schema, AvroSchema):
            self._schema_string = str(schema)
        else:
            raise ValueError('schema for AvroCoder must be string or AvroSchema')

    def get_impl(self):
        return coder_impl.AvroCoderImpl(self._schema_string)


class LocalDateCoder(FieldCoder):

    def get_impl(self):
        return coder_impl.LocalDateCoderImpl()


class LocalTimeCoder(FieldCoder):

    def get_impl(self):
        return coder_impl.LocalTimeCoderImpl()


class LocalDateTimeCoder(FieldCoder):

    def get_impl(self):
        return coder_impl.LocalDateTimeCoderImpl()


def from_proto(field_type):
    """
    Creates the corresponding :class:`Coder` given the protocol representation of the field type.

    :param field_type: the protocol representation of the field type
    :return: :class:`Coder`
    """
    from pyflink.fn_execution import flink_fn_execution_pb2

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

    field_type_name = field_type.type_name
    coder = _type_name_mappings.get(field_type_name)
    if coder is not None:
        return coder
    if field_type_name == type_name.ROW:
        return RowCoder([from_proto(f.type) for f in field_type.row_schema.fields],
                        [f.name for f in field_type.row_schema.fields])
    if field_type_name == type_name.TIMESTAMP:
        return TimestampCoder(field_type.timestamp_info.precision)
    if field_type_name == type_name.LOCAL_ZONED_TIMESTAMP:
        timezone = pytz.timezone(os.environ['TABLE_LOCAL_TIME_ZONE'])
        return LocalZonedTimestampCoder(field_type.local_zoned_timestamp_info.precision, timezone)
    elif field_type_name == type_name.BASIC_ARRAY:
        return GenericArrayCoder(from_proto(field_type.collection_element_type))
    elif field_type_name == type_name.MAP:
        return MapCoder(from_proto(field_type.map_info.key_type),
                        from_proto(field_type.map_info.value_type))
    elif field_type_name == type_name.DECIMAL:
        return DecimalCoder(field_type.decimal_info.precision,
                            field_type.decimal_info.scale)
    else:
        raise ValueError("field_type %s is not supported." % field_type)


def from_type_info_proto(type_info):
    # for data stream type information.
    from pyflink.fn_execution import flink_fn_execution_pb2

    type_info_name = flink_fn_execution_pb2.TypeInfo
    _type_info_name_mappings = {
        type_info_name.STRING: CharCoder(),
        type_info_name.BYTE: TinyIntCoder(),
        type_info_name.BOOLEAN: BooleanCoder(),
        type_info_name.SHORT: SmallIntCoder(),
        type_info_name.INT: IntCoder(),
        type_info_name.LONG: BigIntCoder(),
        type_info_name.FLOAT: FloatCoder(),
        type_info_name.DOUBLE: DoubleCoder(),
        type_info_name.CHAR: CharCoder(),
        type_info_name.BIG_INT: BigIntCoder(),
        type_info_name.BIG_DEC: BigDecimalCoder(),
        type_info_name.SQL_DATE: DateCoder(),
        type_info_name.SQL_TIME: TimeCoder(),
        type_info_name.SQL_TIMESTAMP: TimestampCoder(3),
        type_info_name.PICKLED_BYTES: CloudPickleCoder(),
        type_info_name.INSTANT: InstantCoder(),
        type_info_name.LOCAL_DATE: LocalDateCoder(),
        type_info_name.LOCAL_TIME: LocalTimeCoder(),
        type_info_name.LOCAL_DATETIME: LocalDateTimeCoder(),
    }

    field_type_name = type_info.type_name
    try:
        return _type_info_name_mappings[field_type_name]
    except KeyError:
        if field_type_name == type_info_name.ROW:
            return RowCoder(
                [from_type_info_proto(f.field_type) for f in type_info.row_type_info.fields],
                [f.field_name for f in type_info.row_type_info.fields])
        elif field_type_name in (
            type_info_name.PRIMITIVE_ARRAY,
            type_info_name.LIST,
        ):
            if type_info.collection_element_type.type_name == type_info_name.BYTE:
                return BinaryCoder()
            return PrimitiveArrayCoder(from_type_info_proto(type_info.collection_element_type))
        elif field_type_name in (
            type_info_name.BASIC_ARRAY,
            type_info_name.OBJECT_ARRAY,
        ):
            return GenericArrayCoder(from_type_info_proto(type_info.collection_element_type))
        elif field_type_name == type_info_name.TUPLE:
            return TupleCoder([from_type_info_proto(field_type)
                               for field_type in type_info.tuple_type_info.field_types])
        elif field_type_name == type_info_name.MAP:
            return MapCoder(from_type_info_proto(type_info.map_type_info.key_type),
                            from_type_info_proto(type_info.map_type_info.value_type))
        elif field_type_name == type_info_name.AVRO:
            return AvroCoder(type_info.avro_type_info.schema)
        elif field_type_name == type_info_name.LOCAL_ZONED_TIMESTAMP:
            return LocalZonedTimestampCoder(
                3, timezone=pytz.timezone(os.environ['TABLE_LOCAL_TIME_ZONE'])
            )
        else:
            raise ValueError("Unsupported type_info %s." % type_info)


_basic_type_info_mappings = {
    BasicType.BYTE: TinyIntCoder(),
    BasicType.BOOLEAN: BooleanCoder(),
    BasicType.SHORT: SmallIntCoder(),
    BasicType.INT: IntCoder(),
    BasicType.LONG: BigIntCoder(),
    BasicType.BIG_INT: BigIntCoder(),
    BasicType.FLOAT: FloatCoder(),
    BasicType.DOUBLE: DoubleCoder(),
    BasicType.STRING: CharCoder(),
    BasicType.CHAR: CharCoder(),
    BasicType.BIG_DEC: BigDecimalCoder(),
    BasicType.INSTANT: InstantCoder()
}


def from_type_info(type_info: TypeInformation) -> FieldCoder:
    """
    Mappings from type_info to Coder
    """

    if isinstance(type_info, PickledBytesTypeInfo):
        return PickleCoder()
    elif isinstance(type_info, BasicTypeInfo):
        return _basic_type_info_mappings[type_info._basic_type]
    elif isinstance(type_info, DateTypeInfo):
        return DateCoder()
    elif isinstance(type_info, TimeTypeInfo):
        return TimeCoder()
    elif isinstance(type_info, TimestampTypeInfo):
        return TimestampCoder(3)
    elif isinstance(type_info, PrimitiveArrayTypeInfo):
        element_type = type_info._element_type
        if isinstance(element_type, BasicTypeInfo) and element_type._basic_type == BasicType.BYTE:
            return BinaryCoder()
        else:
            return PrimitiveArrayCoder(from_type_info(element_type))
    elif isinstance(type_info, (BasicArrayTypeInfo, ObjectArrayTypeInfo)):
        return GenericArrayCoder(from_type_info(type_info._element_type))
    elif isinstance(type_info, ListTypeInfo):
        return GenericArrayCoder(from_type_info(type_info.elem_type))
    elif isinstance(type_info, MapTypeInfo):
        return MapCoder(
            from_type_info(type_info._key_type_info), from_type_info(type_info._value_type_info))
    elif isinstance(type_info, TupleTypeInfo):
        return TupleCoder([from_type_info(field_type)
                           for field_type in type_info.get_field_types()])
    elif isinstance(type_info, RowTypeInfo):
        return RowCoder(
            [from_type_info(f) for f in type_info.get_field_types()],
            [f for f in type_info.get_field_names()])
    elif isinstance(type_info, ExternalTypeInfo):
        return from_type_info(type_info._type_info)
    elif isinstance(type_info, GenericRecordAvroTypeInfo):
        return AvroCoder(type_info._schema)
    else:
        raise ValueError("Unsupported type_info %s." % type_info)
