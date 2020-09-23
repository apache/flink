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
import pyarrow as pa
import pytz
from apache_beam.coders import Coder
from apache_beam.coders.coders import FastCoder, LengthPrefixCoder
from apache_beam.portability import common_urns
from apache_beam.typehints import typehints

from pyflink.fn_execution.beam import beam_coder_impl_slow
from pyflink.fn_execution.coders import FLINK_MAP_FUNCTION_DATA_STREAM_CODER_URN, \
    FLINK_FLAT_MAP_FUNCTION_DATA_STREAM_CODER_URN

try:
    from pyflink.fn_execution.beam import beam_coder_impl_fast as beam_coder_impl
    from pyflink.fn_execution.beam.beam_coder_impl_fast import BeamCoderImpl
except ImportError:
    beam_coder_impl = beam_coder_impl_slow
    BeamCoderImpl = lambda a: a

from pyflink.fn_execution import flink_fn_execution_pb2, coders
from pyflink.table.types import TinyIntType, SmallIntType, IntType, BigIntType, BooleanType, \
    FloatType, DoubleType, VarCharType, VarBinaryType, DecimalType, DateType, TimeType, \
    LocalZonedTimestampType, RowType, RowField, to_arrow_type, TimestampType, ArrayType


class PassThroughLengthPrefixCoder(LengthPrefixCoder):
    """
    Coder which doesn't prefix the length of the encoded object as the length prefix will be handled
    by the wrapped value coder.
    """

    def __init__(self, value_coder):
        super(PassThroughLengthPrefixCoder, self).__init__(value_coder)

    def _create_impl(self):
        return beam_coder_impl.PassThroughLengthPrefixCoderImpl(self._value_coder.get_impl())

    def __repr__(self):
        return 'PassThroughLengthPrefixCoder[%s]' % self._value_coder


Coder.register_structured_urn(
    common_urns.coders.LENGTH_PREFIX.urn, PassThroughLengthPrefixCoder)


class BeamTableFunctionRowCoder(FastCoder):
    """
    Coder for Table Function Row.
    """

    def __init__(self, table_function_row_coder):
        self._table_function_row_coder = table_function_row_coder

    def _create_impl(self):
        return self._table_function_row_coder.get_impl()

    def get_impl(self):
        return BeamCoderImpl(self._create_impl())

    def to_type_hint(self):
        return typehints.List

    @Coder.register_urn(coders.FLINK_TABLE_FUNCTION_SCHEMA_CODER_URN, flink_fn_execution_pb2.Schema)
    def _pickle_from_runner_api_parameter(schema_proto, unused_components, unused_context):
        return BeamTableFunctionRowCoder(
            coders.TableFunctionRowCoder.from_schema_proto(schema_proto))

    def __repr__(self):
        return 'TableFunctionRowCoder[%s]' % repr(self._table_function_row_coder)

    def __eq__(self, other):
        return (self.__class__ == other.__class__
                and self._table_function_row_coder == other._table_function_row_coder)

    def __ne__(self, other):
        return not self == other

    def __hash__(self):
        return hash(self._table_function_row_coder)


class BeamAggregateFunctionRowCoder(FastCoder):
    """
    Coder for Aggregate Function input row.
    """

    def __init__(self, aggregate_function_row_coder):
        self._aggregate_function_row_coder = aggregate_function_row_coder

    def _create_impl(self):
        return self._aggregate_function_row_coder.get_impl()

    def get_impl(self):
        return BeamCoderImpl(self._create_impl())

    def to_type_hint(self):
        return typehints.List

    @Coder.register_urn(coders.FLINK_AGGREGATE_FUNCTION_SCHEMA_CODER_URN,
                        flink_fn_execution_pb2.Schema)
    def _pickle_from_runner_api_parameter(schema_proto, unused_components, unused_context):
        return BeamAggregateFunctionRowCoder(
            coders.AggregateFunctionRowCoder.from_schema_proto(schema_proto))

    def __repr__(self):
        return 'BeamAggregateFunctionRowCoder[%s]' % repr(self._aggregate_function_row_coder)

    def __eq__(self, other):
        return (self.__class__ == other.__class__
                and self._aggregate_function_row_coder == other._aggregate_function_row_coder)

    def __ne__(self, other):
        return not self == other

    def __hash__(self):
        return hash(self._aggregate_function_row_coder)


class BeamFlattenRowCoder(FastCoder):
    """
    Coder for Row. The decoded result will be flattened as a list of column values of a row instead
    of a row object.
    """

    def __init__(self, flatten_coder):
        self._flatten_coder = flatten_coder

    def _create_impl(self):
        return self._flatten_coder.get_impl()

    def get_impl(self):
        return BeamCoderImpl(self._create_impl())

    def to_type_hint(self):
        return typehints.List

    @Coder.register_urn(coders.FLINK_SCALAR_FUNCTION_SCHEMA_CODER_URN,
                        flink_fn_execution_pb2.Schema)
    def _pickle_from_runner_api_parameter(schema_proto, unused_components, unused_context):
        return BeamFlattenRowCoder(coders.FlattenRowCoder.from_schema_proto(schema_proto))

    def __repr__(self):
        return 'BeamFlattenRowCoder[%s]' % repr(self._flatten_coder)

    def __eq__(self, other):
        return (self.__class__ == other.__class__
                and self._flatten_coder == other._flatten_coder)

    def __ne__(self, other):
        return not self == other

    def __hash__(self):
        return hash(self._flatten_coder)


class ArrowCoder(FastCoder):
    """
    Coder for Arrow.
    """

    def __init__(self, schema, row_type, timezone):
        self._schema = schema
        self._row_type = row_type
        self._timezone = timezone

    def _create_impl(self):
        return beam_coder_impl_slow.ArrowCoderImpl(self._schema, self._row_type, self._timezone)

    def to_type_hint(self):
        import pandas as pd
        return pd.Series

    @Coder.register_urn(coders.FLINK_SCHEMA_ARROW_CODER_URN,
                        flink_fn_execution_pb2.Schema)
    @Coder.register_urn(coders.FLINK_SCALAR_FUNCTION_SCHEMA_ARROW_CODER_URN,
                        flink_fn_execution_pb2.Schema)
    def _pickle_from_runner_api_parameter(schema_proto, unused_components, unused_context):

        def _to_arrow_schema(row_type):
            return pa.schema([pa.field(n, to_arrow_type(t), t._nullable)
                              for n, t in zip(row_type.field_names(), row_type.field_types())])

        def _to_data_type(field_type):
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
            elif field_type.type_name == flink_fn_execution_pb2.Schema.VARCHAR:
                return VarCharType(0x7fffffff, field_type.nullable)
            elif field_type.type_name == flink_fn_execution_pb2.Schema.VARBINARY:
                return VarBinaryType(0x7fffffff, field_type.nullable)
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
                return ArrayType(_to_data_type(field_type.collection_element_type),
                                 field_type.nullable)
            elif field_type.type_name == flink_fn_execution_pb2.Schema.TypeName.ROW:
                return RowType(
                    [RowField(f.name, _to_data_type(f.type), f.description)
                     for f in field_type.row_schema.fields], field_type.nullable)
            else:
                raise ValueError("field_type %s is not supported." % field_type)

        def _to_row_type(row_schema):
            return RowType([RowField(f.name, _to_data_type(f.type)) for f in row_schema.fields])

        timezone = pytz.timezone(os.environ['table.exec.timezone'])
        row_type = _to_row_type(schema_proto)
        return ArrowCoder(_to_arrow_schema(row_type), row_type, timezone)

    def __repr__(self):
        return 'ArrowCoder[%s]' % self._schema


class OverWindowArrowCoder(FastCoder):
    """
    Coder for batch pandas over window aggregation.
    """
    def __init__(self, arrow_coder):
        self._arrow_coder = arrow_coder

    def _create_impl(self):
        return beam_coder_impl_slow.OverWindowArrowCoderImpl(
            self._arrow_coder._create_impl())

    def to_type_hint(self):
        return typehints.List

    @Coder.register_urn(coders.FLINK_OVER_WINDOW_ARROW_CODER_URN, flink_fn_execution_pb2.Schema)
    def _pickle_from_runner_api_parameter(schema_proto, unused_components, unused_context):
        return OverWindowArrowCoder(
            ArrowCoder._pickle_from_runner_api_parameter(
                schema_proto, unused_components, unused_context))

    def __repr__(self):
        return 'OverWindowArrowCoder[%s]' % self._arrow_coder


class BeamDataStreamStatelessMapCoder(FastCoder):

    def __init__(self, field_coder):
        self._field_coder = field_coder

    def _create_impl(self):
        return self._field_coder.get_impl()

    def get_impl(self):
        return BeamCoderImpl(self._create_impl())

    def is_deterministic(self):  # type: () -> bool
        return all(c.is_deterministic() for c in self._field_coder)

    @Coder.register_urn(FLINK_MAP_FUNCTION_DATA_STREAM_CODER_URN, flink_fn_execution_pb2.TypeInfo)
    def _pickled_from_runner_api_parameter(type_info_proto, unused_components, unused_context):
        return BeamDataStreamStatelessMapCoder(
            coders.DataStreamStatelessMapCoder.from_type_info_proto(type_info_proto))

    def to_type_hint(self):
        return typehints.Any

    def __repr__(self):
        return 'BeamDataStreamStatelessMapCoder[%s]' % repr(self._field_coder)


class BeamDataStreamStatelessFlatMapCoder(FastCoder):

    def __init__(self, field_coder):
        self._field_coder = field_coder

    def _create_impl(self):
        return self._field_coder.get_impl()

    def get_impl(self):
        return BeamCoderImpl(self._create_impl())

    def is_deterministic(self):  # type: () -> bool
        return all(c.is_deterministic() for c in self._field_coder)

    @Coder.register_urn(FLINK_FLAT_MAP_FUNCTION_DATA_STREAM_CODER_URN,
                        flink_fn_execution_pb2.TypeInfo)
    def _pickled_from_runner_api_parameter(type_info_proto, unused_components, unused_context):
        return BeamDataStreamStatelessFlatMapCoder(
            coders.DataStreamStatelessFlatMapCoder.from_type_info_proto(type_info_proto))

    def to_type_hint(self):
        return typehints.Generator

    def __repr__(self):
        return 'BeamDataStreamStatelessFlatMapCoder[%s]' % repr(self._field_coder)
