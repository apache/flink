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

"""Tests common to all coder implementations."""
import decimal
import logging
import os
import unittest
from unittest import mock

import pyarrow as pa
import pytz

from pyflink.fn_execution.coders import BigIntCoder, TinyIntCoder, BooleanCoder, \
    SmallIntCoder, IntCoder, FloatCoder, DoubleCoder, BinaryCoder, CharCoder, DateCoder, \
    TimeCoder, TimestampCoder, GenericArrayCoder, MapCoder, DecimalCoder, FlattenRowCoder, \
    RowCoder, LocalZonedTimestampCoder, BigDecimalCoder, TupleCoder, PrimitiveArrayCoder, \
    TimeWindowCoder, CountWindowCoder, InstantCoder
from pyflink.datastream.window import TimeWindow, CountWindow
from pyflink.testing.test_case_utils import PyFlinkTestCase


class ArrowSchemaTests(unittest.TestCase):
    def test_nested_types_require_arrow_mode(self):
        from pyflink.table import DataTypes
        from pyflink.table.types import create_arrow_schema

        for data_type, error, expected in (
            (DataTypes.ARRAY(DataTypes.ROW([DataTypes.FIELD("v", DataTypes.INT())])), ValueError,
             pa.list_(pa.field("item", pa.struct([pa.field("v", pa.int32())])))),
            (DataTypes.ARRAY(DataTypes.TIMESTAMP_LTZ(3)), ValueError, pa.list_(pa.timestamp('ms'))),
            (DataTypes.ROW([DataTypes.FIELD("r", DataTypes.ROW([
                DataTypes.FIELD("v", DataTypes.INT())]))]), TypeError,
             pa.struct([pa.field("r", pa.struct([pa.field("v", pa.int32())]))])),
            (DataTypes.ROW([DataTypes.FIELD("t", DataTypes.TIMESTAMP_LTZ(3))]), TypeError,
             pa.struct([pa.field("t", pa.timestamp('ms'))])),
        ):
            with self.subTest(data_type=data_type):
                with self.assertRaises(error):
                    create_arrow_schema(["value"], [data_type])
                schema = create_arrow_schema(["value"], [data_type], allow_nested=True)
                self.assertEqual(schema.field("value").type, expected)

    def test_pandas_collection_schema_and_round_trip(self):
        import pandas as pd
        from pyflink.table import DataTypes
        from pyflink.table.types import create_arrow_schema
        from pyflink.table.utils import arrow_to_pandas, pandas_to_arrow

        names = ["values", "lookup", "record"]
        types = [
            DataTypes.ARRAY(DataTypes.INT().not_null()),
            DataTypes.MAP(DataTypes.STRING(), DataTypes.ARRAY(DataTypes.INT()).not_null()),
            DataTypes.ROW([DataTypes.FIELD("values", DataTypes.ARRAY(DataTypes.INT().not_null()))])]
        schema = create_arrow_schema(names, types)
        self.assertEqual(schema.field("values").type.value_field.name, "item")
        self.assertFalse(schema.field("values").type.value_field.nullable)
        self.assertFalse(schema.field("lookup").type.key_field.nullable)
        self.assertFalse(schema.field("lookup").type.item_field.nullable)
        self.assertTrue(schema.field("lookup").type.item_type.value_field.nullable)
        self.assertFalse(schema.field("record").type[0].type.value_field.nullable)

        # Correcting schema metadata must not add null checks to the pandas conversion path.
        columns = [pd.Series([[1, 2], [], None, [None]]),
                   pd.Series([[('a', [1])], [], None, [('b', None)]]),
                   pd.DataFrame({"values": [[3], [], None, [None]]})]
        batch = pandas_to_arrow(schema, pytz.UTC, types, columns)
        with pa.BufferOutputStream() as output:
            with pa.ipc.new_stream(output, schema) as writer:
                writer.write_batch(batch)
            decoded = pa.ipc.open_stream(output.getvalue()).read_next_batch()
        expected = [
            {"values": [1, 2], "lookup": [('a', [1])], "record": {"values": [3]}},
            {"values": [], "lookup": [], "record": {"values": []}},
            {"values": None, "lookup": None, "record": {"values": None}},
            {"values": [None], "lookup": [('b', None)], "record": {"values": [None]}}]
        self.assertEqual(decoded.to_pylist(), expected)
        restored = arrow_to_pandas(pytz.UTC, types, [decoded])
        self.assertEqual(pandas_to_arrow(schema, pytz.UTC, types, restored).to_pylist(), expected)

    def test_arrow_descriptor_preserves_pandas_default(self):
        from pyflink.fn_execution import flink_fn_execution_pb2 as proto
        from pyflink.fn_execution.coders import LengthPrefixBaseCoder
        import pandas as pd

        arrow_type = proto.CoderInfoDescriptor.ArrowType(schema=proto.Schema(fields=[
            proto.Schema.Field(name="name", type=proto.Schema.FieldType(
                type_name=proto.Schema.VARCHAR, nullable=True,
                var_char_info=proto.Schema.VarCharInfo(length=2147483647)))]))
        descriptor = proto.CoderInfoDescriptor(arrow_type=arrow_type)
        with mock.patch.dict(os.environ, {"TABLE_LOCAL_TIME_ZONE": "UTC"}):
            pandas_coder = LengthPrefixBaseCoder._to_field_coder(descriptor).get_impl()
            result = pandas_coder.decode(pandas_coder.encode([pd.Series(["a", None])]))
            self.assertIsInstance(result[0], pd.Series)
            self.assertEqual(result[0].tolist(), ["a", None])
            descriptor.arrow_type.batch_format = proto.CoderInfoDescriptor.ArrowType.ARROW
            arrow_coder = LengthPrefixBaseCoder._to_field_coder(descriptor).get_impl()
            batch = pa.record_batch([pa.array(["a", None])], names=["name"])
            self.assertEqual(arrow_coder.decode(arrow_coder.encode(batch)), batch)


class ArrowCodersTests(unittest.TestCase):
    from pyflink.fn_execution import coder_impl_slow as implementation

    def arrow_coder(self, schema, row_type):
        return self.implementation.ArrowCoderImpl(schema, row_type, pytz.UTC, "ARROW")

    @staticmethod
    def with_parent_nulls(column, nulls):
        validity = pa.array([not value for value in nulls]).buffers()[1]
        if pa.types.is_struct(column.type):
            buffers = [validity]
            children = [column.field(index) for index in range(column.type.num_fields)]
        else:
            buffers = [validity, column.offsets.buffers()[1]]
            children = [column.values]
        return pa.Array.from_buffers(column.type, len(column), buffers, children=children)

    def test_native_arrow_timezone(self):
        from pyflink.fn_execution import flink_fn_execution_pb2 as proto
        from pyflink.fn_execution.coders import LengthPrefixBaseCoder

        schema_proto = proto.Schema(fields=[
            proto.Schema.Field(name="number", type=proto.Schema.FieldType(
                type_name=proto.Schema.BIGINT, nullable=True)),
            proto.Schema.Field(name="timestamp", type=proto.Schema.FieldType(
                type_name=proto.Schema.TIMESTAMP, nullable=True,
                timestamp_info=proto.Schema.TimestampInfo(precision=3))),
            proto.Schema.Field(name="local_timestamp", type=proto.Schema.FieldType(
                type_name=proto.Schema.LOCAL_ZONED_TIMESTAMP, nullable=True,
                local_zoned_timestamp_info=proto.Schema.LocalZonedTimestampInfo(precision=3)))])
        descriptor = proto.CoderInfoDescriptor(arrow_type=proto.CoderInfoDescriptor.ArrowType(
            schema=schema_proto, batch_format=proto.CoderInfoDescriptor.ArrowType.ARROW))
        batch = pa.record_batch([pa.array([1, None]), pa.array([0, None], type=pa.timestamp('ms')),
                                 pa.array([0, None], type=pa.timestamp('ms'))],
                                names=["number", "timestamp", "local_timestamp"])
        for timezone in ("UTC", "GMT+08:00", "SystemV/PST8PDT"):
            with self.subTest(timezone=timezone):
                with mock.patch.dict(os.environ, {"TABLE_LOCAL_TIME_ZONE": timezone}), \
                        mock.patch('pyflink.fn_execution.coders.coder_impl', self.implementation):
                    coder = LengthPrefixBaseCoder._to_field_coder(descriptor).get_impl()
                    self.assertEqual(coder.decode(coder.encode(batch)), batch)

    def test_arrow_nested_nullability(self):
        from pyflink.table import DataTypes
        from pyflink.table.types import to_arrow_type

        row_type = DataTypes.ROW([
            DataTypes.FIELD("values", DataTypes.ARRAY(DataTypes.INT().not_null()))])
        schema = pa.schema([pa.field("values", to_arrow_type(row_type.field_types()[0]))])
        coder = self.arrow_coder(schema, row_type)
        batch = pa.record_batch([pa.array([[1, None]], type=pa.list_(pa.int32()))], schema=schema)
        with self.assertRaisesRegex(ValueError, "values.*not nullable"):
            coder.encode(batch)

        valid = pa.record_batch([pa.array([[1, 2], None], type=pa.list_(pa.int32()))],
                                schema=schema)
        self.assertEqual(coder.decode(coder.encode(valid)), valid)

    def test_struct_map_and_temporal_results(self):
        import datetime
        from pyflink.table import DataTypes
        from pyflink.table.types import create_arrow_schema

        row_type = DataTypes.ROW([
            DataTypes.FIELD("record", DataTypes.ROW([
                DataTypes.FIELD("inner", DataTypes.ROW([
                    DataTypes.FIELD("value", DataTypes.INT().not_null())]))])),
            DataTypes.FIELD("lookup", DataTypes.MAP(
                DataTypes.STRING().not_null(), DataTypes.INT().not_null())),
            DataTypes.FIELD("amount", DataTypes.DECIMAL(6, 2)),
            DataTypes.FIELD("time", DataTypes.TIMESTAMP(3))])
        schema = create_arrow_schema(row_type.field_names(), row_type.field_types(),
                                     allow_nested=True)
        coder = self.arrow_coder(schema, row_type)
        rows = [
            {"record": {"inner": {"value": 7}}, "lookup": [("a", 1)],
             "amount": decimal.Decimal("12.34"), "time": datetime.datetime(2020, 1, 2)},
            {"record": None, "lookup": None, "amount": None, "time": None},
            {"record": {"inner": None}, "lookup": [],
             "amount": decimal.Decimal("-0.50"), "time": datetime.datetime(2021, 3, 4)}]
        batch = pa.RecordBatch.from_pylist(rows, schema=schema)
        self.assertEqual(coder.decode(coder.encode(batch)).to_pylist(), rows)
        self.assertEqual(coder.decode(coder.encode(batch.slice(1))).to_pylist(), rows[1:])

        for field, value, message in (
            ("record", {"inner": {"value": None}}, "record.inner.value.*not nullable"),
            ("lookup", [("a", None)], "lookup.value.*not nullable"),
        ):
            with self.subTest(field=field):
                invalid = pa.RecordBatch.from_pylist([{**rows[0], field: value}], schema=schema)
                with self.assertRaisesRegex(ValueError, message):
                    coder.encode(invalid)

        wrong = pa.StructArray.from_arrays([pa.array([1], type=pa.int64())], names=["value"])
        invalid = pa.record_batch([pa.StructArray.from_arrays([wrong], names=["inner"]),
                                   batch.column(1).slice(0, 1), batch.column(2).slice(0, 1),
                                   batch.column(3).slice(0, 1)], names=schema.names)
        with self.assertRaisesRegex(TypeError, "record.inner.value.*int64.*int32"):
            coder.encode(invalid)

    def test_native_arrow_round_trip(self):
        from pyflink.table import DataTypes

        row_type = DataTypes.ROW([DataTypes.FIELD("name", DataTypes.STRING())])
        schema = pa.schema([pa.field("name", pa.string())])
        coder = self.arrow_coder(schema, row_type)
        batch = pa.record_batch([pa.array(["ALICE", None, "BOB"])], schema=schema)
        self.assertEqual(coder.decode(coder.encode(batch)), batch)

        with self.assertRaisesRegex(TypeError, "name.*string"):
            coder.encode(pa.record_batch([pa.array([1, 2])], names=["name"]))

    def test_sliced_container_nullability(self):
        from pyflink.table import DataTypes
        from pyflink.table.types import create_arrow_schema, to_arrow_type

        item_type = DataTypes.ROW([DataTypes.FIELD("required", DataTypes.INT().not_null())])
        items = pa.array([{"required": None}, {"required": 1}, {"required": None},
                          {"required": None}, None, {"required": None}],
                         type=to_arrow_type(item_type))
        nulls = [False, False, True, False, False, False]
        offsets = pa.array(range(7), type=pa.int32())
        for data_type, column, first, last in (
            (DataTypes.ROW([DataTypes.FIELD("value", item_type)]),
             pa.StructArray.from_arrays([items], names=["value"]),
             {"value": {"required": 1}}, {"value": None}),
            (DataTypes.ARRAY(item_type), pa.ListArray.from_arrays(offsets, items),
             [{"required": 1}], [None]),
            (DataTypes.MAP(DataTypes.STRING().not_null(), item_type),
             pa.MapArray.from_arrays(offsets, pa.array(['k'] * 6), items),
             [('k', {"required": 1})], [('k', None)]),
        ):
            with self.subTest(data_type=data_type):
                column = self.with_parent_nulls(column, nulls)
                row_type = DataTypes.ROW([DataTypes.FIELD("record", DataTypes.ROW([
                    DataTypes.FIELD("container", data_type)]))])
                schema = create_arrow_schema(row_type.field_names(), row_type.field_types(),
                                             allow_nested=True)
                outer = self.with_parent_nulls(
                    pa.StructArray.from_arrays([column], names=["container"]),
                    [False, False, False, True, False, False])
                batch = pa.record_batch([outer], names=["record"])
                coder = self.arrow_coder(schema, row_type)
                self.assertEqual(coder.decode(coder.encode(batch.slice(1, 4))).to_pylist(), [
                    {"record": {"container": first}}, {"record": {"container": None}},
                    {"record": None}, {"record": {"container": last}}])
                self.assertEqual(coder.decode(coder.encode(batch.slice(0, 0))).num_rows, 0)
                with self.assertRaisesRegex(ValueError, "required.*not nullable"):
                    coder.encode(batch.slice(0, 1))

    def test_nullable_container_validation_memory(self):
        from pyflink.table import DataTypes
        from pyflink.table.types import create_arrow_schema

        count = 512
        payload = pa.array([b'x' * 2048] * count)
        nulls = [index == count // 2 for index in range(count)]
        offsets = pa.array(range(count + 1), type=pa.int32())
        for data_type, column in (
            (DataTypes.ROW([DataTypes.FIELD("payload", DataTypes.BYTES())]),
             pa.StructArray.from_arrays([payload], names=["payload"])),
            (DataTypes.ARRAY(DataTypes.BYTES()),
             pa.ListArray.from_arrays(offsets, payload)),
            (DataTypes.MAP(DataTypes.STRING().not_null(), DataTypes.BYTES()),
             pa.MapArray.from_arrays(offsets, pa.array(['k'] * count), payload)),
        ):
            with self.subTest(data_type=data_type):
                column = self.with_parent_nulls(column, nulls)
                row_type = DataTypes.ROW([DataTypes.FIELD("value", data_type)])
                schema = create_arrow_schema(["value"], [data_type], allow_nested=True)
                batch = pa.record_batch([column], names=["value"])
                coder = self.arrow_coder(schema, row_type)
                default_pool = pa.default_memory_pool()
                pool = pa.proxy_memory_pool(default_pool)
                try:
                    pa.set_memory_pool(pool)
                    encoded = coder.encode(batch)
                finally:
                    pa.set_memory_pool(default_pool)
                # Allow masks, indices and IPC metadata, but not a copy of the binary payload.
                self.assertLess(pool.max_memory(), payload.nbytes // 4)
                self.assertEqual(coder.decode(encoded).to_pylist(), batch.to_pylist())


try:
    from pyflink.fn_execution import coder_impl_fast
except ImportError:
    coder_impl_fast = None


@unittest.skipIf(coder_impl_fast is None, "Compiled coders are not installed")
class FastArrowCodersTests(ArrowCodersTests):
    implementation = coder_impl_fast


class CodersTest(PyFlinkTestCase):

    def check_coder(self, coder, *values):
        coder_impl = coder.get_impl()
        for v in values:
            if isinstance(v, float):
                from pyflink.table.tests.test_udf import float_equal
                assert float_equal(v, coder_impl.decode(coder_impl.encode(v)), 1e-6)
            else:
                self.assertEqual(v, coder_impl.decode(coder_impl.encode(v)))

    # decide whether two floats are equal
    @staticmethod
    def float_equal(a, b, rel_tol=1e-09, abs_tol=0.0):
        return abs(a - b) <= max(rel_tol * max(abs(a), abs(b)), abs_tol)

    def test_bigint_coder(self):
        coder = BigIntCoder()
        self.check_coder(coder, 1, 100, -100, -1000)

    def test_tinyint_coder(self):
        coder = TinyIntCoder()
        self.check_coder(coder, 1, 10, 127, -128)

    def test_boolean_coder(self):
        coder = BooleanCoder()
        self.check_coder(coder, True, False)

    def test_smallint_coder(self):
        coder = SmallIntCoder()
        self.check_coder(coder, 32767, -32768, 0)

    def test_int_coder(self):
        coder = IntCoder()
        self.check_coder(coder, -2147483648, 2147483647)

    def test_float_coder(self):
        coder = FloatCoder()
        self.check_coder(coder, 1.02, 1.32)

    def test_double_coder(self):
        coder = DoubleCoder()
        self.check_coder(coder, -12.02, 1.98932)

    def test_binary_coder(self):
        coder = BinaryCoder()
        self.check_coder(coder, b'pyflink')

    def test_char_coder(self):
        coder = CharCoder()
        self.check_coder(coder, 'flink', '🐿')

    def test_date_coder(self):
        import datetime
        coder = DateCoder()
        self.check_coder(coder, datetime.date(2019, 9, 10))

    def test_time_coder(self):
        import datetime
        coder = TimeCoder()
        self.check_coder(coder, datetime.time(hour=11, minute=11, second=11, microsecond=123000))

    def test_timestamp_coder(self):
        import datetime
        coder = TimestampCoder(3)
        self.check_coder(coder, datetime.datetime(2019, 9, 10, 18, 30, 20, 123000))
        coder = TimestampCoder(6)
        self.check_coder(coder, datetime.datetime(2019, 9, 10, 18, 30, 20, 123456))

    def test_local_zoned_timestamp_coder(self):
        import datetime
        import pytz
        timezone = pytz.timezone("Asia/Shanghai")
        coder = LocalZonedTimestampCoder(3, timezone)
        self.check_coder(coder,
                         timezone.localize(datetime.datetime(2019, 9, 10, 18, 30, 20, 123000)))
        coder = LocalZonedTimestampCoder(6, timezone)
        self.check_coder(coder,
                         timezone.localize(datetime.datetime(2019, 9, 10, 18, 30, 20, 123456)))

    def test_instant_coder(self):
        from pyflink.common.time import Instant

        coder = InstantCoder()
        self.check_coder(coder, Instant(100, 2000), None, Instant(-9223372036854775808, 0))

    def test_array_coder(self):
        element_coder = BigIntCoder()
        coder = GenericArrayCoder(element_coder)
        self.check_coder(coder, [1, 2, 3, None])

    def test_primitive_array_coder(self):
        element_coder = CharCoder()
        coder = PrimitiveArrayCoder(element_coder)
        self.check_coder(coder, ['hi', 'hello', 'flink'])

    def test_map_coder(self):
        key_coder = CharCoder()
        value_coder = BigIntCoder()
        coder = MapCoder(key_coder, value_coder)
        self.check_coder(coder, {'flink': 1, 'pyflink': 2, 'coder': None})

    def test_decimal_coder(self):
        import decimal
        coder = DecimalCoder(38, 18)
        self.check_coder(coder, decimal.Decimal('0.00001'), decimal.Decimal('1.23E-8'))
        coder = DecimalCoder(4, 3)
        decimal.getcontext().prec = 2
        self.check_coder(coder, decimal.Decimal('1.001'))
        self.assertEqual(decimal.getcontext().prec, 2)

    def test_flatten_row_coder(self):
        field_coder = BigIntCoder()
        field_count = 10
        coder = FlattenRowCoder([field_coder for _ in range(field_count)]).get_impl()
        v = [None if i % 2 == 0 else i for i in range(field_count)]
        generator_result = coder.decode(coder.encode(v))
        result = []
        for item in generator_result:
            result.append(item)
        self.assertEqual(v, result)

    def test_row_coder(self):
        from pyflink.common import Row, RowKind
        field_coder = BigIntCoder()
        field_count = 10
        field_names = ['f{}'.format(i) for i in range(field_count)]
        coder = RowCoder([field_coder for _ in range(field_count)], field_names)
        v = Row(**{field_names[i]: None if i % 2 == 0 else i for i in range(field_count)})
        v.set_row_kind(RowKind.INSERT)
        self.check_coder(coder, v)
        v.set_row_kind(RowKind.UPDATE_BEFORE)
        self.check_coder(coder, v)
        v.set_row_kind(RowKind.UPDATE_AFTER)
        self.check_coder(coder, v)
        v.set_row_kind(RowKind.DELETE)
        self.check_coder(coder, v)

        coder = RowCoder([BigIntCoder(), CharCoder()], ['f1', 'f0'])
        v = Row(f0="flink", f1=11)
        self.check_coder(coder, v)

    def test_basic_decimal_coder(self):
        basic_dec_coder = BigDecimalCoder()
        value = decimal.Decimal(1.200)
        self.check_coder(basic_dec_coder, value)

    def test_tuple_coder(self):
        field_coders = [IntCoder(), CharCoder(), CharCoder()]
        tuple_coder = TupleCoder(field_coders=field_coders)
        data = (1, "Hello", "Hi")
        self.check_coder(tuple_coder, data)

    def test_window_coder(self):
        coder = TimeWindowCoder()
        self.check_coder(coder, TimeWindow(100, 1000))
        coder = CountWindowCoder()
        self.check_coder(coder, CountWindow(100))

    def test_coder_with_unmatched_type(self):
        from pyflink.common import Row
        coder = FlattenRowCoder([BigIntCoder()])
        with self.assertRaises(TypeError, msg='Expected list, got Row'):
            self.check_coder(coder, Row(1))


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    unittest.main()
