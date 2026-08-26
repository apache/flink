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

import array
import datetime
import decimal

from py4j.protocol import Py4JJavaError

from pyflink.common import Row, RowKind
from pyflink.java_gateway import get_gateway
from pyflink.table import DataTypes
from pyflink.table.expressions import lit
from pyflink.table.types import _array_type_mappings
from pyflink.testing.test_case_utils import PyFlinkBatchTableTestCase


# Keep the fold test independent of the system's IANA time zone database.
class _FoldAwareTimezone(datetime.tzinfo):
    def utcoffset(self, value):
        return datetime.timedelta(hours=-4 if value.fold == 0 else -5)

    def dst(self, value):
        return datetime.timedelta(hours=1 if value.fold == 0 else 0)


class LiteralITCase(PyFlinkBatchTableTestCase):
    def test_timezone_aware_datetime_literals(self):
        source = self.t_env.from_elements([(1,)], ["id"])
        same_instant_with_different_offsets = (
            datetime.datetime(
                2026, 8, 3, 12, tzinfo=datetime.timezone(datetime.timedelta(hours=8))
            ),
            datetime.datetime(2026, 8, 3, 4, tzinfo=datetime.timezone.utc),
        )
        fractional_offset_instant = (
            datetime.datetime(
                1970,
                1,
                1,
                0,
                0,
                0,
                100000,
                tzinfo=datetime.timezone(datetime.timedelta(microseconds=500000)),
            ),
            datetime.datetime(
                1969, 12, 31, 23, 59, 59, 600000, tzinfo=datetime.timezone.utc
            ),
        )
        ambiguous_local_time = datetime.datetime(
            2026, 11, 1, 1, 30, tzinfo=_FoldAwareTimezone()
        )

        result = source.select(
            lit(same_instant_with_different_offsets[0])
            == lit(same_instant_with_different_offsets[1]),
            lit(
                same_instant_with_different_offsets[0],
                DataTypes.TIMESTAMP(6).not_null(),
            )
            == lit(
                same_instant_with_different_offsets[1],
                DataTypes.TIMESTAMP(6).not_null(),
            ),
            lit(fractional_offset_instant[0]) == lit(fractional_offset_instant[1]),
            lit(
                fractional_offset_instant[0],
                DataTypes.TIMESTAMP_LTZ(6).not_null(),
            )
            == lit(
                fractional_offset_instant[1],
                DataTypes.TIMESTAMP_LTZ(6).not_null(),
            ),
            lit(
                ambiguous_local_time.replace(fold=0),
                DataTypes.TIMESTAMP_LTZ(6).not_null(),
            )
            == lit(
                datetime.datetime(2026, 11, 1, 5, 30, tzinfo=datetime.timezone.utc),
                DataTypes.TIMESTAMP_LTZ(6).not_null(),
            ),
            lit(
                ambiguous_local_time.replace(fold=1),
                DataTypes.TIMESTAMP_LTZ(6).not_null(),
            )
            == lit(
                datetime.datetime(2026, 11, 1, 6, 30, tzinfo=datetime.timezone.utc),
                DataTypes.TIMESTAMP_LTZ(6).not_null(),
            ),
            lit(
                ambiguous_local_time.replace(fold=0),
                DataTypes.TIMESTAMP_LTZ(6).not_null(),
            )
            != lit(
                ambiguous_local_time.replace(fold=1),
                DataTypes.TIMESTAMP_LTZ(6).not_null(),
            ),
        )

        self.assertEqual(
            list(result.execute().collect()),
            [Row(True, True, True, True, True, True, True)],
        )

    def test_aware_temporal_literals_use_client_timezone(self):
        jvm = get_gateway().jvm
        original_timezone = jvm.java.util.TimeZone.getDefault()
        try:
            jvm.java.util.TimeZone.setDefault(
                jvm.java.util.TimeZone.getTimeZone("Asia/Shanghai")
            )
            source = self.t_env.from_elements([(1,)], ["id"])
            aware_time = datetime.time(4, tzinfo=datetime.timezone.utc)
            aware_timestamp = datetime.datetime(
                2026, 8, 3, 4, tzinfo=datetime.timezone.utc
            )
            inferred_time = lit(aware_time)
            explicit_time = lit(aware_time, DataTypes.TIME().not_null())
            inferred_timestamp = lit(aware_timestamp)
            explicit_timestamp = lit(
                aware_timestamp, DataTypes.TIMESTAMP().not_null()
            )

            local_time_class = jvm.java.lang.Class.forName("java.time.LocalTime")
            local_datetime_class = jvm.java.lang.Class.forName(
                "java.time.LocalDateTime"
            )
            expected_time = jvm.java.time.LocalTime.of(12, 0)
            expected_timestamp = jvm.java.time.LocalDateTime.of(2026, 8, 3, 12, 0)
            for expression in (inferred_time, explicit_time):
                literal = expression._j_expr.toExpr()
                self.assertEqual(expected_time, literal.getValueAs(local_time_class).get())
            for expression in (inferred_timestamp, explicit_timestamp):
                literal = expression._j_expr.toExpr()
                self.assertEqual(
                    expected_timestamp,
                    literal.getValueAs(local_datetime_class).get(),
                )

            result = source.select(
                inferred_time == explicit_time,
                inferred_timestamp == explicit_timestamp,
            )
            self.assertEqual(
                list(result.execute().collect()),
                [Row(True, True)],
            )
        finally:
            jvm.java.util.TimeZone.setDefault(original_timezone)

    def test_scalar_literals_can_be_executed(self):
        source = self.t_env.from_elements([(1,)], ["id"])

        result = source.select(
            lit(True),
            lit(2),
            lit(1.25),
            lit("x"),
            lit(b"x"),
            lit(bytearray(b"y")),
            lit(b"x", DataTypes.BINARY(1).not_null()),
            lit(bytearray(b"y"), DataTypes.BINARY(1).not_null()),
            lit(decimal.Decimal("1.25")),
            lit(datetime.date(2026, 8, 3)),
            lit(datetime.time(1, 2, 3, 4000)),
            lit(datetime.datetime(2026, 8, 3, 1, 2, 3, 4000)),
            lit(datetime.timedelta(days=1, seconds=2, microseconds=3000)).is_not_null,
            lit(1, DataTypes.TINYINT().not_null()),
            lit(1, DataTypes.SMALLINT().not_null()),
            lit(1, DataTypes.BIGINT().not_null()),
            lit(1.25, DataTypes.FLOAT().not_null()),
            lit(
                datetime.datetime(2026, 8, 3, 1, 2, 3, 4000),
                DataTypes.DATE().not_null(),
            ),
            lit(
                datetime.time(1, 2, 3, 4000),
                DataTypes.TIME(6).not_null(),
            ),
            lit(
                datetime.datetime(2026, 8, 3, 1, 2, 3, 4000),
                DataTypes.TIMESTAMP(6).not_null(),
            ),
            lit(
                datetime.datetime(
                    2026,
                    8,
                    3,
                    1,
                    2,
                    3,
                    4000,
                    datetime.timezone.utc,
                ),
                DataTypes.TIMESTAMP_LTZ(6).not_null(),
            ).is_not_null,
            lit(
                datetime.timedelta(days=1, seconds=2, microseconds=3000),
                DataTypes.INTERVAL(DataTypes.DAY(), DataTypes.SECOND(6)).not_null(),
            ).is_not_null,
            lit(
                14,
                DataTypes.INTERVAL(DataTypes.YEAR(), DataTypes.MONTH()).not_null(),
            ).is_not_null,
            lit(None, DataTypes.ARRAY(DataTypes.SMALLINT())),
            lit(None, DataTypes.MAP(DataTypes.SMALLINT(), DataTypes.FLOAT())),
            lit(
                None,
                DataTypes.ROW(
                    [DataTypes.FIELD("small_value", DataTypes.SMALLINT())]
                ),
            ),
            lit(None, DataTypes.MULTISET(DataTypes.SMALLINT())),
        )

        self.assertEqual(
            list(result.execute().collect()),
            [
                Row(
                    True,
                    2,
                    1.25,
                    "x",
                    b"x",
                    b"y",
                    b"x",
                    b"y",
                    decimal.Decimal("1.25"),
                    datetime.date(2026, 8, 3),
                    datetime.time(1, 2, 3, 4000),
                    datetime.datetime(2026, 8, 3, 1, 2, 3, 4000),
                    True,
                    1,
                    1,
                    1,
                    1.25,
                    datetime.date(2026, 8, 3),
                    datetime.time(1, 2, 3, 4000),
                    datetime.datetime(2026, 8, 3, 1, 2, 3, 4000),
                    True,
                    True,
                    True,
                    None,
                    None,
                    None,
                    None,
                )
            ],
        )

    def test_constructed_literals_can_be_executed(self):
        source = self.t_env.from_elements([(1,)], ["id"])
        row_type = DataTypes.ROW(
            [
                DataTypes.FIELD("small_value", DataTypes.SMALLINT()),
                DataTypes.FIELD("float_value", DataTypes.FLOAT()),
            ]
        ).not_null()
        map_type = DataTypes.MAP(
            DataTypes.SMALLINT(),
            DataTypes.FLOAT(),
        ).not_null()
        nested_type = DataTypes.ARRAY(
            DataTypes.ROW(
                [
                    DataTypes.FIELD("values", DataTypes.ARRAY(DataTypes.SMALLINT())),
                    DataTypes.FIELD("mapping", map_type),
                ]
            )
        ).not_null()

        result = source.select(
            lit(["abc"]),
            lit([[datetime.date(2026, 8, 3)]]),
            lit((1, 2)),
            lit([1, 2], DataTypes.ARRAY(DataTypes.SMALLINT()).not_null()),
            lit(
                [b"x"],
                DataTypes.ARRAY(DataTypes.BINARY(1)).not_null(),
            ),
            lit((1, 1.25), row_type),
            lit({1: 1.25}, map_type),
            lit([([1, 2], {3: 1.25})], nested_type),
            lit(
                [],
                DataTypes.ARRAY(DataTypes.SMALLINT().not_null()).not_null(),
            ),
            lit({}, map_type),
            lit(array.array("h")),
            *(lit(array.array(typecode, [1, 2])) for typecode in "bhilfd"),
        )

        self.assertIsInstance(result.explain(), str)
        self.assertEqual(
            list(result.execute().collect()),
            [
                Row(
                    ["abc"],
                    [[datetime.date(2026, 8, 3)]],
                    [1, 2],
                    [1, 2],
                    [b"x"],
                    Row(1, 1.25),
                    {1: 1.25},
                    [Row([1, 2], {3: 1.25})],
                    [],
                    {},
                    [],
                    [1, 2],
                    [1, 2],
                    [1, 2],
                    [1, 2],
                    [1.0, 2.0],
                    [1.0, 2.0],
                )
            ],
        )

    def test_inferred_nested_arrays_use_sibling_types(self):
        source = self.t_env.from_elements([(1,)], ["id"])
        empty_inner_array = lit([[1], []])
        null_only_inner_array = lit([[1], [None]])
        char_empty_inner_array = lit([["a"], []])
        char_null_only_inner_array = lit([["a"], [None]])
        decimal_empty_inner_array = lit([[decimal.Decimal("1.20")], []])
        time_empty_inner_array = lit([[datetime.time(12, 0, 0, 123000)], []])
        binary_empty_inner_array = lit([[b"a"], []])
        nested_array_type = DataTypes.ARRAY(DataTypes.ARRAY(DataTypes.INT())).not_null()

        literal_table = source.select(
            empty_inner_array,
            null_only_inner_array,
            char_empty_inner_array,
            char_null_only_inner_array,
            decimal_empty_inner_array,
            time_empty_inner_array,
            binary_empty_inner_array,
        )
        self.assertEqual(
            literal_table.get_resolved_schema().get_column_data_types(),
            [
                nested_array_type,
                nested_array_type,
                DataTypes.ARRAY(DataTypes.ARRAY(DataTypes.CHAR(1))).not_null(),
                DataTypes.ARRAY(DataTypes.ARRAY(DataTypes.CHAR(1))).not_null(),
                DataTypes.ARRAY(DataTypes.ARRAY(DataTypes.DECIMAL(3, 2))).not_null(),
                DataTypes.ARRAY(DataTypes.ARRAY(DataTypes.TIME(3))).not_null(),
                DataTypes.ARRAY(DataTypes.ARRAY(DataTypes.BINARY(1))).not_null(),
            ],
        )
        self.assertIsInstance(literal_table.explain(), str)

        result = source.select(
            empty_inner_array.cardinality,
            empty_inner_array.at(1).cardinality,
            empty_inner_array.at(2).cardinality,
            empty_inner_array.at(1).at(1),
            null_only_inner_array.at(2).cardinality,
            null_only_inner_array.at(2).at(1).is_null,
            char_empty_inner_array.at(1).at(1),
            char_empty_inner_array.at(2).cardinality,
            char_null_only_inner_array.at(2).at(1).is_null,
            decimal_empty_inner_array.at(1).at(1),
            decimal_empty_inner_array.at(2).cardinality,
            time_empty_inner_array.at(1).at(1),
            time_empty_inner_array.at(2).cardinality,
            binary_empty_inner_array.at(1).at(1),
            binary_empty_inner_array.at(2).cardinality,
        )
        self.assertEqual(
            list(result.execute().collect()),
            [
                Row(
                    2,
                    1,
                    0,
                    1,
                    1,
                    True,
                    "a",
                    0,
                    True,
                    decimal.Decimal("1.20"),
                    0,
                    datetime.time(12, 0, 0, 123000),
                    0,
                    b"a",
                    0,
                )
            ],
        )

    def test_unsupported_constructed_literals_are_rejected(self):
        with self.assertRaisesRegex(Py4JJavaError, "Non-null MULTISET literals are not supported"):
            lit({1: 2}, DataTypes.MULTISET(DataTypes.SMALLINT()).not_null())

        with self.assertRaisesRegex(Py4JJavaError, "Non-null empty ROW literals are not supported"):
            lit((), DataTypes.ROW([]).not_null())

        with self.assertRaises(Py4JJavaError):
            lit([1.25], DataTypes.ARRAY(DataTypes.SMALLINT()).not_null())

        with self.assertRaisesRegex(Py4JJavaError, "ROW literal has arity 2"):
            lit(
                (1, 2),
                DataTypes.ROW([DataTypes.FIELD("value", DataTypes.INT())]).not_null(),
            )

        with self.assertRaisesRegex(Py4JJavaError, "ROW literal has arity 2"):
            lit(
                Row(a=1, b=2),
                DataTypes.ROW([DataTypes.FIELD("a", DataTypes.INT())]).not_null(),
            )

        with self.assertRaisesRegex(Py4JJavaError, "Unsupported kind 'DELETE'"):
            lit(
                Row.of_kind(RowKind.DELETE, 1),
                DataTypes.ROW([DataTypes.FIELD("value", DataTypes.INT())]).not_null(),
            )

    def test_primitive_java_arrays_can_be_executed(self):
        gateway = get_gateway()
        jvm = gateway.jvm
        primitive_values = [
            (jvm.boolean, True, DataTypes.BOOLEAN().not_null()),
            (jvm.short, 1, DataTypes.SMALLINT().not_null()),
            (jvm.int, 1, DataTypes.INT().not_null()),
            (jvm.long, 1, DataTypes.BIGINT().not_null()),
            (jvm.float, 1.0, DataTypes.FLOAT().not_null()),
            (jvm.double, 1.0, DataTypes.DOUBLE().not_null()),
        ]
        expressions = []
        for primitive_class, value, _ in primitive_values:
            j_array = gateway.new_array(primitive_class, 1)
            j_array[0] = value
            expressions.append(lit(j_array))

        source = self.t_env.from_elements([(1,)], ["id"])
        result = source.select(*expressions)
        self.assertEqual(
            result.get_resolved_schema().get_column_data_types(),
            [
                DataTypes.ARRAY(element_data_type).not_null()
                for _, _, element_data_type in primitive_values
            ],
        )
        self.assertIsInstance(result.explain(), str)
        self.assertEqual(
            list(result.execute().collect()),
            [Row([True], [1], [1], [1], [1.0], [1.0])],
        )

        j_int_array = gateway.new_array(jvm.int, 1)
        j_int_array[0] = 1
        nested_expression = lit([j_int_array, []])
        nested_result = source.select(
            nested_expression.cardinality,
            nested_expression.at(1).cardinality,
            nested_expression.at(2).cardinality,
            nested_expression.at(1).at(1),
        )
        self.assertIsInstance(nested_result.explain(), str)
        self.assertEqual(
            list(nested_result.execute().collect()),
            [Row(2, 1, 0, 1)],
        )

    def test_empty_python_arrays_preserve_typecodes(self):
        typecodes = sorted(_array_type_mappings)
        source = self.t_env.from_elements([(1,)], ["id"])
        result = source.select(*(lit(array.array(typecode)) for typecode in typecodes))

        expected_types = [
            DataTypes.ARRAY(_array_type_mappings[typecode]).not_null()
            for typecode in typecodes
        ]
        self.assertEqual(result.get_resolved_schema().get_column_data_types(), expected_types)
        self.assertEqual(list(result.execute().collect()), [Row(*([[]] * len(typecodes)))])

    def test_empty_unicode_array_typecode_propagates_to_sibling(self):
        if "u" not in _array_type_mappings:
            self.skipTest("Unicode arrays are not supported on this Python version")

        source = self.t_env.from_elements([(1,)], ["id"])
        expression = lit([array.array("u"), []])
        result = source.select(expression)

        self.assertEqual(
            result.get_resolved_schema().get_column_data_types(),
            [
                DataTypes.ARRAY(
                    DataTypes.ARRAY(_array_type_mappings["u"])
                ).not_null()
            ],
        )
        self.assertIsInstance(result.explain(), str)
        self.assertEqual(
            list(
                source.select(
                    expression.at(1).cardinality,
                    expression.at(2).cardinality,
                )
                .execute()
                .collect()
            ),
            [Row(0, 0)],
        )

    def test_unicode_python_array_can_be_executed(self):
        if "u" not in _array_type_mappings:
            self.skipTest("Unicode arrays are not supported on this Python version")

        source = self.t_env.from_elements([(1,)], ["id"])
        result = source.select(lit(array.array("u", "ab")))

        self.assertEqual(
            result.get_resolved_schema().get_column_data_types(),
            [DataTypes.ARRAY(DataTypes.CHAR(1)).not_null()],
        )
        self.assertEqual(list(result.execute().collect()), [Row(["a", "b"])])
