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

from pyflink.common import Row
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


class LiteralITTests(PyFlinkBatchTableTestCase):
    def test_timezone_aware_datetime_literals_preserve_instant(self):
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
            lit(fractional_offset_instant[0]) == lit(fractional_offset_instant[1]),
            lit(
                fractional_offset_instant[0],
                DataTypes.TIMESTAMP_LTZ(6).not_null(),
            )
            == lit(
                fractional_offset_instant[1],
                DataTypes.TIMESTAMP_LTZ(6).not_null(),
            ),
            lit(ambiguous_local_time.replace(fold=0))
            == lit(datetime.datetime(2026, 11, 1, 5, 30, tzinfo=datetime.timezone.utc)),
            lit(ambiguous_local_time.replace(fold=1))
            == lit(datetime.datetime(2026, 11, 1, 6, 30, tzinfo=datetime.timezone.utc)),
            lit(ambiguous_local_time.replace(fold=0))
            != lit(ambiguous_local_time.replace(fold=1)),
        )

        self.assertEqual(
            list(result.execute().collect()),
            [Row(True, True, True, True, True, True)],
        )

    def test_scalar_literals_can_be_executed(self):
        source = self.t_env.from_elements([(1,)], ["id"])

        result = source.select(
            lit(True),
            lit(2),
            lit(1.25),
            lit("x"),
            lit(b"x"),
            lit(bytearray(b"y")),
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

    def test_empty_python_arrays_preserve_numeric_typecodes(self):
        typecodes = sorted(set(_array_type_mappings) - {"u"})
        source = self.t_env.from_elements([(1,)], ["id"])
        result = source.select(*(lit(array.array(typecode)) for typecode in typecodes))

        expected_types = [
            DataTypes.ARRAY(_array_type_mappings[typecode]).not_null()
            for typecode in typecodes
        ]
        self.assertEqual(result.get_resolved_schema().get_column_data_types(), expected_types)
        self.assertEqual(list(result.execute().collect()), [Row(*([[]] * len(typecodes)))])

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
