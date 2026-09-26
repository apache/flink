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
from datetime import datetime, timedelta

import pyflink.dataframe as pf
from pyflink.common import Row
from pyflink.datastream import RuntimeExecutionMode, StreamExecutionEnvironment
from pyflink.java_gateway import get_gateway
from pyflink.table import StreamTableEnvironment
from pyflink.table.expressions import (
    CURRENT_ROW as TABLE_CURRENT_ROW,
    lit,
    row,
)
from pyflink.table.table import Table
from pyflink.table.utils import to_expression_jarray
from pyflink.testing.test_case_utils import PyFlinkTestCase
from pyflink.util.exceptions import TableException
from pyflink.util.java_utils import to_jarray


def _register_rowtime_events(table_environment, input_path, rows):
    with open(input_path, "w", encoding="utf-8") as events:
        events.write("\n".join(rows) + "\n")
    table_environment.execute_sql("DROP TEMPORARY TABLE IF EXISTS events")
    table_environment.execute_sql(
        "CREATE TEMPORARY TABLE events ("
        "amount BIGINT, event_time TIMESTAMP(3), "
        "WATERMARK FOR event_time AS event_time - INTERVAL '1' SECOND"
        ") WITH ("
        "'connector' = 'filesystem', "
        "'path' = 'file://%s', "
        "'format' = 'csv'"
        ")" % input_path
    )
    return pf.from_table(table_environment.from_path("events"))


class OverWindowTests(PyFlinkTestCase):

    def test_legacy_over_call_is_preserved(self):
        self.assertEqual("over(sum(amount), w)", str(pf.col("amount").sum.over(pf.col("w"))))

    def test_dataframe_over_supports_scalar_and_explicit_frames(self):
        scalar = pf.col("amount").sum.over(order_by="rowtime", rows=2)
        explicit = pf.col("amount").sum.over(
            order_by="rowtime",
            partition_by="category",
            rows=(pf.preceding(2), pf.following(1)),
        )

        self.assertIn("over(sum(amount), rowtime, 2, CURRENT_ROW)", str(scalar))
        self.assertIn("over(sum(amount), rowtime, 2, 1, category)", str(explicit))

    def test_dataframe_over_expression_remains_composable(self):
        expression = (
            pf.col("amount").sum.over(order_by="rowtime", rows=2) + 1
        ).alias("adjusted")
        self.assertIn("as(plus(over(sum(amount)", str(expression))

    def test_over_rejects_invalid_dispatch(self):
        with self.assertRaisesRegex(TypeError, "multiple values"):
            pf.col("amount").sum.over(pf.col("rowtime"), order_by=pf.col("other"))
        with self.assertRaisesRegex(TypeError, "unexpected keyword"):
            pf.col("amount").sum.over(order_by="rowtime", unknown=True)

    def test_frame_validation_through_public_over_api(self):
        with self.assertRaisesRegex(ValueError, "mutually exclusive"):
            pf.col("amount").sum.over(order_by="rowtime", rows=1, range="1 second")
        with self.assertRaisesRegex(ValueError, "larger than 0"):
            pf.col("amount").sum.over(order_by="rowtime", rows=0)
        with self.assertRaisesRegex(TypeError, "two-element"):
            pf.col("amount").sum.over(order_by="rowtime", rows=(pf.preceding(1),))
        with self.assertRaisesRegex(ValueError, "lower bound"):
            pf.col("amount").sum.over(
                order_by="rowtime", rows=(pf.following(1), pf.CURRENT_ROW)
            )
        with self.assertRaisesRegex(TypeError, "bool"):
            pf.col("amount").sum.over(order_by="rowtime", rows=True)

    def test_partition_validation_through_public_over_api(self):
        expression = pf.col("amount").sum.over(
            order_by="rowtime", partition_by=["category", pf.col("user")]
        )
        self.assertIn("category", str(expression))
        self.assertIn("user", str(expression))
        with self.assertRaisesRegex(TypeError, "must not be empty"):
            pf.col("amount").sum.over(order_by="rowtime", partition_by=[])
        with self.assertRaisesRegex(TypeError, "column name or Expression"):
            pf.col("amount").sum.over(order_by="rowtime", partition_by=[42])

    def test_flink_duration_strings_are_accepted_and_sql_interval_is_rejected(self):
        hour_word = pf.col("amount").sum.over(order_by="rowtime", range="1 hour")
        hour_abbreviation = pf.col("amount").sum.over(
            order_by="rowtime", range="1 h"
        )

        self.assertEqual(str(hour_word), str(hour_abbreviation))
        with self.assertRaisesRegex(ValueError, "duration string"):
            pf.col("amount").sum.over(
                order_by="rowtime", range="INTERVAL '1' HOUR"
            )

    def test_sub_millisecond_range_is_truncated_to_milliseconds(self):
        truncated = pf.col("amount").sum.over(
            order_by="rowtime", range=timedelta(milliseconds=1, microseconds=500)
        )
        whole_millisecond = pf.col("amount").sum.over(
            order_by="rowtime", range=timedelta(milliseconds=1)
        )

        self.assertEqual(str(truncated), str(whole_millisecond))

    def test_default_and_unbounded_rows_frames_are_constructed(self):
        default_frame = pf.col("amount").sum.over(order_by="rowtime")
        unbounded_rows = pf.col("amount").sum.over(
            order_by="rowtime", rows=pf.UNBOUNDED
        )

        self.assertIn("UNBOUNDED_RANGE", str(default_frame))
        self.assertIn("CURRENT_RANGE", str(default_frame))
        self.assertIn("UNBOUNDED_ROW", str(unbounded_rows))
        self.assertIn("CURRENT_ROW", str(unbounded_rows))

    def test_table_current_row_constant_is_rejected_with_dataframe_guidance(self):
        with self.assertRaisesRegex(
            TypeError, "Use pyflink.dataframe.CURRENT_ROW"
        ):
            pf.col("amount").sum.over(
                order_by="rowtime", rows=(pf.preceding(1), TABLE_CURRENT_ROW)
            )


class OverWindowBatchITTests(PyFlinkTestCase):
    def setUp(self):
        previous_environment = pf.get_table_environment()
        self.addCleanup(pf.set_table_environment, previous_environment)
        self.env = StreamExecutionEnvironment.get_execution_environment()
        self.env.set_runtime_mode(RuntimeExecutionMode.BATCH)
        self.t_env = StreamTableEnvironment.create(self.env)
        pf.set_table_environment(self.t_env)

    def _rowtime_dataframe(self, rows):
        # Build a bounded source whose schema carries the legacy ROWTIME indicator.
        jvm = get_gateway().jvm
        event_time_type = jvm.org.apache.flink.table.types.AtomicDataType(
            jvm.org.apache.flink.table.types.logical.TimestampType(
                False, jvm.org.apache.flink.table.types.logical.TimestampKind.ROWTIME, 3
            )
        ).bridgedTo(jvm.java.sql.Timestamp(0).getClass())
        row_type = jvm.org.apache.flink.table.api.DataTypes.ROW(
            to_jarray(
                jvm.org.apache.flink.table.api.DataTypes.Field,
                [
                    jvm.org.apache.flink.table.api.DataTypes.FIELD(
                        "amount", jvm.org.apache.flink.table.api.DataTypes.BIGINT()
                    ),
                    jvm.org.apache.flink.table.api.DataTypes.FIELD(
                        "event_time", event_time_type
                    ),
                ],
            )
        )
        elements = [
            row(
                lit(int(value)),
                lit(datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S.%f")),
            )
            for value, timestamp in (line.split(",") for line in rows)
        ]
        j_table = self.t_env._j_tenv.fromValues(
            row_type, to_expression_jarray(elements)
        )
        return pf.from_table(Table(j_table, self.t_env))

    def test_rows_frames_and_global_cumulative_result(self):
        dataframe = self._rowtime_dataframe(
            [
                "10,1970-01-01 00:00:00.000",
                "20,1970-01-01 00:00:01.000",
                "5,1970-01-01 00:00:02.000",
            ],
        )

        result = dataframe.select(
            "amount",
            rows_1=pf.col("amount").sum.over(
                order_by="event_time", rows=1
            ),
            rows_2=pf.col("amount").sum.over(
                order_by="event_time", rows=2
            ),
            global_total=pf.col("amount").sum.over(
                order_by="event_time", rows=pf.UNBOUNDED
            ),
        )

        self.assertCountEqual(
            result.collect(),
            [
                Row(10, 10, 10, 10),
                Row(20, 30, 30, 30),
                Row(5, 25, 35, 35),
            ],
        )

    def test_rows_and_range_current_row_handle_peers_differently(self):
        dataframe = self._rowtime_dataframe(
            [
                "10,1970-01-01 00:00:00.000",
                "20,1970-01-01 00:00:00.000",
                "5,1970-01-01 00:00:01.000",
            ],
        )

        result = dataframe.select(
            "amount",
            rows_sum=pf.col("amount").sum.over(
                order_by="event_time",
                rows=(pf.CURRENT_ROW, pf.CURRENT_ROW),
            ),
            range_sum=pf.col("amount").sum.over(
                order_by="event_time",
                range=(pf.CURRENT_ROW, pf.CURRENT_ROW),
            ),
        )

        self.assertCountEqual(
            result.collect(),
            [Row(10, 10, 30), Row(20, 20, 30), Row(5, 5, 5)],
        )

    def test_range_interval_uses_event_time_distance(self):
        dataframe = self._rowtime_dataframe(
            [
                "10,1970-01-01 00:00:00.000",
                "20,1970-01-01 00:00:01.000",
                "5,1970-01-01 00:00:02.000",
            ],
        )

        result = dataframe.select(
            "amount",
            range_sum=pf.col("amount").sum.over(
                order_by="event_time", range=timedelta(seconds=1)
            ),
        )

        self.assertCountEqual(
            result.collect(),
            [Row(10, 10), Row(20, 30), Row(5, 25)],
        )


class OverWindowStreamITTests(PyFlinkTestCase):
    def setUp(self):
        previous_environment = pf.get_table_environment()
        self.addCleanup(pf.set_table_environment, previous_environment)
        self.env = StreamExecutionEnvironment.get_execution_environment()
        self.t_env = StreamTableEnvironment.create(self.env)
        pf.set_table_environment(self.t_env)
        self.input_path = os.path.join(self.tempdir, "over_events.csv")
        _register_rowtime_events(
            self.t_env,
            self.input_path,
            [
                "10,1970-01-01 00:00:00.000",
                "20,1970-01-01 00:00:01.000",
            ],
        )

    def _rowtime_source(self):
        return pf.from_table(self.t_env.from_path("events"))

    def test_unbounded_rows_over_emits_streaming_results(self):
        result = self._rowtime_source().select(
            "amount",
            running_total=pf.col("amount").sum.over(
                order_by="event_time", rows=pf.UNBOUNDED
            ),
        )

        self.assertCountEqual(
            result.collect(),
            [Row(10, 10), Row(20, 30)],
        )

    def test_different_frames_in_one_projection_keep_streaming_restriction(self):
        dataframe = self._rowtime_source().select(
            first=pf.col("amount").sum.over(order_by="event_time", rows=1),
            second=pf.col("amount").sum.over(order_by="event_time", rows=2),
        )

        with self.assertRaisesRegex(
            TableException, "All aggregates must be computed on the same window"
        ):
            dataframe.collect()

    def test_following_upper_bound_keeps_streaming_restriction(self):
        dataframe = self._rowtime_source().select(
            total=pf.col("amount").sum.over(
                order_by="event_time",
                rows=(pf.preceding(1), pf.following(1)),
            )
        )

        with self.assertRaisesRegex(
            TableException, "OVER RANGE FOLLOWING windows are not supported yet"
        ):
            dataframe.collect()

    def test_unpartitioned_over_plan_uses_singleton_distribution(self):
        dataframe = self._rowtime_source().select(
            total=pf.col("amount").sum.over(
                order_by="event_time", rows=pf.UNBOUNDED
            )
        )

        self.assertRegex(
            dataframe._table.explain().lower(),
            r"exchange\(distribution=\[single\]\)",
        )


if __name__ == "__main__":
    import unittest

    unittest.main()
