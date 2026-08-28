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

import unittest

from py4j.protocol import Py4JJavaError

import pyflink.dataframe as pf
from pyflink.common import Row
from pyflink.table import DataTypes, EnvironmentSettings, TableEnvironment
from pyflink.table.udf import udf
from pyflink.testing.test_case_utils import PyFlinkDataFrameUTTestCase


class SqlValidationTests(unittest.TestCase):
    def setUp(self):
        previous_environment = pf.get_table_environment()
        self.addCleanup(pf.set_table_environment, previous_environment)
        pf.set_table_environment(None)

    def test_query_must_be_a_string_checked_before_environment_creation(self):
        with self.assertRaisesRegex(TypeError, "query must be a string"):
            pf.sql(42)

        self.assertIsNone(pf.get_table_environment())


class SqlTests(PyFlinkDataFrameUTTestCase):
    def test_non_query_statements_are_rejected(self):
        self.t_env.execute_sql(
            "CREATE TABLE sink (a BIGINT) WITH ('connector' = 'blackhole')"
        )
        self.addCleanup(self.t_env.execute_sql, "DROP TABLE sink")

        for statement in [
            "INSERT INTO sink VALUES (1)",
            "CREATE TABLE t (a INT)",
            "DROP TABLE t",
            "EXPLAIN SELECT 1",
        ]:
            with self.subTest(statement=statement):
                with self.assertRaisesRegex(
                    ValueError, "only supports queries that return a result"
                ):
                    pf.sql(statement)

    def test_unparsable_statements_surface_the_java_error(self):
        for statement, error in [
            ("", "SQL parse failed"),
            ("-- only a comment", "only single statement supported"),
            ("SELECT 1; SELECT 2", "only single statement supported"),
        ]:
            with self.subTest(statement=statement):
                with self.assertRaisesRegex(Py4JJavaError, error):
                    pf.sql(statement)

    def test_auto_bind_joins_dataframes_by_variable_name(self):
        df1 = pf.from_dict({"a": [1, 2, 3], "b": ["x", "y", "z"]})  # noqa: F841
        df2 = pf.from_dict({"a": [1, 2, 3], "c": ["p", "q", "r"]})  # noqa: F841

        joined = pf.sql(
            "SELECT df1.a, b, c FROM df1 JOIN df2 ON df1.a = df2.a"
        )

        self.assertEqual(
            sorted(joined.collect(), key=lambda row: row[0]),
            [Row(1, "x", "p"), Row(2, "y", "q"), Row(3, "z", "r")],
        )

    def test_select_variants_are_accepted(self):
        df = pf.from_dict({"a": [1, 2]})  # noqa: F841

        for query in [
            "SELECT a FROM df",
            "-- leading comment\nSELECT a FROM df",
            "/* block comment */ SELECT a FROM df",
            "WITH cte AS (SELECT a FROM df) SELECT a FROM cte",
            "(SELECT a FROM df)",
        ]:
            with self.subTest(query=query):
                self.assertEqual(
                    sorted(pf.sql(query).collect(), key=lambda row: row[0]),
                    [Row(1), Row(2)],
                )

        self.assertEqual(pf.sql("VALUES (1)").collect(), [Row(1)])

    def test_explicit_bindings_with_auto_bind_disabled(self):
        src = pf.from_dict({"a": [1, 2]})  # noqa: F841
        other = pf.from_dict({"a": [10, 20]})

        result = pf.sql(
            "SELECT a FROM src WHERE a > 1", auto_bind=False, src=other
        )

        self.assertEqual(
            sorted(result.collect(), key=lambda row: row[0]),
            [Row(10), Row(20)],
        )

    def test_auto_bind_disabled_ignores_caller_variables(self):
        df = pf.from_dict({"a": [1]})  # noqa: F841

        with self.assertRaisesRegex(Py4JJavaError, "Object 'df' not found"):
            pf.sql("SELECT a FROM df", auto_bind=False)

        self.assertNotIn("df", self.t_env.list_temporary_views())

    def test_explicit_bindings_take_precedence_over_auto_bind(self):
        df = pf.from_dict({"a": [1]})  # noqa: F841
        other = pf.from_dict({"a": [42]})

        result = pf.sql("SELECT a FROM df", df=other)

        self.assertEqual(result.collect(), [Row(42)])

    def test_auto_bind_finds_module_level_globals(self):
        globals()["global_test_df"] = pf.from_dict({"a": [7]})
        self.addCleanup(globals().pop, "global_test_df", None)

        result = pf.sql("SELECT a FROM global_test_df")

        self.assertEqual(result.collect(), [Row(7)])

    def test_auto_bind_locals_take_precedence_over_globals(self):
        globals()["shadow_df"] = pf.from_dict({"a": [1]})
        self.addCleanup(globals().pop, "shadow_df", None)
        shadow_df = pf.from_dict({"a": [2]})  # noqa: F841

        self.assertEqual(pf.sql("SELECT a FROM shadow_df").collect(), [Row(2)])

    def test_auto_bind_warns_and_skips_on_collision_with_existing_view(self):
        self.t_env.create_temporary_view(
            "df", pf.from_dict({"a": [100]}).to_table()
        )
        self.addCleanup(self.t_env.drop_temporary_view, "df")
        df = pf.from_dict({"a": [1]})  # noqa: F841

        with self.assertWarnsRegex(UserWarning, "skipped 'df'"):
            result = pf.sql("SELECT a FROM df")

        # The pre-existing view wins and survives the call.
        self.assertEqual(result.collect(), [Row(100)])
        self.assertIn("df", self.t_env.list_temporary_views())

    def test_explicit_binding_collision_with_temporary_view_raises(self):
        self.t_env.create_temporary_view(
            "src", pf.from_dict({"a": [100]}).to_table()
        )
        self.addCleanup(self.t_env.drop_temporary_view, "src")

        with self.assertRaisesRegex(ValueError, "'src'.*already exists"):
            pf.sql(
                "SELECT a FROM src",
                auto_bind=False,
                src=pf.from_dict({"a": [1]}),
            )

    def test_partial_registrations_are_dropped_when_a_later_binding_fails(self):
        self.t_env.create_temporary_view(
            "taken", pf.from_dict({"a": [100]}).to_table()
        )
        self.addCleanup(self.t_env.drop_temporary_view, "taken")

        with self.assertRaisesRegex(ValueError, "'taken'.*already exists"):
            pf.sql(
                "SELECT a FROM fresh",
                auto_bind=False,
                fresh=pf.from_dict({"a": [1]}),
                taken=pf.from_dict({"a": [2]}),
            )

        # The binding registered before the failure is cleaned up.
        self.assertNotIn("fresh", self.t_env.list_temporary_views())
        self.assertIn("taken", self.t_env.list_temporary_views())

    def test_explicit_binding_shadows_permanent_table(self):
        self.t_env.execute_sql(
            "CREATE TABLE perm (a BIGINT) "
            "WITH ('connector' = 'datagen', 'number-of-rows' = '1')"
        )
        self.addCleanup(self.t_env.execute_sql, "DROP TABLE perm")

        result = pf.sql(
            "SELECT a FROM perm",
            auto_bind=False,
            perm=pf.from_dict({"a": [42]}),
        )

        self.assertEqual(result.collect(), [Row(42)])
        # The permanent table is intact after the call.
        self.assertIn("perm", self.t_env.list_tables())
        self.assertNotIn("perm", self.t_env.list_temporary_views())

    def test_auto_bind_warns_and_skips_on_collision_with_permanent_table(self):
        self.t_env.execute_sql(
            "CREATE TABLE perm (a BIGINT) WITH ("
            "'connector' = 'datagen', 'fields.a.kind' = 'sequence', "
            "'fields.a.start' = '100', 'fields.a.end' = '100')"
        )
        self.addCleanup(self.t_env.execute_sql, "DROP TABLE perm")
        perm = pf.from_dict({"a": [1]})  # noqa: F841

        with self.assertWarnsRegex(UserWarning, "skipped 'perm'"):
            result = pf.sql("SELECT a FROM perm")

        # The permanent table wins and is never shadowed.
        self.assertEqual(result.collect(), [Row(100)])
        self.assertNotIn("perm", self.t_env.list_temporary_views())

    def test_auto_bind_skips_invalid_sql_identifiers_with_warning(self):
        globals()["my df"] = pf.from_dict({"a": [1]})
        self.addCleanup(globals().pop, "my df", None)
        df = pf.from_dict({"a": [2]})  # noqa: F841

        with self.assertWarnsRegex(
            UserWarning, "skipped 'my df'.*not a valid SQL identifier"
        ):
            result = pf.sql("SELECT a FROM df")

        self.assertEqual(result.collect(), [Row(2)])

    def test_auto_bind_supports_unicode_identifiers(self):
        globals()["dfé"] = pf.from_dict({"a": [1]})
        self.addCleanup(globals().pop, "dfé", None)

        self.assertEqual(pf.sql("SELECT a FROM dfé").collect(), [Row(1)])

    def test_auto_bind_supports_keyword_names_via_quoting(self):
        order = pf.from_dict({"a": [1]})  # noqa: F841

        self.assertEqual(pf.sql("SELECT a FROM `order`").collect(), [Row(1)])

    def test_bindings_are_dropped_after_success(self):
        df = pf.from_dict({"a": [1]})  # noqa: F841

        pf.sql("SELECT a FROM df")

        self.assertNotIn("df", self.t_env.list_temporary_views())

    def test_bindings_are_dropped_after_failure(self):
        df = pf.from_dict({"a": [1]})  # noqa: F841

        with self.assertRaises(Py4JJavaError):
            pf.sql("SELECT nonexistent_column FROM df")

        self.assertNotIn("df", self.t_env.list_temporary_views())

    def test_result_composes_with_dataframe_api(self):
        df1 = pf.from_dict({"a": [1, 2, 3], "b": ["x", "y", "z"]})  # noqa: F841

        result = (
            pf.sql("SELECT a, b FROM df1")
            .filter(pf.col("a") > 1)
            .to_pandas()
        )

        self.assertEqual(sorted(result["a"].tolist()), [2, 3])

    def test_explicit_binding_of_unsupported_type_raises(self):
        with self.assertRaisesRegex(TypeError, "'x' must be a DataFrame"):
            pf.sql("SELECT * FROM x", auto_bind=False, x=42)

    def test_explicit_binding_of_raw_table_raises(self):
        table = pf.from_dict({"a": [1]}).to_table()

        with self.assertRaisesRegex(TypeError, "'x' must be a DataFrame"):
            pf.sql("SELECT * FROM x", auto_bind=False, x=table)

    def test_udfs_are_not_bindable(self):
        # UDF support will come in a separate change once the DataFrame API grows
        # UDF support in general: sql() must reject them rather than half-support them.
        add_one = udf(lambda i: i + 1, result_type=DataTypes.BIGINT())

        with self.assertRaisesRegex(TypeError, "'add_one' must be a DataFrame"):
            pf.sql("SELECT add_one(a) FROM df", auto_bind=False, add_one=add_one)

    def test_auto_bind_ignores_udfs(self):
        df = pf.from_dict({"a": [1]})  # noqa: F841
        add_one = udf(lambda i: i + 1, result_type=DataTypes.BIGINT())  # noqa: F841

        with self.assertRaisesRegex(Py4JJavaError, "No match found for function"):
            pf.sql("SELECT add_one(a) FROM df")

    def test_explicit_bindings_resolve_the_environment(self):
        other_env = TableEnvironment.create(EnvironmentSettings.in_batch_mode())
        source = pf.DataFrame(other_env.from_elements([(1,), (2,)], ["a"]))

        result = pf.sql("SELECT a FROM src", auto_bind=False, src=source)

        self.assertEqual(
            sorted(result.collect(), key=lambda row: row[0]), [Row(1), Row(2)]
        )
        # The environment is resolved per call; the global one is untouched.
        self.assertIs(pf.get_table_environment(), self.t_env)
        self.assertNotIn("src", other_env.list_temporary_views())

    def test_explicit_bindings_from_different_environments_raise(self):
        other_env = TableEnvironment.create(EnvironmentSettings.in_batch_mode())
        foreign = pf.DataFrame(other_env.from_elements([(1,)], ["a"]))
        local = pf.from_dict({"b": [2]})

        with self.assertRaisesRegex(ValueError, "different TableEnvironments"):
            pf.sql(
                "SELECT * FROM one JOIN two ON TRUE",
                auto_bind=False,
                one=foreign,
                two=local,
            )

        self.assertNotIn("one", other_env.list_temporary_views())
        self.assertNotIn("two", self.t_env.list_temporary_views())

    def test_auto_bound_dataframes_sharing_an_environment_resolve_it(self):
        other_env = TableEnvironment.create(EnvironmentSettings.in_batch_mode())
        remote_df = pf.DataFrame(other_env.from_elements([(1,)], ["a"]))  # noqa: F841

        self.assertEqual(pf.sql("SELECT a FROM remote_df").collect(), [Row(1)])
        self.assertIs(pf.get_table_environment(), self.t_env)

    def test_environment_resolved_from_bindings_does_not_become_global(self):
        pf.set_table_environment(None)
        self.addCleanup(pf.set_table_environment, self.t_env)
        other_env = TableEnvironment.create(EnvironmentSettings.in_batch_mode())
        source = pf.DataFrame(other_env.from_elements([(1,)], ["a"]))

        self.assertEqual(
            pf.sql("SELECT a FROM src", auto_bind=False, src=source).collect(),
            [Row(1)],
        )
        self.assertIsNone(pf.get_table_environment())

    def test_explicit_binding_with_invalid_sql_identifier_raises(self):
        df = pf.from_dict({"a": [1]})

        with self.assertRaisesRegex(
            ValueError, "'my df'.*not a valid SQL identifier"
        ):
            pf.sql("SELECT a FROM `my df`", auto_bind=False, **{"my df": df})

        self.assertNotIn("my df", self.t_env.list_temporary_views())

    def test_auto_bound_dataframe_from_other_environment_raises(self):
        other_env = TableEnvironment.create(EnvironmentSettings.in_batch_mode())
        foreign = pf.DataFrame(other_env.from_elements([(1,)], ["a"]))  # noqa: F841
        df = pf.from_dict({"a": [2]})  # noqa: F841

        with self.assertRaisesRegex(
            ValueError, "auto-bound DataFrames belong to different TableEnvironments"
        ):
            pf.sql("SELECT a FROM df")


if __name__ == "__main__":
    unittest.main()
