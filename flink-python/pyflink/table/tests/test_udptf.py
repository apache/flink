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

from pyflink.common import Duration, Row
from pyflink.table import DataTypes, EnvironmentSettings, TableEnvironment
from pyflink.table.udf import (
    ProcessTableFunction,
    ProcessTableFunctionArgument,
    ProcessTableFunctionArgumentTrait as Trait,
    ProcessTableFunctionState,
    table_arg,
    udptf,
    value_state,
)
from pyflink.testing.test_case_utils import PyFlinkTestCase


class Tokenize(ProcessTableFunction):

    def eval(self, ctx, event, separator):
        for token in event.text.split(separator):
            yield Row(token)


class CountWithTimeout(ProcessTableFunction):

    def eval(self, ctx, memory, event):
        memory["count"] = (memory.count or 0) + 1
        yield Row(memory.count)

    def on_timer(self, ctx, memory):
        yield Row(memory.count)


class UdptfTests(PyFlinkTestCase):

    @staticmethod
    def _result_type():
        return DataTypes.ROW([DataTypes.FIELD("result", DataTypes.STRING())])

    @staticmethod
    def _memory_state():
        return ProcessTableFunctionState.value(
            "memory",
            DataTypes.ROW([DataTypes.FIELD("count", DataTypes.BIGINT())]),
            ttl=Duration.of_days(1),
        )

    def test_creates_stateless_function_with_ordered_arguments(self):
        function = udptf(
            Tokenize(),
            arguments=[
                ProcessTableFunctionArgument.table("event"),
                ProcessTableFunctionArgument.scalar("separator", DataTypes.STRING()),
            ],
            result_type=self._result_type(),
        )

        java_function = function._java_user_defined_function()
        arguments = java_function.getTypeInference(None).getStaticArguments().get()

        self.assertEqual("PythonProcessTableFunction", java_function.getClass().getSimpleName())
        self.assertEqual(["event", "separator"], [argument.getName() for argument in arguments])
        self.assertFalse(java_function.hasOnTimer())

        table_env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())
        table_env.create_temporary_system_function("tokenize", function)
        self.assertIn("tokenize", table_env.list_user_defined_functions())

    def test_decorator_with_argument_mapping(self):
        @udptf(
            arguments={
                "event": table_arg(),
                "separator": DataTypes.STRING(),
            },
            result_type=DataTypes.ROW([
                DataTypes.FIELD("token", DataTypes.STRING()),
            ]),
            deterministic=False,
        )
        def tokenize(ctx, event, separator):
            for token in event.text.split(separator):
                yield Row(token=token)

        java_function = tokenize._java_user_defined_function()
        arguments = java_function.getTypeInference(None).getStaticArguments().get()

        self.assertEqual(["event", "separator"], [arg.getName() for arg in arguments])
        self.assertEqual("tokenize", str(java_function))
        self.assertFalse(java_function.isDeterministic())
        self.assertFalse(java_function.hasOnTimer())

        delegate = tokenize._create_delegate_function()
        self.assertEqual(
            [Row(token="hello"), Row(token="flink")],
            list(delegate.eval(None, Row(text="hello flink"), " ")),
        )

    def test_decorator_with_state_mapping_and_timer_callback(self):
        @udptf(
            arguments={
                "event": table_arg(traits={Trait.SET_SEMANTIC_TABLE}),
            },
            states={
                "memory": value_state(
                    "ROW<count BIGINT>", ttl=Duration.of_days(1)
                ),
            },
            result_type="ROW<count BIGINT>",
        )
        def count_with_timeout(ctx, memory, event):
            memory["count"] = (memory.count or 0) + 1
            yield Row(memory.count)

        @count_with_timeout.on_timer
        def count_with_timeout_timer(ctx, memory):
            yield Row(memory.count)

        java_function = count_with_timeout._java_user_defined_function()

        self.assertTrue(java_function.hasOnTimer())
        self.assertEqual(
            ["memory"],
            list(java_function.getTypeInference(None).getStateTypeStrategies().keySet()),
        )
        self.assertEqual(86_400_000, java_function.getStateTimeToLive()[0].toMillis())

        delegate = count_with_timeout._create_delegate_function()
        self.assertEqual([Row(3)], list(delegate.on_timer(None, Row(count=3))))

    def test_decorator_validates_callback_signatures(self):
        with self.assertRaisesRegex(ValueError, r"Invalid eval\(\) signature"):
            @udptf(
                arguments={
                    "event": table_arg(),
                    "separator": DataTypes.STRING(),
                },
                result_type="ROW<token STRING>",
            )
            def invalid_eval(ctx, separator, event):
                yield Row(separator)

        @udptf(
            arguments={
                "event": table_arg(traits={Trait.SET_SEMANTIC_TABLE}),
            },
            states={"memory": value_state("ROW<count BIGINT>")},
            result_type="ROW<count BIGINT>",
        )
        def invalid_timer(ctx, memory, event):
            yield Row(memory.count)

        with self.assertRaisesRegex(ValueError, r"Invalid on_timer\(\) signature"):
            @invalid_timer.on_timer
            def invalid_timer_callback(memory, ctx):
                yield Row(memory.count)

    def test_creates_stateful_timer_function(self):
        function = udptf(
            CountWithTimeout(),
            arguments=[
                ProcessTableFunctionArgument.table(
                    "event", traits={Trait.SET_SEMANTIC_TABLE}),
            ],
            states=[self._memory_state()],
            result_type=DataTypes.ROW([DataTypes.FIELD("count", DataTypes.BIGINT())]),
        )

        java_function = function._java_user_defined_function()

        self.assertTrue(java_function.hasOnTimer())
        self.assertEqual(
            ["memory"],
            list(java_function.getTypeInference(None).getStateTypeStrategies().keySet()),
        )
        self.assertEqual(
            86_400_000,
            java_function.getStateTimeToLive()[0].toMillis(),
        )

    def test_rejects_invalid_callback_signatures(self):
        class InvalidEval(ProcessTableFunction):
            def eval(self, ctx, separator, event):
                yield Row(separator)

        class InvalidTimer(ProcessTableFunction):
            def eval(self, ctx, memory, event):
                yield Row(memory.count)

            def on_timer(self, memory, ctx):
                yield Row(memory.count)

        with self.assertRaisesRegex(ValueError, r"Invalid eval\(\) signature"):
            udptf(
                InvalidEval(),
                arguments=[
                    ProcessTableFunctionArgument.table("event"),
                    ProcessTableFunctionArgument.scalar("separator", DataTypes.STRING()),
                ],
                result_type=self._result_type(),
            )

        with self.assertRaisesRegex(ValueError, r"Invalid on_timer\(\) signature"):
            udptf(
                InvalidTimer(),
                arguments=[
                    ProcessTableFunctionArgument.table(
                        "event", traits={Trait.SET_SEMANTIC_TABLE}),
                ],
                states=[self._memory_state()],
                result_type=self._result_type(),
            )

    def test_requires_exactly_one_table_argument(self):
        with self.assertRaisesRegex(ValueError, "exactly one table argument"):
            udptf(
                Tokenize(),
                arguments=[
                    ProcessTableFunctionArgument.scalar("event", DataTypes.STRING()),
                    ProcessTableFunctionArgument.scalar("separator", DataTypes.STRING()),
                ],
                result_type=self._result_type(),
            )

        with self.assertRaisesRegex(ValueError, "exactly one table argument"):
            udptf(
                Tokenize(),
                arguments=[
                    ProcessTableFunctionArgument.table("event"),
                    ProcessTableFunctionArgument.table("separator"),
                ],
                result_type=self._result_type(),
            )

    def test_state_and_timer_require_set_semantics(self):
        with self.assertRaisesRegex(ValueError, "State requires"):
            udptf(
                CountWithTimeout(),
                arguments=[ProcessTableFunctionArgument.table("event")],
                states=[self._memory_state()],
                result_type=self._result_type(),
            )

        class StatelessTimer(ProcessTableFunction):
            def eval(self, ctx, event):
                yield Row(event)

            def on_timer(self, ctx):
                yield Row("timer")

        with self.assertRaisesRegex(ValueError, "Timers require"):
            udptf(
                StatelessTimer(),
                arguments=[ProcessTableFunctionArgument.table("event")],
                result_type=self._result_type(),
            )

    def test_rejects_pass_through_with_timer(self):
        with self.assertRaisesRegex(ValueError, "pass-through"):
            udptf(
                CountWithTimeout(),
                arguments=[
                    ProcessTableFunctionArgument.table(
                        "event",
                        traits={
                            Trait.SET_SEMANTIC_TABLE,
                            Trait.PASS_COLUMNS_THROUGH,
                        },
                    ),
                ],
                states=[self._memory_state()],
                result_type=self._result_type(),
            )

    def test_rejects_update_traits_in_first_version(self):
        for update_trait in (
                Trait.SUPPORT_UPDATES,
                Trait.REQUIRE_UPDATE_BEFORE,
                Trait.REQUIRE_FULL_DELETE):
            with self.subTest(update_trait=update_trait):
                with self.assertRaisesRegex(ValueError, "do not support updating inputs"):
                    udptf(
                        Tokenize(),
                        arguments=[
                            ProcessTableFunctionArgument.table(
                                "event",
                                traits={Trait.SET_SEMANTIC_TABLE, update_trait},
                            ),
                            ProcessTableFunctionArgument.scalar(
                                "separator", DataTypes.STRING()),
                        ],
                        result_type=self._result_type(),
                    )

    def test_validates_argument_state_and_result_types(self):
        with self.assertRaisesRegex(ValueError, "both row and set semantics"):
            ProcessTableFunctionArgument.table(
                "event",
                traits={Trait.ROW_SEMANTIC_TABLE, Trait.SET_SEMANTIC_TABLE},
            )
        with self.assertRaisesRegex(TypeError, "state must use a ROW"):
            ProcessTableFunctionState.value("memory", DataTypes.BIGINT())
        with self.assertRaisesRegex(TypeError, "result_type must be a ROW"):
            udptf(
                Tokenize(),
                arguments=[
                    ProcessTableFunctionArgument.table("event"),
                    ProcessTableFunctionArgument.scalar("separator", DataTypes.STRING()),
                ],
                result_type=DataTypes.STRING(),
            )


if __name__ == '__main__':
    import unittest
    unittest.main()
