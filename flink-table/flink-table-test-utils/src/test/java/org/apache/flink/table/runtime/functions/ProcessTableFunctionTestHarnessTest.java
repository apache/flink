/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.runtime.functions;

import org.apache.flink.table.annotation.ArgumentHint;
import org.apache.flink.table.annotation.ArgumentTrait;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.StateHint;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.dataview.ListView;
import org.apache.flink.table.api.dataview.MapView;
import org.apache.flink.table.functions.ProcessTableFunction;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ProcessTableFunctionTestHarnessTest {

    @DataTypeHint("ROW<value INT>")
    public static class PassthroughPTF extends ProcessTableFunction<Row> {
        public void eval(@ArgumentHint(ArgumentTrait.ROW_SEMANTIC_TABLE) Row input) {
            collect(input);
        }
    }

    /** Passthrough PTF for testing field ordering. */
    @DataTypeHint("ROW<user STRING, value INT>")
    public static class UserValuePassthroughPTF extends ProcessTableFunction<Row> {
        public void eval(@ArgumentHint(ArgumentTrait.ROW_SEMANTIC_TABLE) Row input) {
            collect(input);
        }
    }

    /** Filter PTF for testing scalar argument handling. */
    @DataTypeHint("ROW<value INT>")
    public static class FilterPTF extends ProcessTableFunction<Row> {
        public void eval(
                @ArgumentHint(ArgumentTrait.ROW_SEMANTIC_TABLE) Row input,
                @ArgumentHint(ArgumentTrait.SCALAR) Integer threshold) {
            // Use named field access - converter enriches Row with field names
            int value = input.getFieldAs("value");
            if (value >= threshold) {
                collect(input);
            }
        }
    }

    /** PTF for testing transformation of output types. */
    @DataTypeHint("ROW<doubled INT, original INT>")
    public static class DoublePTF extends ProcessTableFunction<Row> {
        public void eval(@ArgumentHint(ArgumentTrait.ROW_SEMANTIC_TABLE) Row input) {
            int value = input.getFieldAs("value");
            collect(Row.of(value * 2, value));
        }
    }

    /** PTF with for testing table argument names set via argument hints. */
    @DataTypeHint("ROW<value INT>")
    public static class ExplicitNamePTF extends ProcessTableFunction<Row> {
        public void eval(
                @ArgumentHint(value = ArgumentTrait.ROW_SEMANTIC_TABLE, name = "customName")
                        Row actualParamName) {
            collect(actualParamName);
        }
    }

    /** PTF with inline type annotation - no builder config needed. */
    @DataTypeHint("ROW<doubled INT>")
    public static class InlineTypePTF extends ProcessTableFunction<Row> {
        public void eval(
                @ArgumentHint(
                                value = ArgumentTrait.ROW_SEMANTIC_TABLE,
                                type = @DataTypeHint("ROW<value INT>"))
                        Row input) {
            int value = input.getFieldAs("value");
            collect(Row.of(value * 2));
        }
    }

    @DataTypeHint("ROW<value INT>")
    public static class PartitionedPTF extends ProcessTableFunction<Row> {
        public void eval(@ArgumentHint(ArgumentTrait.SET_SEMANTIC_TABLE) Row input) {
            collect(Row.of((Integer) input.getFieldAs("value")));
        }
    }

    /**
     * PTF with PASS_COLUMNS_THROUGH for validating that all input columns are prepended to output.
     */
    @DataTypeHint("ROW<doubled INT>")
    public static class PassColumnsThroughPTF extends ProcessTableFunction<Row> {
        public void eval(
                @ArgumentHint({
                            ArgumentTrait.SET_SEMANTIC_TABLE,
                            ArgumentTrait.PASS_COLUMNS_THROUGH
                        })
                        Row input) {
            int value = input.getFieldAs("value");
            collect(Row.of(value * 2));
        }
    }

    /** PTF with OPTIONAL_PARTITION_BY for validating that partition setup can be omitted. */
    @DataTypeHint("ROW<doubled INT>")
    public static class OptionalPartitionPTF extends ProcessTableFunction<Row> {

        public void eval(
                @ArgumentHint({
                            ArgumentTrait.SET_SEMANTIC_TABLE,
                            ArgumentTrait.OPTIONAL_PARTITION_BY
                        })
                        Row input) {
            int value = input.getFieldAs("value");
            collect(Row.of(value * 2));
        }
    }

    /** Simple POJO for testing structured type input/output. */
    public static class User {
        public String name;
        public int age;

        public User() {}

        public User(String name, int age) {
            this.name = name;
            this.age = age;
        }

        @Override
        public String toString() {
            return "User{name='" + name + "', age=" + age + '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            User user = (User) o;
            return age == user.age && java.util.Objects.equals(name, user.name);
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(name, age);
        }
    }

    /** PTF for testing structured type inputs. */
    @DataTypeHint("ROW<name STRING, age INT>")
    public static class UserPTF extends ProcessTableFunction<Row> {
        public void eval(@ArgumentHint(ArgumentTrait.ROW_SEMANTIC_TABLE) User user) {
            if (user.age >= 18) {
                collect(Row.of(user.name, user.age));
            }
        }
    }

    /** PTF that transforms structured type inputs and outputs. */
    public static class UserTransformPTF extends ProcessTableFunction<User> {
        public void eval(@ArgumentHint(ArgumentTrait.ROW_SEMANTIC_TABLE) User user) {
            User transformed = new User(user.name, user.age + 1);
            collect(transformed);
        }
    }

    /** Invalid PTF - uses reserved argument name "on_time". */
    @DataTypeHint("ROW<value INT>")
    public static class InvalidReservedArgOnTimePTF extends ProcessTableFunction<Row> {
        public void eval(
                @ArgumentHint(value = ArgumentTrait.ROW_SEMANTIC_TABLE, name = "on_time")
                        Row input) {
            collect(input);
        }
    }

    /** Invalid PTF - uses reserved argument name "uid". */
    @DataTypeHint("ROW<value INT>")
    public static class InvalidReservedArgUidPTF extends ProcessTableFunction<Row> {
        public void eval(
                @ArgumentHint(ArgumentTrait.ROW_SEMANTIC_TABLE) Row input,
                @ArgumentHint(ArgumentTrait.SCALAR) String uid) {
            collect(input);
        }
    }

    /** Multi-table PTF for validating multi-input processing. */
    @DataTypeHint("ROW<output STRING>")
    public static class MultiTableUnionPTF extends ProcessTableFunction<Row> {
        public void eval(
                @ArgumentHint(ArgumentTrait.SET_SEMANTIC_TABLE) Row leftTable,
                @ArgumentHint(ArgumentTrait.SET_SEMANTIC_TABLE) Row rightTable) {
            if (leftTable != null) {
                collect(Row.of("LEFT: " + leftTable));
            }
            if (rightTable != null) {
                collect(Row.of("RIGHT: " + rightTable));
            }
        }
    }

    /**
     * Multi-table PTF with one POJO and one Row argument, for testing partitioning with structured
     * types.
     */
    @DataTypeHint("ROW<source STRING, age INT>")
    public static class MixedTypeMultiTablePTF extends ProcessTableFunction<Row> {
        public void eval(
                @ArgumentHint(ArgumentTrait.SET_SEMANTIC_TABLE) User userTable,
                @ArgumentHint(ArgumentTrait.SET_SEMANTIC_TABLE) Row rowTable) {
            if (userTable != null) {
                collect(Row.of("USER", userTable.age));
            }
            if (rowTable != null) {
                collect(Row.of("ROW", rowTable.getFieldAs("age")));
            }
        }
    }

    /**
     * Invalid PTF - uses PASS_COLUMNS_THROUGH with multiple table arguments (not allowed per Flink
     * docs).
     */
    @DataTypeHint("ROW<output STRING>")
    public static class InvalidPassColumnsThroughMultiTablePTF extends ProcessTableFunction<Row> {
        public void eval(
                @ArgumentHint({
                            ArgumentTrait.SET_SEMANTIC_TABLE,
                            ArgumentTrait.PASS_COLUMNS_THROUGH
                        })
                        Row leftTable,
                @ArgumentHint(ArgumentTrait.SET_SEMANTIC_TABLE) Row rightTable) {
            if (leftTable != null) {
                collect(Row.of("LEFT: " + leftTable));
            }
            if (rightTable != null) {
                collect(Row.of("RIGHT: " + rightTable));
            }
        }
    }

    /** PTF with only scalar arguments, no tables. */
    @DataTypeHint("ROW<sum INT>")
    public static class ScalarOnlyPTF extends ProcessTableFunction<Row> {
        public void eval(
                @ArgumentHint(ArgumentTrait.SCALAR) Integer a,
                @ArgumentHint(ArgumentTrait.SCALAR) Integer b) {
            collect(Row.of(a + b));
        }
    }

    /** PTF with Context parameter - should be rejected by test harness. */
    @DataTypeHint("ROW<value INT>")
    public static class PTFWithContext extends ProcessTableFunction<Row> {
        public void eval(Context ctx, @ArgumentHint(ArgumentTrait.ROW_SEMANTIC_TABLE) Row input) {
            collect(input);
        }
    }

    /** PTF with simple structured type state - counts rows per partition. */
    @DataTypeHint("ROW<count BIGINT>")
    public static class PTFWithPojoState extends ProcessTableFunction<Row> {
        public static class CounterState {
            public long counter = 0L;
        }

        public void eval(
                @StateHint CounterState state,
                @ArgumentHint(ArgumentTrait.SET_SEMANTIC_TABLE) Row input) {
            state.counter++;
            collect(Row.of(state.counter));
        }
    }

    /** PTF with ListView state - accumulates values in a list. */
    @DataTypeHint("ROW<values ARRAY<INT>>")
    public static class PTFWithListViewState extends ProcessTableFunction<Row> {
        public void eval(
                @StateHint(type = @DataTypeHint("ARRAY<INT>")) ListView<Integer> listState,
                @ArgumentHint(ArgumentTrait.SET_SEMANTIC_TABLE) Row input)
                throws Exception {
            Integer value = input.getFieldAs("value");
            listState.add(value);

            // Collect all values as an array
            java.util.List<Integer> values = new java.util.ArrayList<>();
            for (Integer v : listState.get()) {
                values.add(v);
            }
            collect(Row.of((Object) values.toArray(new Integer[0])));
        }
    }

    /** PTF with MapView state - counts occurrences of each key. */
    @DataTypeHint("ROW<key STRING, count INT>")
    public static class PTFWithMapViewState extends ProcessTableFunction<Row> {
        public void eval(
                @StateHint MapView<String, Integer> mapState,
                @ArgumentHint(ArgumentTrait.SET_SEMANTIC_TABLE) Row input)
                throws Exception {
            String key = input.getFieldAs("key");
            Integer count = mapState.get(key);
            if (count == null) {
                mapState.put(key, 1);
            } else {
                mapState.put(key, count + 1);
            }
            collect(Row.of(key, mapState.get(key)));
        }
    }

    /** PTF with both value state and ListView state. */
    @DataTypeHint("ROW<count BIGINT, sum INT>")
    public static class PTFWithMultipleStates extends ProcessTableFunction<Row> {
        public static class CounterState {
            public long count = 0L;
        }

        public void eval(
                @StateHint CounterState counter,
                @StateHint(type = @DataTypeHint("ARRAY<INT>")) ListView<Integer> history,
                @ArgumentHint(ArgumentTrait.SET_SEMANTIC_TABLE) Row input)
                throws Exception {
            Integer value = input.getFieldAs("value");
            counter.count++;
            history.add(value);

            int sum = 0;
            for (Integer v : history.get()) {
                sum += v;
            }
            collect(Row.of(counter.count, sum));
        }
    }

    // -------------------------------------------------------------------------
    // Builder Configuration Tests
    // -------------------------------------------------------------------------

    @Test
    void testBuilderRejectsDuplicateScalarArguments() {
        Exception exception =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> {
                            ProcessTableFunctionTestHarness.ofClass(FilterPTF.class)
                                    .withTableArgument("input", DataTypes.of("ROW<value INT>"))
                                    .withScalarArgument("threshold", 50)
                                    .withScalarArgument("threshold", 100);
                        });

        assertThat(exception.getMessage()).contains("threshold");
    }

    @Test
    void testBuilderRejectsDuplicateTableArguments() {
        Exception exception =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> {
                            ProcessTableFunctionTestHarness.ofClass(MultiTableUnionPTF.class)
                                    .withTableArgument(
                                            "leftTable", DataTypes.of("ROW<id INT, name STRING>"))
                                    .withTableArgument(
                                            "leftTable", DataTypes.of("ROW<id INT, value INT>"));
                        });

        assertThat(exception.getMessage()).contains("leftTable");
    }

    @Test
    void testBuilderRejectsMixedDuplicateArguments() {
        Exception exception =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> {
                            ProcessTableFunctionTestHarness.ofClass(FilterPTF.class)
                                    .withTableArgument("input", DataTypes.of("ROW<value INT>"))
                                    .withScalarArgument("input", 42);
                        });

        assertThat(exception.getMessage()).contains("input");
    }

    @Test
    void testBuilderRejectsReservedArgumentOnTime() {
        // We should reject PTFs that use reserved argument name "on_time"
        ProcessTableFunctionTestHarness.Builder harnessBuilder =
                ProcessTableFunctionTestHarness.ofClass(InvalidReservedArgOnTimePTF.class)
                        .withTableArgument("on_time", DataTypes.of("ROW<id INT>"));

        ValidationException exception =
                assertThrows(
                        ValidationException.class,
                        () -> {
                            harnessBuilder.build();
                        });

        assertThat(exception.getMessage())
                .contains("Function signature must not declare system arguments")
                .contains("on_time");
    }

    @Test
    void testBuilderRejectsReservedArgumentUid() {
        // We should reject PTFs that use reserved argument name "uid"
        ProcessTableFunctionTestHarness.Builder harnessBuilder =
                ProcessTableFunctionTestHarness.ofClass(InvalidReservedArgUidPTF.class)
                        .withTableArgument("input", DataTypes.of("ROW<id INT>"))
                        .withScalarArgument("uid", "my-id");

        ValidationException exception =
                assertThrows(
                        ValidationException.class,
                        () -> {
                            harnessBuilder.build();
                        });

        assertThat(exception.getMessage())
                .contains("Function signature must not declare system arguments")
                .contains("uid");
    }

    // -------------------------------------------------------------------------
    // Argument Configuration Tests
    // -------------------------------------------------------------------------

    @Test
    void testExplicitNameTakesPrecedence() throws Exception {
        // Verify that @ArgumentHint(name="customName") takes precedence over actual parameter
        // name when processing elements and calling eval.

        try (ProcessTableFunctionTestHarness<Row> harness =
                ProcessTableFunctionTestHarness.ofClass(ExplicitNamePTF.class)
                        .withTableArgument("customName", DataTypes.of("ROW<value INT>"))
                        .build()) {

            harness.processElement(Row.of(42));
            harness.processElement(Row.of(100));

            List<Row> output = harness.getOutput();
            assertThat(output).hasSize(2);
            assertThat(output.get(0).getField("value")).isEqualTo(42);
            assertThat(output.get(1).getField("value")).isEqualTo(100);
        }
    }

    @Test
    void testScalarOnlyPTF() throws Exception {
        // Test scalar-only PTF with no table arguments
        try (ProcessTableFunctionTestHarness<Row> harness =
                ProcessTableFunctionTestHarness.ofClass(ScalarOnlyPTF.class)
                        .withScalarArgument("a", 10)
                        .withScalarArgument("b", 20)
                        .build()) {

            harness.process();

            List<Row> output = harness.getOutput();

            assertThat(output).hasSize(1);
            assertThat(output.get(0).getField("sum")).isEqualTo(30);
        }
    }

    @Test
    void testScalarOnlyPTFWithWrongArgumentTypes() {
        Exception exception =
                assertThrows(
                        IllegalStateException.class,
                        () -> {
                            ProcessTableFunctionTestHarness.ofClass(ScalarOnlyPTF.class)
                                    .withScalarArgument("a", "not_an_integer")
                                    .withScalarArgument("b", 20)
                                    .build();
                        });

        assertThat(exception.getMessage()).contains("Type mismatch");
        assertThat(exception.getMessage()).contains("java.lang.Integer");
        assertThat(exception.getMessage()).contains("java.lang.String");
    }

    @Test
    void testInvokeRejectsTableArguments() throws Exception {
        // Verify that invoke() rejects PTFs with table arguments
        try (ProcessTableFunctionTestHarness<Row> harness =
                ProcessTableFunctionTestHarness.ofClass(FilterPTF.class)
                        .withTableArgument("input", DataTypes.of("ROW<value INT>"))
                        .withScalarArgument("threshold", 50)
                        .build()) {

            Exception exception =
                    assertThrows(
                            IllegalStateException.class,
                            () -> {
                                harness.process();
                            });

            assertThat(exception.getMessage()).contains("process() is only for scalar-only PTFs");
        }
    }

    @Test
    void testTableProcessingWithScalarArgument() throws Exception {
        // Test a PTF that uses a scalar parameter
        try (ProcessTableFunctionTestHarness<Row> harness =
                ProcessTableFunctionTestHarness.ofClass(FilterPTF.class)
                        .withTableArgument("input", DataTypes.of("ROW<value INT>"))
                        .withScalarArgument("threshold", 50) // Scalar argument: threshold = 50
                        .build()) {

            harness.processElement(Row.of(25));
            harness.processElement(Row.of(75));
            harness.processElement(Row.of(50));
            harness.processElement(Row.of(10));
            harness.processElement(Row.of(100));

            List<Row> output = harness.getOutput();

            assertThat(output).hasSize(3);
            assertThat(output.get(0).getField("value")).isEqualTo(75);
            assertThat(output.get(1).getField("value")).isEqualTo(50);
            assertThat(output.get(2).getField("value")).isEqualTo(100);
        }
    }

    @Test
    void testTableProcessingWithScalarArgumentWrongType() {
        Exception exception =
                assertThrows(
                        IllegalStateException.class,
                        () -> {
                            ProcessTableFunctionTestHarness.ofClass(FilterPTF.class)
                                    .withTableArgument("input", DataTypes.of("ROW<value INT>"))
                                    .withScalarArgument("threshold", "not_an_integer")
                                    .build();
                        });

        assertThat(exception.getMessage()).contains("Type mismatch");
        assertThat(exception.getMessage()).contains("java.lang.Integer");
        assertThat(exception.getMessage()).contains("java.lang.String");
    }

    // -------------------------------------------------------------------------
    // Argument Trait Tests
    // -------------------------------------------------------------------------

    @Test
    void testProcessElementWithRowKind() throws Exception {
        // Verify RowKind is preserved through processing (ROW_SEMANTIC_TABLE)
        try (ProcessTableFunctionTestHarness<Row> harness =
                ProcessTableFunctionTestHarness.ofClass(PassthroughPTF.class)
                        .withTableArgument("input", DataTypes.of("ROW<value INT>"))
                        .build()) {

            harness.processElement(RowKind.INSERT, 10);
            harness.processElement(RowKind.UPDATE_BEFORE, 15);
            harness.processElement(RowKind.UPDATE_AFTER, 20);
            harness.processElement(RowKind.DELETE, 30);

            List<Row> output = harness.getOutput();
            assertThat(output).hasSize(4);
            assertThat(output.get(0).getKind()).isEqualTo(RowKind.INSERT);
            assertThat(output.get(0).getField("value")).isEqualTo(10);
            assertThat(output.get(1).getKind()).isEqualTo(RowKind.UPDATE_BEFORE);
            assertThat(output.get(1).getField("value")).isEqualTo(15);
            assertThat(output.get(2).getKind()).isEqualTo(RowKind.UPDATE_AFTER);
            assertThat(output.get(2).getField("value")).isEqualTo(20);
            assertThat(output.get(3).getKind()).isEqualTo(RowKind.DELETE);
            assertThat(output.get(3).getField("value")).isEqualTo(30);
        }
    }

    @Test
    void testPassColumnsThroughTrait() throws Exception {
        // Verify PASS_COLUMNS_THROUGH prepends ALL input columns (not just partition keys)
        try (ProcessTableFunctionTestHarness<Row> harness =
                ProcessTableFunctionTestHarness.ofClass(PassColumnsThroughPTF.class)
                        .withTableArgument("input", DataTypes.of("ROW<key STRING, value INT>"))
                        .withPartitionBy("input", "key")
                        .build()) {

            harness.processElement(Row.of("A", 10));
            harness.processElement(Row.of("B", 20));

            List<Row> output = harness.getOutput();
            assertThat(output).hasSize(2);

            assertThat(output.get(0)).isEqualTo(Row.of("A", 10, 20));
            assertThat(output.get(1)).isEqualTo(Row.of("B", 20, 40));
        }
    }

    @Test
    void testOptionalPartitionByWithoutPartition() throws Exception {
        // Verify OPTIONAL_PARTITION_BY allows omitting partition configuration
        try (ProcessTableFunctionTestHarness<Row> harness =
                ProcessTableFunctionTestHarness.ofClass(OptionalPartitionPTF.class)
                        .withTableArgument("input", DataTypes.of("ROW<key STRING, value INT>"))
                        .build()) {

            harness.processElement(Row.of("A", 10));
            harness.processElement(Row.of("B", 20));
            harness.processElement(Row.of("C", 30));

            List<Row> output = harness.getOutput();
            assertThat(output).hasSize(3);

            assertThat(output.get(0)).isEqualTo(Row.of(20));
            assertThat(output.get(1)).isEqualTo(Row.of(40));
            assertThat(output.get(2)).isEqualTo(Row.of(60));
        }
    }

    @Test
    void testOptionalPartitionByWithPartition() throws Exception {
        // Verify OPTIONAL_PARTITION_BY still works when partition is configured
        try (ProcessTableFunctionTestHarness<Row> harness =
                ProcessTableFunctionTestHarness.ofClass(OptionalPartitionPTF.class)
                        .withTableArgument("input", DataTypes.of("ROW<key STRING, value INT>"))
                        .withPartitionBy("input", "key")
                        .build()) {

            harness.processElement(Row.of("A", 10));
            harness.processElement(Row.of("A", 5));
            harness.processElement(Row.of("B", 20));

            List<Row> output = harness.getOutput();
            assertThat(output).hasSize(3);

            assertThat(output.get(0)).isEqualTo(Row.of("A", 20));
            assertThat(output.get(1)).isEqualTo(Row.of("A", 10));
            assertThat(output.get(2)).isEqualTo(Row.of("B", 40));
        }
    }

    // -------------------------------------------------------------------------
    // Data Type Conversion Tests
    // -------------------------------------------------------------------------

    @Test
    void testNamedRowFieldOrdering() throws Exception {
        // Test what happens when Row field order differs from DataType schema order
        try (ProcessTableFunctionTestHarness<Row> harness =
                ProcessTableFunctionTestHarness.ofClass(UserValuePassthroughPTF.class)
                        .withTableArgument("input", DataTypes.of("ROW<user STRING, value INT>"))
                        .build()) {

            Row rowA = Row.withNames();
            rowA.setField("value", 100);
            rowA.setField("user", "Alice");

            harness.processElement(rowA);

            List<Row> output = harness.getOutput();
            assertThat(output).hasSize(1);

            Row result = output.get(0);

            // Named field access
            assertThat(result.getField("user")).isEqualTo("Alice");
            assertThat(result.getField("value")).isEqualTo(100);
        }
    }

    @Test
    void testPositionalRowWithWrongTypeOrder() throws Exception {
        // Verify that type mismatches are caught when Row values don't match schema types
        try (ProcessTableFunctionTestHarness<Row> harness =
                ProcessTableFunctionTestHarness.ofClass(UserValuePassthroughPTF.class)
                        .withTableArgument("input", DataTypes.of("ROW<user STRING, value INT>"))
                        .build()) {

            Row wrongOrderRow = Row.of(10, "Alice");

            Exception exception =
                    assertThrows(
                            ClassCastException.class, () -> harness.processElement(wrongOrderRow));

            assertThat(exception.getMessage()).contains("Integer");
            assertThat(exception.getMessage()).contains("String");
            assertThat(exception.getMessage()).contains("cannot be cast");
        }
    }

    @Test
    void testStructuredTypeInput() throws Exception {
        // Test that a PTF can declare an input type as a structured type,
        // and that the harness can handle the conversion from Row into
        // that type.
        try (ProcessTableFunctionTestHarness<Row> harness =
                ProcessTableFunctionTestHarness.ofClass(UserPTF.class)
                        .withTableArgument("user")
                        .build()) {

            harness.processElement(Row.of("Alice", 25));
            harness.processElement(Row.of("Bob", 17));

            List<Row> output = harness.getOutput();
            assertThat(output).hasSize(1);

            Row result = output.get(0);
            assertThat(result.getField("name")).isEqualTo("Alice");
            assertThat(result.getField("age")).isEqualTo(25);
        }
    }

    @Test
    void testStructuredTypeInputAndOutput() throws Exception {
        // Test PTF with structured type inputs and outputs
        try (ProcessTableFunctionTestHarness<User> harness =
                ProcessTableFunctionTestHarness.ofClass(UserTransformPTF.class)
                        .withTableArgument("user")
                        .build()) {

            harness.processElement(Row.of("Alice", 25));

            List<User> output = harness.getOutput();
            assertThat(output).hasSize(1);

            User result = output.get(0);
            assertThat(result.getClass()).isEqualTo(User.class);
            assertThat(result.name).isEqualTo("Alice");
            assertThat(result.age).isEqualTo(26);
        }
    }

    @Test
    void testInlineTypeAnnotation() throws Exception {
        // Verify that PTFs can declare table argument types via @ArgumentHint(type = ...)
        // without needing .withTableArgument() configuration
        try (ProcessTableFunctionTestHarness<Row> harness =
                ProcessTableFunctionTestHarness.ofClass(InlineTypePTF.class).build()) {

            harness.processElement(Row.of(5));
            harness.processElement(Row.of(10));

            List<Row> output = harness.getOutput();
            assertThat(output).hasSize(2);
            assertThat(output.get(0)).isEqualTo(Row.of(10));
            assertThat(output.get(1)).isEqualTo(Row.of(20));
        }
    }

    @Test
    void testInlineTypeMatchesBuilderConfig() throws Exception {
        // Verify that when both inline annotation and builder config are provided with matching
        // types, the harness builds successfully
        try (ProcessTableFunctionTestHarness<Row> harness =
                ProcessTableFunctionTestHarness.ofClass(InlineTypePTF.class)
                        .withTableArgument("input", DataTypes.of("ROW<value INT>"))
                        .build()) {

            harness.processElement(Row.of(7));

            List<Row> output = harness.getOutput();
            assertThat(output).hasSize(1);
            assertThat(output.get(0)).isEqualTo(Row.of(14));
        }
    }

    // -------------------------------------------------------------------------
    // Partitioning Tests
    // -------------------------------------------------------------------------

    @Test
    void testSetSemanticWithPartitionByName() throws Exception {
        // Verify set-semantic table with partition configuration by column name
        try (ProcessTableFunctionTestHarness<Row> harness =
                ProcessTableFunctionTestHarness.ofClass(PartitionedPTF.class)
                        .withTableArgument("input", DataTypes.of("ROW<key STRING, value INT>"))
                        .withPartitionBy("input", "key") // Partition by "key" column name
                        .build()) {

            harness.processElement(Row.of("X", 10));
            harness.processElement(Row.of("Y", 20));
            harness.processElement(Row.of("X", 30));

            List<Row> output = harness.getOutput();
            assertThat(output).hasSize(3);
            assertThat(output.get(0)).isEqualTo(Row.of("X", 10));
            assertThat(output.get(1)).isEqualTo(Row.of("Y", 20));
            assertThat(output.get(2)).isEqualTo(Row.of("X", 30));
        }
    }

    @Test
    void testSetSemanticWithMultiplePartitionColumns() throws Exception {
        // Verify composite partition key (multiple columns)
        try (ProcessTableFunctionTestHarness<Row> harness =
                ProcessTableFunctionTestHarness.ofClass(PartitionedPTF.class)
                        .withTableArgument(
                                "input",
                                DataTypes.of("ROW<region STRING, country STRING, value INT>"))
                        .withPartitionBy("input", "region", "country")
                        .build()) {

            harness.processElement(Row.of("EU", "DE", 100));
            harness.processElement(Row.of("EU", "DE", 200));
            harness.processElement(Row.of("EU", "FR", 300));
            harness.processElement(Row.of("US", "NY", 400));

            List<Row> output = harness.getOutput();
            assertThat(output).hasSize(4);
            assertThat(output.get(0)).isEqualTo(Row.of("EU", "DE", 100));
            assertThat(output.get(1)).isEqualTo(Row.of("EU", "DE", 200));
            assertThat(output.get(2)).isEqualTo(Row.of("EU", "FR", 300));
            assertThat(output.get(3)).isEqualTo(Row.of("US", "NY", 400));
        }
    }

    @Test
    void testSetSemanticWithSelectivePartitioning() throws Exception {
        // Verify that only partition columns are automatically included in output
        try (ProcessTableFunctionTestHarness<Row> harness =
                ProcessTableFunctionTestHarness.ofClass(PartitionedPTF.class)
                        .withTableArgument(
                                "input",
                                DataTypes.of(
                                        "ROW<id INT, region STRING, country STRING, city STRING, value INT>"))
                        .withPartitionBy("input", "region")
                        .build()) {

            harness.processElement(Row.of(1, "EU", "DE", "Berlin", 100));
            harness.processElement(Row.of(4, "US", "CA", "LA", 200));

            List<Row> output = harness.getOutput();

            assertThat(output.get(0)).isEqualTo(Row.of("EU", 100));
            assertThat(output.get(1)).isEqualTo(Row.of("US", 200));
        }
    }

    @Test
    void testMultipleSetSemanticTablesWithMatchingPartitionKeys() throws Exception {
        try (ProcessTableFunctionTestHarness<Row> harness =
                ProcessTableFunctionTestHarness.ofClass(MultiTableUnionPTF.class)
                        .withTableArgument("leftTable", DataTypes.of("ROW<name STRING, score INT>"))
                        .withPartitionBy("leftTable", "name")
                        .withTableArgument(
                                "rightTable", DataTypes.of("ROW<name STRING, city STRING>"))
                        .withPartitionBy("rightTable", "name")
                        .build()) {

            harness.processElementForTable("leftTable", Row.of("Alice", 100));
            harness.processElementForTable("leftTable", Row.of("Bob", 200));

            harness.processElementForTable("rightTable", Row.of("Alice", "Berlin"));
            harness.processElementForTable("rightTable", Row.of("Bob", "London"));

            List<Row> output = harness.getOutput();
            assertThat(output).hasSize(4);

            assertThat(output.get(0).getField("name")).isEqualTo("Alice");
            assertThat(output.get(0).getField("name0")).isEqualTo("Alice");
            assertThat(output.get(0).getField("output")).isEqualTo("LEFT: +I[Alice, 100]");

            assertThat(output.get(1).getField("name")).isEqualTo("Bob");
            assertThat(output.get(1).getField("name0")).isEqualTo("Bob");
            assertThat(output.get(1).getField("output")).isEqualTo("LEFT: +I[Bob, 200]");

            assertThat(output.get(2).getField("name")).isEqualTo("Alice");
            assertThat(output.get(2).getField("name0")).isEqualTo("Alice");
            assertThat(output.get(2).getField("output")).isEqualTo("RIGHT: +I[Alice, Berlin]");

            assertThat(output.get(3).getField("name")).isEqualTo("Bob");
            assertThat(output.get(3).getField("name0")).isEqualTo("Bob");
            assertThat(output.get(3).getField("output")).isEqualTo("RIGHT: +I[Bob, London]");
        }
    }

    @Test
    void testMultipleSetSemanticTablesWithStructuredTypePartitioning() throws Exception {
        // Verify that multi-table PTFs work when one argument is a structured type
        // and another is a Row, both partitioned by the same field type
        try (ProcessTableFunctionTestHarness<Row> harness =
                ProcessTableFunctionTestHarness.ofClass(MixedTypeMultiTablePTF.class)
                        .withTableArgument("userTable")
                        .withPartitionBy("userTable", "age")
                        .withTableArgument(
                                "rowTable", DataTypes.of("ROW<name STRING, age INT NOT NULL>"))
                        .withPartitionBy("rowTable", "age")
                        .build()) {

            harness.processElementForTable("userTable", Row.of("Alice", 25));
            harness.processElementForTable("rowTable", Row.of("Bob", 30));

            List<Row> output = harness.getOutput();
            assertThat(output)
                    .containsExactlyInAnyOrder(
                            Row.of(25, 25, "USER", 25), Row.of(30, 30, "ROW", 30));
        }
    }

    @Test
    void testMultipleSetSemanticTablesWithMismatchedPartitionTypes() {
        // Verify that multi-table PTFs with inconsistent partition types are rejected
        Exception exception =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> {
                            ProcessTableFunctionTestHarness.ofClass(MultiTableUnionPTF.class)
                                    .withTableArgument(
                                            "leftTable", DataTypes.of("ROW<id INT, name STRING>"))
                                    .withPartitionBy("leftTable", "id")
                                    .withTableArgument(
                                            "rightTable",
                                            DataTypes.of("ROW<key STRING, city STRING>"))
                                    .withPartitionBy("rightTable", "key")
                                    .build();
                        });

        assertThat(exception.getMessage()).contains("Inconsistent partitioning");
    }

    @Test
    void testMultipleSetSemanticTablesWithMismatchedPartitionColumnCount() {
        // Verify that multi-table PTFs with different partition column counts are rejected
        Exception exception =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> {
                            ProcessTableFunctionTestHarness.ofClass(MultiTableUnionPTF.class)
                                    .withTableArgument(
                                            "leftTable",
                                            DataTypes.of("ROW<id INT, region STRING, name STRING>"))
                                    .withPartitionBy("leftTable", "id", "region")
                                    .withTableArgument(
                                            "rightTable", DataTypes.of("ROW<id INT, city STRING>"))
                                    .withPartitionBy("rightTable", "id")
                                    .build();
                        });

        assertThat(exception.getMessage()).contains("Inconsistent partitioning");
    }

    @Test
    void testPassColumnsThroughWithMultipleTablesRejected() {
        // Verify that PASS_COLUMNS_THROUGH is rejected when used with multiple table arguments
        Exception exception =
                assertThrows(
                        org.apache.flink.table.api.ValidationException.class,
                        () -> {
                            ProcessTableFunctionTestHarness.ofClass(
                                            InvalidPassColumnsThroughMultiTablePTF.class)
                                    .withTableArgument("leftTable", DataTypes.of("ROW<a INT>"))
                                    .withTableArgument("rightTable", DataTypes.of("ROW<b INT>"))
                                    .build();
                        });

        assertThat(exception.getMessage())
                .contains("Pass-through columns")
                .contains("multiple table arguments");
    }

    // -------------------------------------------------------------------------
    // Element Processing Tests
    // -------------------------------------------------------------------------

    @Test
    void testProcessElementOnMultiTableThrows() throws Exception {
        try (ProcessTableFunctionTestHarness<Row> harness =
                ProcessTableFunctionTestHarness.ofClass(MultiTableUnionPTF.class)
                        .withTableArgument("leftTable", DataTypes.of("ROW<id INT, name STRING>"))
                        .withTableArgument("rightTable", DataTypes.of("ROW<id INT, value STRING>"))
                        .withPartitionBy("leftTable", "id")
                        .withPartitionBy("rightTable", "id")
                        .build()) {

            Exception exception =
                    assertThrows(
                            IllegalStateException.class,
                            () -> harness.processElement(Row.of(1, "Alice")));
            assertThat(exception.getMessage())
                    .contains("multiple table arguments")
                    .contains("processElementForTable");
        }
    }

    // -------------------------------------------------------------------------
    // Output Collection Tests
    // -------------------------------------------------------------------------

    @Test
    void testClearOutput() throws Exception {
        try (ProcessTableFunctionTestHarness<Row> harness =
                ProcessTableFunctionTestHarness.ofClass(PassthroughPTF.class)
                        .withTableArgument("input", DataTypes.of("ROW<value INT>"))
                        .build()) {

            harness.processElement(Row.of(10));
            harness.processElement(Row.of(20));
            assertThat(harness.getOutput()).hasSize(2);

            harness.clearOutput();
            assertThat(harness.getOutput()).isEmpty();

            harness.processElement(Row.of(30));
            assertThat(harness.getOutput()).hasSize(1);
        }
    }

    // -------------------------------------------------------------------------
    // Error Cases Tests
    // -------------------------------------------------------------------------

    @Test
    void testProcessElementForTableWithInvalidName() throws Exception {
        try (ProcessTableFunctionTestHarness<Row> harness =
                ProcessTableFunctionTestHarness.ofClass(PassthroughPTF.class)
                        .withTableArgument("input", DataTypes.of("ROW<value INT>"))
                        .build()) {

            Exception exception =
                    assertThrows(
                            IllegalArgumentException.class,
                            () -> harness.processElementForTable("nonexistent", Row.of(42)));
            assertThat(exception.getMessage()).contains("nonexistent");
        }
    }

    @Test
    void testContextParameterRejected() {
        Exception exception =
                assertThrows(
                        IllegalStateException.class,
                        () ->
                                ProcessTableFunctionTestHarness.ofClass(PTFWithContext.class)
                                        .withTableArgument("input", DataTypes.of("ROW<value INT>"))
                                        .build());

        assertThat(exception.getMessage())
                .contains("does not yet support Context parameters")
                .contains("Context parameter")
                .contains("position 0");
    }

    @Test
    void testSetSemanticMissingPartitionConfigThrows() {
        Exception exception =
                assertThrows(
                        IllegalStateException.class,
                        () -> {
                            ProcessTableFunctionTestHarness.ofClass(PartitionedPTF.class)
                                    .withTableArgument(
                                            "input", DataTypes.of("ROW<key STRING, value INT>"))
                                    .build();
                        });

        assertThat(exception.getMessage()).contains("No partition configuration found");
        assertThat(exception.getMessage()).contains("withPartitionBy");
    }

    @Test
    void testPartitionByInvalidColumnName() {
        Exception exception =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> {
                            ProcessTableFunctionTestHarness.ofClass(PartitionedPTF.class)
                                    .withTableArgument(
                                            "input", DataTypes.of("ROW<key STRING, value INT>"))
                                    .withPartitionBy("input", "nonexistent")
                                    .build();
                        });

        assertThat(exception.getMessage()).contains("not found");
        assertThat(exception.getMessage()).contains("Available columns");
    }

    @Test
    void testPartitionByDuplicateConfigThrows() {
        Exception exception =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> {
                            ProcessTableFunctionTestHarness.ofClass(PartitionedPTF.class)
                                    .withTableArgument(
                                            "input", DataTypes.of("ROW<key STRING, value INT>"))
                                    .withPartitionBy("input", "key") // First config
                                    .withPartitionBy("input", "key"); // Duplicate - should fail
                        });

        assertThat(exception.getMessage()).contains("Partition config already exists");
    }

    // -------------------------------------------------------------------------
    // State Tests
    // -------------------------------------------------------------------------

    @Test
    void testPojoState() throws Exception {
        ProcessTableFunctionTestHarness<Row> harness =
                ProcessTableFunctionTestHarness.ofClass(PTFWithPojoState.class)
                        .withTableArgument("input", DataTypes.of("ROW<name STRING, value INT>"))
                        .withPartitionBy("input", "name")
                        .build();

        harness.processElementForTable("input", Row.of("Alice", 10));
        assertThat(harness.getOutput()).containsExactly(Row.of("Alice", 1L));

        PTFWithPojoState.CounterState state = harness.getStateForKey("state", Row.of("Alice"));
        assertThat(state.counter).isEqualTo(1L);

        harness.processElementForTable("input", Row.of("Alice", 15));
        assertThat(harness.getOutput().get(1)).isEqualTo(Row.of("Alice", 2L));

        state = harness.getStateForKey("state", Row.of("Alice"));
        assertThat(state.counter).isEqualTo(2L);

        harness.close();
    }

    @Test
    void testPojoStatePartitionIsolation() throws Exception {
        ProcessTableFunctionTestHarness<Row> harness =
                ProcessTableFunctionTestHarness.ofClass(PTFWithPojoState.class)
                        .withTableArgument("input", DataTypes.of("ROW<name STRING, value INT>"))
                        .withPartitionBy("input", "name")
                        .build();

        harness.processElementForTable("input", Row.of("Alice", 10));
        harness.processElementForTable("input", Row.of("Bob", 20));
        harness.processElementForTable("input", Row.of("Alice", 15));

        PTFWithPojoState.CounterState aliceState = harness.getStateForKey("state", Row.of("Alice"));
        PTFWithPojoState.CounterState bobState = harness.getStateForKey("state", Row.of("Bob"));

        assertThat(aliceState.counter).isEqualTo(2L);
        assertThat(bobState.counter).isEqualTo(1L);

        harness.close();
    }

    @Test
    void testPojoStateWithInitialState() throws Exception {
        PTFWithPojoState.CounterState initialState = new PTFWithPojoState.CounterState();
        initialState.counter = 100L;

        ProcessTableFunctionTestHarness<Row> harness =
                ProcessTableFunctionTestHarness.ofClass(PTFWithPojoState.class)
                        .withTableArgument("input", DataTypes.of("ROW<id INT>"))
                        .withPartitionBy("input", "id")
                        .withInitialStateArgument("state", Row.of(1), initialState)
                        .build();

        PTFWithPojoState.CounterState state = harness.getStateForKey("state", Row.of(1));
        assertThat(state.counter).isEqualTo(100L);

        harness.processElement(Row.of(1));
        assertThat(harness.getOutput()).containsExactly(Row.of(1, 101L));

        harness.processElement(Row.of(2));
        assertThat(harness.getOutput().get(1)).isEqualTo(Row.of(2, 1L));

        harness.close();
    }

    @Test
    void testGetStateKeys() throws Exception {
        ProcessTableFunctionTestHarness<Row> harness =
                ProcessTableFunctionTestHarness.ofClass(PTFWithPojoState.class)
                        .withTableArgument("input", DataTypes.of("ROW<name STRING, value INT>"))
                        .withPartitionBy("input", "name")
                        .build();

        harness.processElementForTable("input", Row.of("Alice", 10));
        harness.processElementForTable("input", Row.of("Bob", 20));
        harness.processElementForTable("input", Row.of("Charlie", 30));

        java.util.Set<Row> keys = harness.getStateKeys("state");
        assertThat(keys)
                .containsExactlyInAnyOrder(Row.of("Alice"), Row.of("Bob"), Row.of("Charlie"));

        harness.close();
    }

    @Test
    void testGetAllState() throws Exception {
        ProcessTableFunctionTestHarness<Row> harness =
                ProcessTableFunctionTestHarness.ofClass(PTFWithPojoState.class)
                        .withTableArgument("input", DataTypes.of("ROW<name STRING, value INT>"))
                        .withPartitionBy("input", "name")
                        .build();

        harness.processElementForTable("input", Row.of("Alice", 10));
        harness.processElementForTable("input", Row.of("Alice", 15));
        harness.processElementForTable("input", Row.of("Bob", 20));

        java.util.Map<Row, PTFWithPojoState.CounterState> allState = harness.getAllState("state");

        assertThat(allState).hasSize(2);
        assertThat(allState.get(Row.of("Alice")).counter).isEqualTo(2L);
        assertThat(allState.get(Row.of("Bob")).counter).isEqualTo(1L);

        harness.close();
    }

    @Test
    void testListViewState() throws Exception {
        ProcessTableFunctionTestHarness<Row> harness =
                ProcessTableFunctionTestHarness.ofClass(PTFWithListViewState.class)
                        .withTableArgument("input", DataTypes.of("ROW<key STRING, value INT>"))
                        .withPartitionBy("input", "key")
                        .build();

        harness.processElementForTable("input", Row.of("A", 1));
        assertThat(harness.getOutput()).containsExactly(Row.of("A", new Integer[] {1}));

        harness.processElementForTable("input", Row.of("A", 2));
        assertThat(harness.getOutput().get(1)).isEqualTo(Row.of("A", new Integer[] {1, 2}));

        org.apache.flink.table.api.dataview.ListView<Integer> listState =
                harness.getStateForKey("listState", Row.of("A"));
        assertThat(listState.get()).containsExactly(1, 2);

        harness.close();
    }

    @Test
    void testMapViewState() throws Exception {
        ProcessTableFunctionTestHarness<Row> harness =
                ProcessTableFunctionTestHarness.ofClass(PTFWithMapViewState.class)
                        .withTableArgument(
                                "input", DataTypes.of("ROW<partition STRING, key STRING>"))
                        .withPartitionBy("input", "partition")
                        .build();

        harness.processElementForTable("input", Row.of("P1", "foo"));
        assertThat(harness.getOutput()).containsExactly(Row.of("P1", "foo", 1));

        harness.processElementForTable("input", Row.of("P1", "foo"));
        assertThat(harness.getOutput().get(1)).isEqualTo(Row.of("P1", "foo", 2));

        harness.processElementForTable("input", Row.of("P1", "bar"));

        org.apache.flink.table.api.dataview.MapView<String, Integer> mapState =
                harness.getStateForKey("mapState", Row.of("P1"));
        assertThat(mapState.get("foo")).isEqualTo(2);
        assertThat(mapState.get("bar")).isEqualTo(1);

        harness.close();
    }

    @Test
    void testEmptyState() throws Exception {
        ProcessTableFunctionTestHarness<Row> harness =
                ProcessTableFunctionTestHarness.ofClass(PTFWithPojoState.class)
                        .withTableArgument("input", DataTypes.of("ROW<name STRING, value INT>"))
                        .withPartitionBy("input", "name")
                        .build();

        PTFWithPojoState.CounterState state = harness.getStateForKey("state", Row.of("Alice"));

        assertThat(state).isNull();

        harness.close();
    }

    @Test
    void testClearStateForPartition() throws Exception {
        ProcessTableFunctionTestHarness<Row> harness =
                ProcessTableFunctionTestHarness.ofClass(PTFWithPojoState.class)
                        .withTableArgument("input", DataTypes.of("ROW<name STRING, value INT>"))
                        .withPartitionBy("input", "name")
                        .build();

        harness.processElementForTable("input", Row.of("Alice", 10));
        harness.processElementForTable("input", Row.of("Alice", 15));

        PTFWithPojoState.CounterState state = harness.getStateForKey("state", Row.of("Alice"));
        assertThat(state.counter).isEqualTo(2L);

        harness.clearStateForPartition(Row.of("Alice"));

        state = harness.getStateForKey("state", Row.of("Alice"));
        assertThat(state).isNull();

        harness.close();
    }

    @Test
    void testClearStateEntry() throws Exception {
        ProcessTableFunctionTestHarness<Row> harness =
                ProcessTableFunctionTestHarness.ofClass(PTFWithPojoState.class)
                        .withTableArgument("input", DataTypes.of("ROW<name STRING, value INT>"))
                        .withPartitionBy("input", "name")
                        .build();

        harness.processElementForTable("input", Row.of("Alice", 10));
        harness.processElementForTable("input", Row.of("Alice", 15));

        PTFWithPojoState.CounterState state = harness.getStateForKey("state", Row.of("Alice"));
        assertThat(state.counter).isEqualTo(2L);

        harness.clearStateEntry(Row.of("Alice"), "state");

        state = harness.getStateForKey("state", Row.of("Alice"));
        assertThat(state.counter).isEqualTo(0L);

        harness.close();
    }

    @Test
    void testMultipleStateParameters() throws Exception {
        ProcessTableFunctionTestHarness<Row> harness =
                ProcessTableFunctionTestHarness.ofClass(PTFWithMultipleStates.class)
                        .withTableArgument("input", DataTypes.of("ROW<key STRING, value INT>"))
                        .withPartitionBy("input", "key")
                        .build();

        harness.processElementForTable("input", Row.of("A", 10));
        harness.processElementForTable("input", Row.of("A", 20));
        harness.processElementForTable("input", Row.of("B", 5));

        assertThat(harness.getOutput())
                .containsExactly(Row.of("A", 1L, 10), Row.of("A", 2L, 30), Row.of("B", 1L, 5));

        PTFWithMultipleStates.CounterState counterA =
                harness.getStateForKey("counter", Row.of("A"));
        assertThat(counterA.count).isEqualTo(2L);

        ListView<Integer> historyA = harness.getStateForKey("history", Row.of("A"));
        assertThat(historyA.get()).containsExactly(10, 20);

        harness.close();
    }

    @Test
    void testInitialStateWithListView() throws Exception {
        ListView<Integer> initialList = new ListView<>();
        initialList.add(100);
        initialList.add(200);

        ProcessTableFunctionTestHarness<Row> harness =
                ProcessTableFunctionTestHarness.ofClass(PTFWithListViewState.class)
                        .withTableArgument("input", DataTypes.of("ROW<key STRING, value INT>"))
                        .withPartitionBy("input", "key")
                        .withInitialStateArgument("listState", Row.of("A"), initialList)
                        .build();

        ListView<Integer> listState = harness.getStateForKey("listState", Row.of("A"));
        assertThat(listState.get()).containsExactly(100, 200);

        harness.processElementForTable("input", Row.of("A", 3));
        assertThat(harness.getOutput()).containsExactly(Row.of("A", new Integer[] {100, 200, 3}));

        harness.close();
    }

    @Test
    void testInitialStateWithMapView() throws Exception {
        MapView<String, Integer> initialMap = new MapView<>();
        initialMap.put("existing", 42);

        ProcessTableFunctionTestHarness<Row> harness =
                ProcessTableFunctionTestHarness.ofClass(PTFWithMapViewState.class)
                        .withTableArgument(
                                "input", DataTypes.of("ROW<partition STRING, key STRING>"))
                        .withPartitionBy("input", "partition")
                        .withInitialStateArgument("mapState", Row.of("P1"), initialMap)
                        .build();

        MapView<String, Integer> mapState = harness.getStateForKey("mapState", Row.of("P1"));
        assertThat(mapState.get("existing")).isEqualTo(42);

        harness.processElementForTable("input", Row.of("P1", "existing"));
        assertThat(harness.getOutput()).containsExactly(Row.of("P1", "existing", 43));

        harness.close();
    }

    @Test
    void testInvalidStateNameInWithInitialState() {
        Exception exception =
                assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                ProcessTableFunctionTestHarness.ofClass(PTFWithPojoState.class)
                                        .withTableArgument("input", DataTypes.of("ROW<id INT>"))
                                        .withPartitionBy("input", "id")
                                        .withInitialStateArgument(
                                                "nonExistentState", Row.of(1), "value")
                                        .build());

        assertThat(exception.getMessage()).contains("Unknown state");
        assertThat(exception.getMessage()).contains("nonExistentState");
        assertThat(exception.getMessage()).contains("Available states");
        assertThat(exception.getMessage()).contains("state");
    }
}
