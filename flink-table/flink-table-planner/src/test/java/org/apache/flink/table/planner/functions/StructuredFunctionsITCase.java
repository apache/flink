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

package org.apache.flink.table.planner.functions;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.types.logical.StructuredType;
import org.apache.flink.types.Row;

import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;
import static org.apache.flink.table.api.Expressions.nullOf;
import static org.apache.flink.table.api.Expressions.objectOf;

/** Tests for functions dealing with {@link StructuredType}. */
public class StructuredFunctionsITCase extends BuiltInFunctionTestBase {

    @Override
    Stream<TestSetSpec> getTestSetSpecs() {
        return Stream.of(structuredTypeTestCases(), objectOfTestCases(), objectUpdateTestCases())
                .flatMap(s -> s);
    }

    private static Stream<TestSetSpec> structuredTypeTestCases() {
        return Stream.of(
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.EQUALS)
                        .onFieldsWithData(14, "Bob")
                        .andDataTypes(DataTypes.INT(), DataTypes.STRING())
                        .withFunction(Type1.Type1Constructor.class)
                        .withFunction(Type2.Type2Constructor.class)
                        .withFunction(NestedType.NestedConstructor.class)
                        // Same value from function
                        .testSqlResult(
                                "Type1Constructor(f0, f1) = Type1Constructor(14, 'Bob')",
                                true,
                                DataTypes.BOOLEAN())
                        // Same value from CAST
                        .testSqlResult(
                                "Type1Constructor(f0, f1) = CAST((14, 'Bob') AS "
                                        + Type1.TYPE_STRING
                                        + ")",
                                true,
                                DataTypes.BOOLEAN())
                        // Different value from function
                        .testSqlResult(
                                "Type1Constructor(f0, f1) = Type1Constructor(15, 'Alice')",
                                false,
                                DataTypes.BOOLEAN())
                        // Different value from CAST
                        .testSqlResult(
                                "Type1Constructor(f0, f1) = CAST((15, 'Alice') AS "
                                        + Type1.TYPE_STRING
                                        + ")",
                                false,
                                DataTypes.BOOLEAN())
                        // Different class name from function
                        .testSqlValidationError(
                                "Type1Constructor(f0, f1) = Type2Constructor(14, 'Bob')",
                                "Incompatible structured types")
                        // Different class name from CAST
                        .testSqlValidationError(
                                "Type1Constructor(f0, f1) = CAST((14, 'Bob') AS "
                                        + Type2.TYPE_STRING
                                        + ")",
                                "Incompatible structured types")
                        // Same class name but different fields
                        .testSqlValidationError(
                                "Type1Constructor(f0, f1) = CAST((14, 'Bob') AS STRUCTURED<'"
                                        + Type1.class.getName()
                                        + "', a BOOLEAN, b BOOLEAN>)",
                                "Cannot apply '=' to arguments")
                        // Test nesting
                        .testSqlResult(
                                String.format(
                                        "NestedConstructor(Type1Constructor(f0, f1), Type2Constructor(15, 'Alice')) = CAST("
                                                + "(CAST((14, 'Bob') AS %s), CAST((15, 'Alice') AS %s))"
                                                + " AS %s)",
                                        Type1.TYPE_STRING,
                                        Type2.TYPE_STRING,
                                        NestedType.TYPE_STRING),
                                true,
                                DataTypes.BOOLEAN())
                        .testSqlResult(
                                String.format(
                                        "CAST((42, ARRAY['1', '2', '3'], MAP['A', TIMESTAMP '2025-06-20 12:00:01', 'B', TIMESTAMP '2025-06-20 12:00:02']) AS %s)",
                                        NonDefaultType.TYPE_STRING),
                                new NonDefaultType(
                                        42,
                                        List.of("1", "2", "3"),
                                        Map.of(
                                                "A",
                                                Timestamp.valueOf("2025-06-20 12:00:01"),
                                                "B",
                                                Timestamp.valueOf("2025-06-20 12:00:02"))),
                                DataTypes.of(NonDefaultType.class).notNull()));
    }

    private static Stream<TestSetSpec> objectOfTestCases() {
        final Type1 type1 = Type1.of(42, "Bob");
        final Type2 type2 = Type2.of(15, "Alice");
        final NestedType nestedType = NestedType.of(type1, type2);
        return Stream.of(
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.OBJECT_OF)
                        .onFieldsWithData(42, "Bob")
                        .andDataTypes(DataTypes.INT(), DataTypes.STRING())
                        .withFunction(Type1.Type1Constructor.class)
                        .withFunction(Type2.Type2Constructor.class)
                        .withFunction(NestedType.NestedConstructor.class)
                        // Test with OBJECT_OF
                        .testResult(
                                objectOf(Type1.class, "a", 42, "b", "Bob"),
                                "OBJECT_OF('" + Type1.class.getName() + "', 'a', 42, 'b', 'Bob')",
                                type1,
                                DataTypes.STRUCTURED(
                                        Type1.class.getName(),
                                        DataTypes.FIELD("a", DataTypes.INT().notNull()),
                                        DataTypes.FIELD("b", DataTypes.CHAR(3).notNull())))
                        // Test with nested structured types
                        .testResult(
                                objectOf(
                                        NestedType.class,
                                        "n1",
                                        objectOf(Type1.class, "a", 42, "b", "Bob"),
                                        "n2",
                                        objectOf(Type2.class, "a", 15, "b", "Alice")),
                                "OBJECT_OF('"
                                        + NestedType.class.getName()
                                        + "', 'n1', OBJECT_OF('"
                                        + Type1.class.getName()
                                        + "', 'a', 42, 'b', 'Bob'), "
                                        + "'n2', OBJECT_OF('"
                                        + Type2.class.getName()
                                        + "', 'a', 15, 'b', 'Alice'))",
                                nestedType,
                                DataTypes.STRUCTURED(
                                        NestedType.class.getName(),
                                        DataTypes.FIELD(
                                                "n1",
                                                DataTypes.STRUCTURED(
                                                        Type1.class.getName(),
                                                        DataTypes.FIELD(
                                                                "a", DataTypes.INT().notNull()),
                                                        DataTypes.FIELD(
                                                                "b", DataTypes.CHAR(3).notNull()))),
                                        DataTypes.FIELD(
                                                "n2",
                                                DataTypes.STRUCTURED(
                                                        Type2.class.getName(),
                                                        DataTypes.FIELD(
                                                                "a", DataTypes.INT().notNull()),
                                                        DataTypes.FIELD(
                                                                "b",
                                                                DataTypes.CHAR(5).notNull())))))
                        // Test equal with constructor
                        // TODO: Test disabled due to FLINK-38083
                        //                        .testSqlResult(
                        //                                "Type1Constructor(f0, f1) = OBJECT_OF('"
                        //                                        + Type1.class.getName()
                        //                                        + "', 'a', 42, 'b', 'Bob')",
                        //                                true,
                        //                                DataTypes.BOOLEAN())
                        // Test OBJECT_OF when class not found
                        .testSqlResult(
                                "OBJECT_OF('not.existing.clazz', 'a', 42, 'b', 'Bob')",
                                Row.of(42, "Bob"),
                                DataTypes.STRUCTURED(
                                        "not.existing.clazz",
                                        DataTypes.FIELD("a", DataTypes.INT().notNull()),
                                        DataTypes.FIELD("b", DataTypes.CHAR(3).notNull())))
                        // Invalid Test - field name is not a string literal
                        .testSqlValidationError(
                                "OBJECT_OF('"
                                        + Type1.class.getName()
                                        + "', CAST(NULL AS STRING), 42, 'b', 'Bob')",
                                "The field key at position 2 must be a non-nullable character string literal.")
                        // Invalid Test - first argument is type string but null
                        .testSqlValidationError(
                                "OBJECT_OF(CAST(NULL AS STRING), 'a', '12', 'b', 'Alice')",
                                "The first argument must be a non-nullable character string literal representing the class name."));
    }

    private static Stream<TestSetSpec> objectUpdateTestCases() {
        return Stream.of(
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.OBJECT_UPDATE)
                        .onFieldsWithData(42, "Bob")
                        .andDataTypes(DataTypes.INT(), DataTypes.STRING())
                        .withFunction(Type1.Type1Constructor.class)
                        .withFunction(Type2.Type2Constructor.class)
                        .withFunction(NestedType.NestedConstructor.class)
                        // Test update all fields equality
                        .testResult(
                                call("Type1Constructor", $("f0"), $("f1"))
                                        .objectUpdate("a", 16, "b", "Alice"),
                                "OBJECT_UPDATE(OBJECT_OF('"
                                        + Type1.class.getName()
                                        + "', 'a', f0, 'b', f1), 'a', 16, 'b', 'Alice')",
                                Type1.of(16, "Alice"),
                                DataTypes.STRUCTURED(
                                        Type1.class.getName(),
                                        DataTypes.FIELD("a", DataTypes.INT().notNull()),
                                        DataTypes.FIELD("b", DataTypes.CHAR(5).notNull())))
                        // Test update single field
                        .testResult(
                                objectOf(Type1.class, "a", 42, "b", "Bob")
                                        .objectUpdate("b", "Alice"),
                                "OBJECT_UPDATE(OBJECT_OF('"
                                        + Type1.class.getName()
                                        + "', 'a', 42, 'b', 'Bob'), 'b', 'Alice')",
                                Type1.of(42, "Alice"),
                                DataTypes.STRUCTURED(
                                        Type1.class.getName(),
                                        DataTypes.FIELD("a", DataTypes.INT().notNull()),
                                        DataTypes.FIELD("b", DataTypes.CHAR(5).notNull())))
                        // Test nested structured types
                        .testResult(
                                objectOf(
                                                NestedType.class,
                                                "n1",
                                                call("Type1Constructor", $("f0"), $("f1")),
                                                "n2",
                                                call("Type2Constructor", 15, "Alice"))
                                        .objectUpdate(
                                                "n1",
                                                objectOf(Type1.class, "a", 16, "b", "UpdatedBob")),
                                "OBJECT_UPDATE(OBJECT_OF('"
                                        + NestedType.class.getName()
                                        + "', 'n1', Type1Constructor(f0, f1), 'n2', Type2Constructor(15, 'Alice')), "
                                        + "'n1', OBJECT_OF('"
                                        + Type1.class.getName()
                                        + "', 'a', 16, 'b', 'UpdatedBob'))",
                                NestedType.of(Type1.of(16, "UpdatedBob"), Type2.of(15, "Alice")),
                                DataTypes.STRUCTURED(
                                        NestedType.class.getName(),
                                        DataTypes.FIELD(
                                                "n1",
                                                DataTypes.STRUCTURED(
                                                        Type1.class.getName(),
                                                        DataTypes.FIELD(
                                                                "a", DataTypes.INT().notNull()),
                                                        DataTypes.FIELD(
                                                                "b",
                                                                DataTypes.CHAR(10).notNull()))),
                                        DataTypes.FIELD(
                                                "n2",
                                                DataTypes.STRUCTURED(
                                                        Type2.class.getName(),
                                                        DataTypes.FIELD("a", DataTypes.INT()),
                                                        DataTypes.FIELD("b", DataTypes.STRING())))))
                        // Test when class not found
                        .testSqlResult(
                                "OBJECT_UPDATE(OBJECT_OF('not.existing.clazz', 'a', 42, 'b', 'Bob'), 'b', 'Alice')",
                                Row.of(42, "Alice"),
                                DataTypes.STRUCTURED(
                                        "not.existing.clazz",
                                        DataTypes.FIELD("a", DataTypes.INT().notNull()),
                                        DataTypes.FIELD("b", DataTypes.CHAR(5).notNull())))
                        // Test update field to null
                        .testResult(
                                objectOf(Type1.class, "a", 42, "b", "Bob")
                                        .objectUpdate("b", nullOf(DataTypes.STRING())),
                                "OBJECT_UPDATE(OBJECT_OF('"
                                        + Type1.class.getName()
                                        + "', 'a', 42, 'b', 'Bob'), 'b', CAST(NULL AS STRING))",
                                Type1.of(42, null),
                                DataTypes.STRUCTURED(
                                        Type1.class.getName(),
                                        DataTypes.FIELD("a", DataTypes.INT().notNull()),
                                        DataTypes.FIELD("b", DataTypes.STRING())))
                        // Test first argument is null
                        .testSqlResult(
                                "OBJECT_UPDATE(CAST(NULL AS STRUCTURED<'"
                                        + Type1.class.getName()
                                        + "', a INT, b STRING>), 'a', 16, 'b', 'Alice')",
                                null,
                                DataTypes.STRUCTURED(
                                        Type1.class.getName(),
                                        DataTypes.FIELD("a", DataTypes.INT().notNull()),
                                        DataTypes.FIELD("b", DataTypes.CHAR(5).notNull())))
                        // Invalid Test - name of the field to update is not in the structured type
                        .testSqlValidationError(
                                "OBJECT_UPDATE(OBJECT_OF('"
                                        + Type1.class.getName()
                                        + "', 'a', f0, 'b', f1), 'someRandomName', 16)",
                                "The field name 'someRandomName' at position 2 is not part of the structured type attributes. Available attributes: [a, b]."));
    }

    // --------------------------------------------------------------------------------------------
    // Helpers
    // --------------------------------------------------------------------------------------------

    /** Structured type Type1. */
    public static class Type1 {
        private static final String TYPE_STRING =
                String.format("STRUCTURED<'%s', a INT, b STRING>", Type1.class.getName());

        public Integer a;
        public String b;

        public static Type1 of(final Integer a, final String b) {
            final Type1 t = new Type1();
            t.a = a;
            t.b = b;
            return t;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Type1 that = (Type1) o;
            return Objects.equals(this.a, that.a) && Objects.equals(this.b, that.b);
        }

        @Override
        public int hashCode() {
            return Objects.hash(a, b);
        }

        public static class Type1Constructor extends ScalarFunction {
            public Type1 eval(Integer a, String b) {
                final Type1 t = new Type1();
                t.a = a;
                t.b = b;
                return t;
            }
        }
    }

    /** Structured type Type2. */
    public static class Type2 {
        private static final String TYPE_STRING =
                String.format("STRUCTURED<'%s', a INT, b STRING>", Type2.class.getName());

        public Integer a;
        public String b;

        public static Type2 of(final Integer a, final String b) {
            final Type2 t = new Type2();
            t.a = a;
            t.b = b;
            return t;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Type2 that = (Type2) o;
            return Objects.equals(a, that.a) && Objects.equals(b, that.b);
        }

        @Override
        public int hashCode() {
            return Objects.hash(a, b);
        }

        public static class Type2Constructor extends ScalarFunction {
            public Type2 eval(Integer a, String b) {
                final Type2 t = new Type2();
                t.a = a;
                t.b = b;
                return t;
            }
        }
    }

    /** Structured type NestedType. */
    public static class NestedType {
        private static final String TYPE_STRING =
                String.format(
                        "STRUCTURED<'%s', n1 %s, n2 %s>",
                        NestedType.class.getName(), Type1.TYPE_STRING, Type2.TYPE_STRING);

        public Type1 n1;
        public Type2 n2;

        public static NestedType of(final Type1 n1, final Type2 n2) {
            final NestedType t = new NestedType();
            t.n1 = n1;
            t.n2 = n2;
            return t;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            NestedType that = (NestedType) o;
            return Objects.equals(n1, that.n1) && Objects.equals(n2, that.n2);
        }

        @Override
        public int hashCode() {
            return Objects.hash(n1, n2);
        }

        public static class NestedConstructor extends ScalarFunction {
            public NestedType eval(Type1 n1, Type2 n2) {
                final NestedType t = new NestedType();
                t.n1 = n1;
                t.n2 = n2;
                return t;
            }
        }
    }

    /**
     * Structured type that does not use default conversion classes but e.g. {@link List} for arrays
     * and primitive types.
     */
    public static class NonDefaultType {
        private static final String TYPE_STRING =
                String.format(
                        "STRUCTURED<'%s', i INT NOT NULL, l ARRAY<STRING>, m MAP<STRING, TIMESTAMP(9)>>",
                        NonDefaultType.class.getName());

        public final int i;
        public final List<String> l;
        public final Map<String, Timestamp> m;

        public NonDefaultType(int i, List<String> l, Map<String, Timestamp> m) {
            this.i = i;
            this.l = l;
            this.m = m;
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final NonDefaultType that = (NonDefaultType) o;
            return i == that.i && Objects.equals(l, that.l) && Objects.equals(m, that.m);
        }

        @Override
        public int hashCode() {
            return Objects.hash(i, l, m);
        }
    }
}
