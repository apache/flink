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

import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

/** Tests for functions dealing with {@link StructuredType}. */
public class StructuredFunctionsITCase extends BuiltInFunctionTestBase {

    @Override
    Stream<TestSetSpec> getTestSetSpecs() {
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

    // --------------------------------------------------------------------------------------------
    // Helpers
    // --------------------------------------------------------------------------------------------

    /** Structured type Type1. */
    public static class Type1 {
        private static final String TYPE_STRING =
                String.format("STRUCTURED<'%s', a INT, b STRING>", Type1.class.getName());

        public Integer a;
        public String b;

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
