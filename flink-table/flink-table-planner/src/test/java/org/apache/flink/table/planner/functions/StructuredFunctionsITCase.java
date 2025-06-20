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

import java.util.stream.Stream;

import static org.apache.flink.table.api.Expressions.objectOf;

/** Tests for functions dealing with {@link StructuredType}. */
public class StructuredFunctionsITCase extends BuiltInFunctionTestBase {

    @Override
    Stream<TestSetSpec> getTestSetSpecs() {
        return Stream.of(structuredTypeTestCases(), objectOfTestCases()).flatMap(s -> s);
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
                                        + Type1.TYPE
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
                                        + Type1.TYPE
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
                                        + Type2.TYPE
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
                                        Type1.TYPE, Type2.TYPE, NestedType.TYPE),
                                true,
                                DataTypes.BOOLEAN()));
    }

    private static Stream<TestSetSpec> objectOfTestCases() {
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
                                Row.of(42, "Bob"),
                                DataTypes.STRUCTURED(
                                        Type1.class,
                                        DataTypes.FIELD("a", DataTypes.INT()),
                                        DataTypes.FIELD("b", DataTypes.STRING())))
                        // Test with same value from function
                        .testSqlResult(
                                "Type1Constructor(f0, f1) = OBJECT_OF('"
                                        + Type1.class.getName()
                                        + "', 'a', 42, 'b', 'Bob')",
                                true,
                                DataTypes.BOOLEAN())
                        // Test with nested structured types
                        .testSqlResult(
                                "NestedConstructor(Type1Constructor(f0, f1), Type2Constructor(15, 'Alice')) = "
                                        + "OBJECT_OF('"
                                        + NestedType.class.getName()
                                        + "', 'n1', OBJECT_OF('"
                                        + Type1.class.getName()
                                        + "', 'a', 42, 'b', 'Bob'), "
                                        + "'n2', OBJECT_OF('"
                                        + Type2.class.getName()
                                        + "', 'a', 15, 'b', 'Alice'))",
                                true,
                                DataTypes.BOOLEAN())
                        // Test with TYPEOF
                        .testSqlResult(
                                "TYPEOF(OBJECT_OF('"
                                        + Type1.class.getName()
                                        + "', 'a', 42, 'b', 'Bob'))",
                                Type1.TYPE,
                                DataTypes.STRING()));
    }

    // --------------------------------------------------------------------------------------------
    // Helpers
    // --------------------------------------------------------------------------------------------

    /** Structured type Type1. */
    public static class Type1 {
        private static final String TYPE =
                "STRUCTURED<'" + Type1.class.getName() + "', `a` INT, `b` STRING>";

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
        private static final String TYPE =
                "STRUCTURED<'" + Type2.class.getName() + "', `a` INT, `b` STRING>";

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
        private static final String TYPE =
                String.format(
                        "STRUCTURED<'" + NestedType.class.getName() + "', `n1` %s, `n2` %s>",
                        Type1.TYPE,
                        Type2.TYPE);

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
}
