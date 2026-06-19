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

package org.apache.flink.table.planner.plan.stream.sql;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.planner.utils.JavaStreamTableTestUtil;
import org.apache.flink.table.planner.utils.TableTestBase;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for usages of {@link DataTypes#NULL()}. */
class NullTypeTest extends TableTestBase {

    private final JavaStreamTableTestUtil util = javaStreamTestUtil();

    @Test
    void testValues() {
        assertThatThrownBy(
                        () ->
                                util.verifyExecPlan(
                                        "SELECT * FROM (VALUES (1, NULL), (2, NULL)) AS T(a, b)"))
                .hasMessageContaining("Illegal use of 'NULL'")
                .isInstanceOf(ValidationException.class);
    }

    @Test
    void testValuesWithoutTypeCoercion() {
        // should work if we enable type coercion, works already in Table API
        assertThatThrownBy(
                        () ->
                                util.verifyExecPlan(
                                        "SELECT * FROM (VALUES (1, NULL), (2, 1)) AS T(a, b)"))
                .hasMessageContaining("Illegal use of 'NULL'")
                .isInstanceOf(ValidationException.class);
    }

    @Test
    void testSetOperationWithoutTypeCoercion() {
        // we might want to support type coercion here
        assertThatThrownBy(
                        () ->
                                util.verifyExecPlan(
                                        "SELECT ARRAY[1,2] IN (ARRAY[1], ARRAY[1,2], ARRAY[NULL, NULL, NULL])"))
                .hasMessageContaining("Parameters must be of the same type")
                .isInstanceOf(ValidationException.class);
    }

    @Test
    void testBuiltInFunction() {
        assertThatThrownBy(() -> util.verifyExecPlan("SELECT ABS(NULL)"))
                .hasMessageContaining("Illegal use of 'NULL'")
                .isInstanceOf(ValidationException.class);
    }

    @Test
    void testArrayConstructor() {
        assertThatThrownBy(() -> util.verifyExecPlan("SELECT ARRAY[NULL]"))
                .hasMessageContaining("Parameters must be of the same type")
                .isInstanceOf(ValidationException.class);
    }

    @Test
    void testMapConstructor() {
        assertThatThrownBy(() -> util.verifyExecPlan("SELECT MAP[NULL, NULL]"))
                .hasMessageContaining("Parameters must be of the same type")
                .isInstanceOf(ValidationException.class);
    }

    @Test
    void testFunctionReturningNull() {
        util.addTemporarySystemFunction("NullTypeFunction", NullTypeFunction.class);
        assertThatThrownBy(() -> util.verifyExecPlan("SELECT NullTypeFunction(12)"))
                .hasMessageContaining("SQL validation failed. Invalid function call")
                .isInstanceOf(ValidationException.class);
    }

    @Test
    void testNestedNull() {
        util.addTemporarySystemFunction("NestedNullTypeFunction", NestedNullTypeFunction.class);
        assertThatThrownBy(() -> util.verifyExecPlan("SELECT NestedNullTypeFunction(12)"))
                .hasMessageContaining("SQL validation failed. Invalid function call")
                .isInstanceOf(ValidationException.class);
    }

    // --------------------------------------------------------------------------------------------

    /** Function that contains an invalid null type. */
    @SuppressWarnings("unused")
    public static class NullTypeFunction extends ScalarFunction {

        public @DataTypeHint("NULL") Object eval(Integer i) {
            return null;
        }
    }

    /** Function that contains an invalid nested null type. */
    @SuppressWarnings("unused")
    public static class NestedNullTypeFunction extends ScalarFunction {

        public @DataTypeHint("ARRAY<NULL>") Object eval(Integer i) {
            return null;
        }
    }
}
