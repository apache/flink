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

import org.junit.Test;

/** Tests for usages of {@link DataTypes#NULL()}. */
public class NullTypeTest extends TableTestBase {

    private final JavaStreamTableTestUtil util = javaStreamTestUtil();

    @Test
    public void testValues() {
        expectedException().expect(ValidationException.class);
        expectedException().expectMessage("Illegal use of 'NULL'");
        util.verifyExecPlan("SELECT * FROM (VALUES (1, NULL), (2, NULL)) AS T(a, b)");
    }

    @Test
    public void testValuesWithoutTypeCoercion() {
        // should work if we enable type coercion, works already in Table API
        expectedException().expect(ValidationException.class);
        expectedException().expectMessage("Illegal use of 'NULL'");
        util.verifyExecPlan("SELECT * FROM (VALUES (1, NULL), (2, 1)) AS T(a, b)");
    }

    @Test
    public void testSetOperationWithoutTypeCoercion() {
        // we might want to support type coercion here
        expectedException().expect(ValidationException.class);
        expectedException().expectMessage("Parameters must be of the same type");
        util.verifyExecPlan("SELECT ARRAY[1,2] IN (ARRAY[1], ARRAY[1,2], ARRAY[NULL, NULL, NULL])");
    }

    @Test
    public void testBuiltInFunction() {
        expectedException().expect(ValidationException.class);
        expectedException().expectMessage("Illegal use of 'NULL'");
        util.verifyExecPlan("SELECT ABS(NULL)");
    }

    @Test
    public void testArrayConstructor() {
        expectedException().expect(ValidationException.class);
        expectedException().expectMessage("Parameters must be of the same type");
        util.verifyExecPlan("SELECT ARRAY[NULL]");
    }

    @Test
    public void testMapConstructor() {
        expectedException().expect(ValidationException.class);
        expectedException().expectMessage("Parameters must be of the same type");
        util.verifyExecPlan("SELECT MAP[NULL, NULL]");
    }

    @Test
    public void testFunctionReturningNull() {
        expectedException().expect(ValidationException.class);
        expectedException().expectMessage("SQL validation failed. Invalid function call");
        util.addTemporarySystemFunction("NullTypeFunction", NullTypeFunction.class);
        util.verifyExecPlan("SELECT NullTypeFunction(12)");
    }

    @Test
    public void testNestedNull() {
        expectedException().expect(ValidationException.class);
        expectedException().expectMessage("SQL validation failed. Invalid function call");
        util.addTemporarySystemFunction("NestedNullTypeFunction", NestedNullTypeFunction.class);
        util.verifyExecPlan("SELECT NestedNullTypeFunction(12)");
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
