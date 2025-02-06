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

package org.apache.flink.table.planner.plan.nodes.exec.stream;

import org.apache.flink.table.annotation.ArgumentHint;
import org.apache.flink.table.annotation.ArgumentTrait;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.functions.ProcessTableFunction;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.TableSemantics;
import org.apache.flink.types.Row;

import static org.apache.flink.table.annotation.ArgumentTrait.OPTIONAL_PARTITION_BY;
import static org.apache.flink.table.annotation.ArgumentTrait.PASS_COLUMNS_THROUGH;
import static org.apache.flink.table.annotation.ArgumentTrait.SUPPORT_UPDATES;
import static org.apache.flink.table.annotation.ArgumentTrait.TABLE_AS_ROW;
import static org.apache.flink.table.annotation.ArgumentTrait.TABLE_AS_SET;

/** Testing functions for {@link ProcessTableFunction}. */
public class ProcessTableFunctionTestUtils {

    /** Testing function. */
    public static class AtomicTypeWrappingFunction extends ProcessTableFunction<Integer> {
        public void eval(@ArgumentHint(TABLE_AS_SET) Row r) {
            collect(r.getFieldAs(1));
        }
    }

    @DataTypeHint("ROW<`out` STRING, `count` INT>")
    public abstract static class TestProcessTableFunctionBase extends ProcessTableFunction<Row> {
        // Every output is emitted two times to test reuse behavior
        public void collectObjects(Object... objects) {
            final String asString = Row.of(objects).toString();
            final String objectsAsString = asString.substring(3, asString.length() - 1);
            collect(Row.of("{" + objectsAsString + "}", 1));
            collect(Row.of("{" + objectsAsString + "}", 2));
        }
    }

    /** Testing function. */
    public static class ScalarArgsFunction extends TestProcessTableFunctionBase {
        public void eval(Integer i, Boolean b) {
            collectObjects(i, b);
        }
    }

    /** Testing function. */
    public static class TableAsRowFunction extends TestProcessTableFunctionBase {
        public void eval(@ArgumentHint(ArgumentTrait.TABLE_AS_ROW) Row r, Integer i) {
            collectObjects(r, i);
        }
    }

    /** Testing function. */
    public static class TypedTableAsRowFunction extends TestProcessTableFunctionBase {
        public void eval(@ArgumentHint(ArgumentTrait.TABLE_AS_ROW) User u, Integer i) {
            collectObjects(u, i);
        }
    }

    /** Testing function. */
    public static class TableAsSetFunction extends TestProcessTableFunctionBase {
        public void eval(@ArgumentHint(TABLE_AS_SET) Row r, Integer i) {
            collectObjects(r, i);
        }
    }

    /** Testing function. */
    public static class TypedTableAsSetFunction extends TestProcessTableFunctionBase {
        public void eval(@ArgumentHint(TABLE_AS_SET) User u, Integer i) {
            collectObjects(u, i);
        }
    }

    /** Testing function. */
    public static class PojoArgsFunction extends TestProcessTableFunctionBase {
        public void eval(@ArgumentHint(TABLE_AS_ROW) User input, User scalar) {
            collectObjects(input, scalar);
        }
    }

    /** Testing function. */
    public static class EmptyArgFunction extends TestProcessTableFunctionBase {
        public void eval() {
            collectObjects("empty");
        }
    }

    /** Testing function. */
    public static class TableAsRowPassThroughFunction extends TestProcessTableFunctionBase {
        public void eval(@ArgumentHint({TABLE_AS_ROW, PASS_COLUMNS_THROUGH}) Row r, Integer i) {
            collectObjects(r, i);
        }
    }

    /** Testing function. */
    public static class TableAsSetPassThroughFunction extends TestProcessTableFunctionBase {
        public void eval(@ArgumentHint({TABLE_AS_SET, PASS_COLUMNS_THROUGH}) Row r, Integer i) {
            collectObjects(r, i);
        }
    }

    /** Testing function. */
    public static class TableAsSetUpdatingArgFunction extends TestProcessTableFunctionBase {
        public void eval(
                @ArgumentHint({TABLE_AS_SET, OPTIONAL_PARTITION_BY, SUPPORT_UPDATES}) Row r) {
            collectObjects(r);
        }
    }

    /** Testing function. */
    public static class TableAsSetOptionalPartitionFunction extends TestProcessTableFunctionBase {
        public void eval(@ArgumentHint({TABLE_AS_SET, OPTIONAL_PARTITION_BY}) Row r, Integer i) {
            collectObjects(r, i);
        }
    }

    /** Testing function. */
    public static class ContextFunction extends TestProcessTableFunctionBase {
        public void eval(Context ctx, @ArgumentHint(TABLE_AS_SET) Row r, String s) {
            final TableSemantics semantics = ctx.tableSemanticsFor("r");
            collectObjects(
                    r,
                    s,
                    semantics.partitionByColumns(),
                    semantics.changelogMode().orElse(null),
                    semantics.dataType());
        }
    }

    // --------------------------------------------------------------------------------------------
    // Helpers
    // --------------------------------------------------------------------------------------------

    /** POJO for typed tables. */
    public static class User {
        public String s;
        public Integer i;

        public User(String s, Integer i) {
            this.s = s;
            this.i = i;
        }

        @Override
        public String toString() {
            return String.format("User(s='%s', i=%s)", s, i);
        }
    }

    public static class PojoCreatingFunction extends ScalarFunction {
        public User eval(String s, Integer i) {
            return new User(s, i);
        }
    }
}
