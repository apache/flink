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

package org.apache.flink.table.runtime.operators.python.process;

import org.apache.flink.table.annotation.ArgumentHint;
import org.apache.flink.table.annotation.ArgumentTrait;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.functions.ProcessTableFunction;
import org.apache.flink.types.Row;

/** Java process table functions used by the PyFlink API tests. */
public final class TestJavaProcessTableFunctions {

    private TestJavaProcessTableFunctions() {}

    /** A PTF with one row-semantic table argument. */
    @DataTypeHint("ROW<`out` STRING>")
    public static class RowSemanticFunction extends ProcessTableFunction<Row> {
        public void eval(
                @ArgumentHint(ArgumentTrait.ROW_SEMANTIC_TABLE) Row input, Integer increment) {
            collect(Row.of(String.format("%s:%s", input.getFieldAs(0), increment)));
        }
    }

    /** A PTF with two set-semantic table arguments. */
    @DataTypeHint("ROW<`out` STRING>")
    public static class MultiInputFunction extends ProcessTableFunction<Row> {
        public void eval(
                Context context,
                @ArgumentHint(ArgumentTrait.SET_SEMANTIC_TABLE) Row in1,
                @ArgumentHint({
                            ArgumentTrait.SET_SEMANTIC_TABLE,
                            ArgumentTrait.OPTIONAL_PARTITION_BY
                        })
                        Row in2) {
            collect(Row.of(String.format("%s:%s", in1.getFieldAs(0), in2.getFieldAs(0))));
        }
    }
}
