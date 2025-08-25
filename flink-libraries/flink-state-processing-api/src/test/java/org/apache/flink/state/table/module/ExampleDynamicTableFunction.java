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

package org.apache.flink.state.table.module;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.functions.BuiltInFunctionDefinition;
import org.apache.flink.table.functions.SpecializedFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.types.inference.TypeStrategies;
import org.apache.flink.types.Row;

import static org.apache.flink.table.functions.FunctionKind.TABLE;

@Internal
@FunctionHint(output = @DataTypeHint("ROW<my-column STRING NOT NULL>"))
public class ExampleDynamicTableFunction extends TableFunction<Row> {

    public static final BuiltInFunctionDefinition FUNCTION_DEFINITION =
            BuiltInFunctionDefinition.newBuilder()
                    .name("example_dynamic_table_function")
                    .kind(TABLE)
                    .runtimeClass(ExampleDynamicTableFunction.class.getName())
                    .outputTypeStrategy(
                            TypeStrategies.explicit(
                                    DataTypes.ROW(
                                            DataTypes.FIELD(
                                                    "my-column", DataTypes.STRING().notNull()))))
                    .build();

    public ExampleDynamicTableFunction(SpecializedFunction.SpecializedContext context) {}

    public void eval() {
        Row row = Row.withNames();
        row.setField("my-column", "my-value");
        collect(row);
    }
}
