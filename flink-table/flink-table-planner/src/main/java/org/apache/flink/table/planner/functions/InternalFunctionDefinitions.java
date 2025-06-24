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
import org.apache.flink.table.functions.BuiltInFunctionDefinition;
import org.apache.flink.table.types.inference.InputTypeStrategies;

import static org.apache.flink.table.functions.FunctionKind.SCALAR;
import static org.apache.flink.table.types.inference.InputTypeStrategies.explicit;
import static org.apache.flink.table.types.inference.InputTypeStrategies.sequence;
import static org.apache.flink.table.types.inference.TypeStrategies.argument;

/** Dictionary of function definitions for all internal used functions. */
public class InternalFunctionDefinitions {

    public static final BuiltInFunctionDefinition THROW_EXCEPTION =
            new BuiltInFunctionDefinition.Builder()
                    .name("throwException")
                    .kind(SCALAR)
                    .inputTypeStrategy(
                            sequence(
                                    explicit(DataTypes.STRING()), InputTypeStrategies.TYPE_LITERAL))
                    .outputTypeStrategy(argument(1))
                    .build();
}
