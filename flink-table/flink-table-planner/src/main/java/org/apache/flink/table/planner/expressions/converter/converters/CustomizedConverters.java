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

package org.apache.flink.table.planner.expressions.converter.converters;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.planner.expressions.converter.CustomizedConvertRule;
import org.apache.flink.table.planner.functions.InternalFunctionDefinitions;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/** Registry of customized converters used by {@link CustomizedConvertRule}. */
@Internal
public class CustomizedConverters {
    private static final Map<FunctionDefinition, CustomizedConverter> CONVERTERS = new HashMap<>();

    static {
        CONVERTERS.put(BuiltInFunctionDefinitions.CAST, new CastConverter());
        CONVERTERS.put(BuiltInFunctionDefinitions.REINTERPRET_CAST, new ReinterpretCastConverter());
        CONVERTERS.put(BuiltInFunctionDefinitions.IN, new InConverter());
        CONVERTERS.put(BuiltInFunctionDefinitions.GET, new GetConverter());
        CONVERTERS.put(BuiltInFunctionDefinitions.TRIM, new TrimConverter());
        CONVERTERS.put(BuiltInFunctionDefinitions.AS, new AsConverter());
        CONVERTERS.put(BuiltInFunctionDefinitions.BETWEEN, new BetweenConverter());
        CONVERTERS.put(BuiltInFunctionDefinitions.NOT_BETWEEN, new NotBetweenConverter());
        CONVERTERS.put(BuiltInFunctionDefinitions.REPLACE, new ReplaceConverter());
        CONVERTERS.put(BuiltInFunctionDefinitions.PLUS, new PlusConverter());
        CONVERTERS.put(
                BuiltInFunctionDefinitions.TEMPORAL_OVERLAPS, new TemporalOverlapsConverter());
        CONVERTERS.put(BuiltInFunctionDefinitions.TIMESTAMP_DIFF, new TimestampDiffConverter());
        CONVERTERS.put(BuiltInFunctionDefinitions.ARRAY, new ArrayConverter());
        CONVERTERS.put(BuiltInFunctionDefinitions.MAP, new MapConverter());
        CONVERTERS.put(BuiltInFunctionDefinitions.ROW, new RowConverter());
        CONVERTERS.put(BuiltInFunctionDefinitions.ORDER_ASC, new OrderAscConverter());
        CONVERTERS.put(BuiltInFunctionDefinitions.SQRT, new SqrtConverter());
        CONVERTERS.put(BuiltInFunctionDefinitions.JSON_EXISTS, new JsonExistsConverter());
        CONVERTERS.put(BuiltInFunctionDefinitions.JSON_VALUE, new JsonValueConverter());
        CONVERTERS.put(InternalFunctionDefinitions.THROW_EXCEPTION, new ThrowExceptionConverter());
    }

    public Optional<CustomizedConverter> getConverter(FunctionDefinition functionDefinition) {
        return Optional.ofNullable(CONVERTERS.get(functionDefinition));
    }
}
