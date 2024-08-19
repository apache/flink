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

package org.apache.flink.table.runtime.functions.scalar;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.SpecializedFunction.SpecializedContext;
import org.apache.flink.table.utils.NumberParser;
import org.apache.flink.table.utils.ThreadLocalCache;

import javax.annotation.Nullable;

import java.util.Optional;

/** Implementation of {@link BuiltInFunctionDefinitions#TO_NUMBER}. */
@Internal
public class ToNumberFunction extends BuiltInScalarFunction {

    private static final ThreadLocalCache<String, Optional<NumberParser>> NUMBER_PARSER_CACHE =
            new ThreadLocalCache<String, Optional<NumberParser>>() {
                @Override
                public Optional<NumberParser> getNewInstance(String key) {
                    return NumberParser.of(key);
                }
            };

    public ToNumberFunction(SpecializedContext context) {
        super(BuiltInFunctionDefinitions.TO_NUMBER, context);
    }

    public @Nullable DecimalData eval(@Nullable StringData expr, StringData format) {
        if (expr == null) {
            return null;
        }
        Optional<NumberParser> parser = NUMBER_PARSER_CACHE.get(format.toString());
        return parser.map(numberParser -> numberParser.parse(expr)).orElse(null);
    }
}
