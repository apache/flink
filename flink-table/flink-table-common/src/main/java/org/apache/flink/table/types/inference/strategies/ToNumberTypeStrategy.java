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

package org.apache.flink.table.types.inference.strategies;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.inference.TypeStrategy;
import org.apache.flink.table.utils.NumberParser;

import java.util.Optional;

/**
 * Type strategy of {@link org.apache.flink.table.functions.BuiltInFunctionDefinitions#TO_NUMBER}.
 *
 * <p>Infer output type from the format string, return Optional.empty() if type is invalid.
 */
@Internal
public class ToNumberTypeStrategy implements TypeStrategy {

    @Override
    public Optional<DataType> inferType(CallContext callContext) {
        if (callContext.isArgumentLiteral(1)) {
            final String format = callContext.getArgumentValue(1, String.class).get();

            Optional<NumberParser> parser = NumberParser.of(format);
            if (parser.isPresent()) {
                return parser.get().getOutputType();
            }
        }
        return Optional.empty();
    }
}
