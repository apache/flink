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
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.inference.TypeStrategy;
import org.apache.flink.table.utils.DateTimeUtils;

import java.util.Optional;

/**
 * Type strategy of {@code TO_TIMESTAMP}. Returns {@code TIMESTAMP(3)} for the 1-arg variant and
 * infers precision from the format pattern's trailing 'S' count for the 2-arg variant.
 */
@Internal
public class ToTimestampTypeStrategy implements TypeStrategy {

    private static final int DEFAULT_PRECISION = 3;

    @Override
    public Optional<DataType> inferType(CallContext callContext) {
        int outputPrecision = DEFAULT_PRECISION;

        if (callContext.getArgumentDataTypes().size() == 2) {
            outputPrecision = inferPrecisionFromFormat(callContext);
        }

        return Optional.of(DataTypes.TIMESTAMP(outputPrecision).nullable());
    }

    /**
     * Infers the output precision from a format string literal. Returns at least {@link
     * #DEFAULT_PRECISION}.
     */
    private static int inferPrecisionFromFormat(CallContext callContext) {
        if (!callContext.isArgumentLiteral(1)) {
            return DEFAULT_PRECISION;
        }
        return callContext
                .getArgumentValue(1, String.class)
                .map(DateTimeUtils::precisionFromFormat)
                .orElse(DEFAULT_PRECISION);
    }
}
