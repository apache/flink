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
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import java.util.List;
import java.util.Optional;

/** Type strategy of {@code TO_TIMESTAMP_LTZ}. */
@Internal
public class ToTimestampLtzTypeStrategy implements TypeStrategy {

    private static final int DEFAULT_PRECISION = 3;

    @Override
    public Optional<DataType> inferType(CallContext callContext) {
        List<DataType> argumentTypes = callContext.getArgumentDataTypes();
        int argCount = argumentTypes.size();

        if (argCount < 1 || argCount > 3) {
            throw new IllegalArgumentException(
                    "TO_TIMESTAMP_LTZ requires 1 to 3 arguments, but "
                            + argCount
                            + "arguments were provided.");
        }

        if (argCount == 1) {
            // TO_TIMESTAMP_LTZ(numeric)
            // TO_TIMESTAMP_LTZ(string)
            return Optional.of(DataTypes.TIMESTAMP_LTZ(DEFAULT_PRECISION));
        } else if (argCount == 2) {
            LogicalTypeRoot firstArgTypeRoot = argumentTypes.get(0).getLogicalType().getTypeRoot();
            boolean isFirstArgNumeric =
                    firstArgTypeRoot == LogicalTypeRoot.TINYINT
                            || firstArgTypeRoot == LogicalTypeRoot.SMALLINT
                            || firstArgTypeRoot == LogicalTypeRoot.INTEGER
                            || firstArgTypeRoot == LogicalTypeRoot.BIGINT
                            || firstArgTypeRoot == LogicalTypeRoot.FLOAT
                            || firstArgTypeRoot == LogicalTypeRoot.DOUBLE
                            || firstArgTypeRoot == LogicalTypeRoot.DECIMAL;
            // TO_TIMESTAMP_LTZ(numeric, precision)
            if (callContext.isArgumentLiteral(1) && isFirstArgNumeric) {
                final int precision = callContext.getArgumentValue(1, Integer.class).get();
                return Optional.of(DataTypes.TIMESTAMP_LTZ(precision));
            }
            // TO_TIMESTAMP_LTZ(string, format)
            return Optional.of(DataTypes.TIMESTAMP_LTZ(DEFAULT_PRECISION));
        } else {
            // argCount == 3
            // TO_TIMESTAMP_LTZ(string, format, timezone)
            return Optional.of(DataTypes.TIMESTAMP_LTZ(DEFAULT_PRECISION));
        }
    }
}
