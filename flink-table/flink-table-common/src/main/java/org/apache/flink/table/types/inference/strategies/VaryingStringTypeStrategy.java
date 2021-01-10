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
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.util.Preconditions;

import java.util.Objects;
import java.util.Optional;

import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.getLength;
import static org.apache.flink.table.types.utils.DataTypeUtils.replaceLogicalType;

/**
 * A type strategy that ensures that the result type is either {@link LogicalTypeRoot#VARCHAR} or
 * {@link LogicalTypeRoot#VARBINARY} from their corresponding non-varying roots.
 */
@Internal
public final class VaryingStringTypeStrategy implements TypeStrategy {

    private final TypeStrategy initialStrategy;

    public VaryingStringTypeStrategy(TypeStrategy initialStrategy) {
        this.initialStrategy = Preconditions.checkNotNull(initialStrategy);
    }

    @Override
    public Optional<DataType> inferType(CallContext callContext) {
        return initialStrategy
                .inferType(callContext)
                .map(
                        inferredDataType -> {
                            final LogicalType inferredType = inferredDataType.getLogicalType();
                            switch (inferredType.getTypeRoot()) {
                                case CHAR:
                                    {
                                        final int length = getLength(inferredType);
                                        final LogicalType varyingType;
                                        if (length == 0) {
                                            varyingType = VarCharType.ofEmptyLiteral();
                                        } else {
                                            varyingType =
                                                    new VarCharType(
                                                            inferredType.isNullable(), length);
                                        }
                                        return replaceLogicalType(inferredDataType, varyingType);
                                    }
                                case BINARY:
                                    {
                                        final int length = getLength(inferredType);
                                        final LogicalType varyingType;
                                        if (length == 0) {
                                            varyingType = VarBinaryType.ofEmptyLiteral();
                                        } else {
                                            varyingType =
                                                    new VarBinaryType(
                                                            inferredType.isNullable(), length);
                                        }
                                        return replaceLogicalType(inferredDataType, varyingType);
                                    }
                                default:
                                    return inferredDataType;
                            }
                        });
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        VaryingStringTypeStrategy that = (VaryingStringTypeStrategy) o;
        return initialStrategy.equals(that.initialStrategy);
    }

    @Override
    public int hashCode() {
        return Objects.hash(initialStrategy);
    }
}
