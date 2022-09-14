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

package org.apache.flink.table.operations.utils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.operations.QueryOperation;
import org.apache.flink.table.operations.SetQueryOperation;
import org.apache.flink.table.operations.SetQueryOperation.SetQueryOperationType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.utils.LogicalTypeMerging;
import org.apache.flink.table.types.utils.TypeConversions;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.lang.String.format;
import static org.apache.flink.table.operations.SetQueryOperation.SetQueryOperationType.UNION;

/** Utility class for creating a valid {@link SetQueryOperation}. */
@Internal
final class SetOperationFactory {

    private final boolean isStreamingMode;

    public SetOperationFactory(boolean isStreamingMode) {
        this.isStreamingMode = isStreamingMode;
    }

    /**
     * Creates a valid algebraic operation.
     *
     * @param type type of operation to create
     * @param left first relational operation of the operation
     * @param right second relational operation of the operation
     * @param all flag defining how duplicates should be handled
     * @return creates a valid algebraic operation
     */
    QueryOperation create(
            SetQueryOperationType type, QueryOperation left, QueryOperation right, boolean all) {
        failIfStreaming(type, all);
        validateSetOperation(type, left, right);
        return new SetQueryOperation(left, right, type, all, createCommonTableSchema(left, right));
    }

    private void validateSetOperation(
            SetQueryOperationType operationType, QueryOperation left, QueryOperation right) {
        ResolvedSchema leftSchema = left.getResolvedSchema();
        int leftFieldCount = leftSchema.getColumnCount();
        ResolvedSchema rightSchema = right.getResolvedSchema();
        int rightFieldCount = rightSchema.getColumnCount();

        if (leftFieldCount != rightFieldCount) {
            throw new ValidationException(
                    format(
                            "The %s operation on two tables of different column sizes: %d and %d is not supported",
                            operationType.toString().toLowerCase(),
                            leftFieldCount,
                            rightFieldCount));
        }

        final List<DataType> leftDataTypes = leftSchema.getColumnDataTypes();
        final List<DataType> rightDataTypes = rightSchema.getColumnDataTypes();

        IntStream.range(0, leftFieldCount)
                .forEach(
                        idx -> {
                            if (!findCommonColumnType(leftDataTypes, rightDataTypes, idx)
                                    .isPresent()) {
                                throw new ValidationException(
                                        format(
                                                "Incompatible types for %s operation. "
                                                        + "Could not find a common type at position %s for '%s' and '%s'.",
                                                operationType.toString().toLowerCase(),
                                                idx,
                                                leftDataTypes.get(idx),
                                                rightDataTypes.get(idx)));
                            }
                        });
    }

    private void failIfStreaming(SetQueryOperationType type, boolean all) {
        boolean shouldFailInCaseOfStreaming = !all || type != UNION;

        if (isStreamingMode && shouldFailInCaseOfStreaming) {
            throw new ValidationException(
                    format(
                            "The %s operation on two unbounded tables is currently not supported.",
                            type));
        }
    }

    private ResolvedSchema createCommonTableSchema(QueryOperation left, QueryOperation right) {
        final ResolvedSchema leftSchema = left.getResolvedSchema();
        final List<DataType> leftDataTypes = leftSchema.getColumnDataTypes();
        final List<DataType> rightDataTypes = right.getResolvedSchema().getColumnDataTypes();

        final List<DataType> resultDataTypes =
                IntStream.range(0, leftSchema.getColumnCount())
                        .mapToObj(
                                idx ->
                                        findCommonColumnType(leftDataTypes, rightDataTypes, idx)
                                                .orElseThrow(AssertionError::new))
                        .map(TypeConversions::fromLogicalToDataType)
                        .collect(Collectors.toList());
        return ResolvedSchema.physical(leftSchema.getColumnNames(), resultDataTypes);
    }

    private Optional<LogicalType> findCommonColumnType(
            List<DataType> leftDataTypes, List<DataType> rightDataTypes, int idx) {
        final LogicalType leftType = leftDataTypes.get(idx).getLogicalType();
        final LogicalType rightType = rightDataTypes.get(idx).getLogicalType();
        return LogicalTypeMerging.findCommonType(Arrays.asList(leftType, rightType));
    }
}
