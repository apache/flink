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

package org.apache.flink.table.planner.plan.nodes.exec.batch;

import org.apache.flink.table.api.TableException;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.common.CommonExecLegacySink;
import org.apache.flink.table.runtime.types.ClassLogicalTypeConverter;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import javax.annotation.Nullable;

import java.lang.reflect.Modifier;

/**
 * Batch {@link ExecNode} to to write data into an external sink defined by a {@link TableSink}.
 *
 * @param <T> The return type of the {@link TableSink}.
 */
public class BatchExecLegacySink<T> extends CommonExecLegacySink<T> implements BatchExecNode<T> {

    public BatchExecLegacySink(
            TableSink<T> tableSink,
            @Nullable String[] upsertKeys,
            InputProperty inputProperty,
            LogicalType outputType,
            String description) {
        super(
                tableSink,
                upsertKeys,
                false, // needRetraction
                false, // isStreaming
                inputProperty,
                outputType,
                description);
    }

    @Override
    protected RowType checkAndConvertInputTypeIfNeeded(RowType inputRowType) {
        final DataType resultDataType = tableSink.getConsumedDataType();
        validateType(resultDataType);
        return inputRowType;
    }

    /**
     * Validate if class represented by the typeInfo is static and globally accessible.
     *
     * @param dataType type to check
     * @throws TableException if type does not meet these criteria
     */
    private void validateType(DataType dataType) {
        Class<?> clazz = dataType.getConversionClass();
        if (clazz == null) {
            clazz =
                    ClassLogicalTypeConverter.getDefaultExternalClassForType(
                            dataType.getLogicalType());
        }
        if (clazz.isMemberClass() && !Modifier.isStatic(clazz.getModifiers())
                || !Modifier.isPublic(clazz.getModifiers())
                || clazz.getCanonicalName() == null) {
            throw new TableException(
                    String.format(
                            "Class '%s' described in type information '%s' must be static and globally accessible.",
                            clazz, dataType));
        }
    }
}
