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

package org.apache.flink.table.runtime.functions.ptf;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.flink.table.data.utils.ProjectedRowData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.SpecializedFunction.SpecializedContext;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.StateTypeStrategy;
import org.apache.flink.table.types.inference.utils.CallContextUtils;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.types.RowKind;

import java.util.LinkedHashMap;

import static org.apache.flink.table.functions.definitions.DeduplicateDefinition.ARG_INPUT;

/** Base class of the {@link Deduplicate} implementations for updating input. */
@Internal
abstract class DeduplicateUpdatingFunctionBase extends DeduplicateFunctionBase {

    private static final long serialVersionUID = 1L;

    protected final boolean isUpdateBeforeEmitted;

    private final LogicalType[] inputTypes;

    private transient RowDataSerializer inputSerializer;

    private transient ProjectedRowData projectedRow;

    private transient JoinedRowData outputRow;

    private transient RowData emptyRow;

    DeduplicateUpdatingFunctionBase(
            final SpecializedContext context,
            final LinkedHashMap<String, StateTypeStrategy> stateStrategies) {
        super(context, stateStrategies);
        this.isUpdateBeforeEmitted =
                context.getCallContext()
                        .getOutputChangelogMode()
                        .map(mode -> mode.contains(RowKind.UPDATE_BEFORE))
                        .orElse(false);
        this.inputTypes =
                DataType.getFieldDataTypes(inputRowType(context)).stream()
                        .map(DataType::getLogicalType)
                        .toArray(LogicalType[]::new);
    }

    @Override
    public void open(final FunctionContext context) throws Exception {
        super.open(context);
        inputSerializer = new RowDataSerializer(inputTypes);
        projectedRow = ProjectedRowData.from(outputIndices);
        outputRow = new JoinedRowData();
        emptyRow = GenericRowData.of();
    }

    /** Returns the type of a full input row, as kept in state. */
    static DataType inputRowType(final SpecializedContext context) {
        return DataTypes.ROW(
                DataType.getFields(
                        CallContextUtils.getTableSemanticsOrThrow(
                                        context.getCallContext(), ARG_INPUT)
                                .dataType()));
    }

    /** Returns a copy of the input row with row kind INSERT, so that equal rows compare equal. */
    protected BinaryRowData toStateRow(final RowData input) {
        final BinaryRowData row = inputSerializer.toBinaryRow(input).copy();
        row.setRowKind(RowKind.INSERT);
        return row;
    }

    /** Emits the output columns of an input row with the given row kind, leaving the row as is. */
    protected void emit(final RowKind kind, final RowData inputRow) {
        projectedRow.replaceRow(inputRow);
        outputRow.replace(projectedRow, emptyRow);
        outputRow.setRowKind(kind);
        collect(outputRow);
    }
}
