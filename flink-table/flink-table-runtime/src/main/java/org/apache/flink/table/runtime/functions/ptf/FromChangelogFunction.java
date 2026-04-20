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
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.utils.ProjectedRowData;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.SpecializedFunction.SpecializedContext;
import org.apache.flink.table.functions.TableSemantics;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.ColumnList;
import org.apache.flink.types.RowKind;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Runtime implementation of {@link BuiltInFunctionDefinitions#FROM_CHANGELOG}.
 *
 * <p>Converts each append-only input row (which contains an operation code column) back into a
 * changelog stream with proper {@link RowKind} annotations. The output schema excludes the
 * operation code column and partition key columns (which are prepended by the framework
 * automatically).
 *
 * <p>This is the reverse operation of {@link ToChangelogFunction}.
 */
@Internal
public class FromChangelogFunction extends BuiltInProcessTableFunction<RowData> {

    private static final long serialVersionUID = 1L;

    private static final String DEFAULT_OP_COLUMN_NAME = "op";
    private static final Map<String, RowKind> DEFAULT_OP_MAPPING =
            Map.of(
                    "INSERT", RowKind.INSERT,
                    "UPDATE_BEFORE", RowKind.UPDATE_BEFORE,
                    "UPDATE_AFTER", RowKind.UPDATE_AFTER,
                    "DELETE", RowKind.DELETE);

    private final Map<String, RowKind> rawOpMap;
    private final int opColumnIndex;
    private final int[] outputIndices;

    private transient HashMap<StringData, RowKind> opMap;
    private transient ProjectedRowData projectedOutput;

    public FromChangelogFunction(final SpecializedContext context) {
        super(BuiltInFunctionDefinitions.FROM_CHANGELOG, context);
        final CallContext callContext = context.getCallContext();

        final TableSemantics tableSemantics =
                callContext
                        .getTableSemantics(0)
                        .orElseThrow(() -> new IllegalStateException("Table argument expected."));

        final RowType inputType = (RowType) tableSemantics.dataType().getLogicalType();
        final String opColumnName = resolveOpColumnName(callContext);
        this.opColumnIndex = inputType.getFieldNames().indexOf(opColumnName);

        // Exclude only the op column from output — all other columns pass through
        this.outputIndices =
                IntStream.range(0, inputType.getFieldCount())
                        .filter(i -> i != opColumnIndex)
                        .toArray();

        this.rawOpMap = buildOpMap(callContext);
    }

    @Override
    public void open(final FunctionContext context) throws Exception {
        super.open(context);
        opMap = new HashMap<>();
        rawOpMap.forEach((code, kind) -> opMap.put(StringData.fromString(code), kind));
        projectedOutput = ProjectedRowData.from(outputIndices);
    }

    private static String resolveOpColumnName(final CallContext callContext) {
        return callContext
                .getArgumentValue(1, ColumnList.class)
                .map(cl -> cl.getNames().get(0))
                .orElse(DEFAULT_OP_COLUMN_NAME);
    }

    /**
     * Builds a String-to-RowKind map. Keys in the provided mapping may be comma-separated (e.g.,
     * "INSERT, UPDATE_AFTER") to map multiple input codes to the same RowKind.
     */
    private static Map<String, RowKind> buildOpMap(CallContext callContext) {
        return callContext
                .getArgumentValue(2, Map.class)
                .map(FromChangelogFunction::parseOpMapping)
                .orElse(DEFAULT_OP_MAPPING);
    }

    private static Map<String, RowKind> parseOpMapping(Map<String, String> opMapping) {
        return opMapping.entrySet().stream()
                .flatMap(
                        e -> {
                            final RowKind kind = RowKind.valueOf(e.getValue().trim());
                            return Arrays.stream(e.getKey().split(","))
                                    .map(code -> Map.entry(code.trim(), kind));
                        })
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    public void eval(
            final Context ctx,
            final RowData input,
            @Nullable final ColumnList op,
            @Nullable final MapData opMapping) {
        final StringData opCode = input.getString(opColumnIndex);
        final RowKind rowKind = opMap.get(opCode);
        if (rowKind == null) {
            return;
        }

        projectedOutput.replaceRow(input);
        projectedOutput.setRowKind(rowKind);
        collect(projectedOutput);
    }
}
