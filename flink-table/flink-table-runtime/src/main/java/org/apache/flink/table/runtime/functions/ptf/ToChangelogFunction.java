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
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.flink.table.data.utils.ProjectedRowData;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.SpecializedFunction.SpecializedContext;
import org.apache.flink.table.functions.TableSemantics;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.inference.strategies.ChangelogTypeStrategyUtils;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.utils.UpsertKeyUtils;
import org.apache.flink.types.ColumnList;
import org.apache.flink.types.RowKind;

import javax.annotation.Nullable;

import java.util.EnumMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.table.types.inference.strategies.ToChangelogTypeStrategy.ARG_OP_MAPPING;
import static org.apache.flink.table.types.inference.strategies.ToChangelogTypeStrategy.ARG_PRODUCES_FULL_DELETES;
import static org.apache.flink.table.types.inference.strategies.ToChangelogTypeStrategy.ARG_TABLE;

/**
 * Runtime implementation of {@link BuiltInFunctionDefinitions#TO_CHANGELOG}.
 *
 * <p>Converts each input row into an INSERT-only output row with an operation code column. Output
 * schema is {@code [op_column, ...projected_input_columns...]}. Partition columns are prepended by
 * the framework outside this function and are not part of the projection.
 *
 * <p>Uses {@link JoinedRowData} to combine the op column with the full input row.
 */
@Internal
public class ToChangelogFunction extends BuiltInProcessTableFunction<RowData> {

    private static final long serialVersionUID = 1L;

    private static final Map<RowKind, String> DEFAULT_OP_MAPPING =
            Map.of(
                    RowKind.INSERT, "INSERT",
                    RowKind.UPDATE_BEFORE, "UPDATE_BEFORE",
                    RowKind.UPDATE_AFTER, "UPDATE_AFTER",
                    RowKind.DELETE, "DELETE");

    private final Map<RowKind, String> rawOpMap;
    private final int[] outputIndices;
    private final RowType inputRowType;
    private final boolean producesFullDelete;
    private final boolean[] upsertKeyColumn;

    private transient Map<RowKind, StringData> opMap;
    private transient GenericRowData opRow;
    private transient JoinedRowData output;
    private transient ProjectedRowData projectedOutput;
    private transient GenericRowData partialDeletePayload;
    private transient RowData.FieldGetter[] preservedFieldGetters;

    @SuppressWarnings("unchecked")
    public ToChangelogFunction(final SpecializedContext context) {
        super(BuiltInFunctionDefinitions.TO_CHANGELOG, context);
        final CallContext callContext = context.getCallContext();
        // Table argument is guaranteed by the type strategy's validation phase.
        final TableSemantics tableSemantics = callContext.getTableSemantics(ARG_TABLE).get();

        final Map<String, String> opMapping =
                callContext.getArgumentValue(ARG_OP_MAPPING, Map.class).orElse(null);
        this.rawOpMap = buildOpMap(opMapping);
        if (opMapping != null) {
            validateOpMap(this.rawOpMap, tableSemantics);
        }
        final boolean producesFullDeletesArg =
                callContext.getArgumentValue(ARG_PRODUCES_FULL_DELETES, Boolean.class).orElse(true);
        final boolean isExplicit = !callContext.isArgumentNull(ARG_PRODUCES_FULL_DELETES);
        validateProducesFullDeletes(producesFullDeletesArg, isExplicit, tableSemantics);

        this.outputIndices = ChangelogTypeStrategyUtils.computeOutputIndices(tableSemantics);
        this.inputRowType = (RowType) tableSemantics.dataType().getLogicalType();
        this.producesFullDelete = producesFullDeletesArg;
        this.upsertKeyColumn =
                computeUpsertKeyColumn(
                        this.outputIndices,
                        UpsertKeyUtils.smallestKey(tableSemantics.upsertKeyColumns()));
    }

    private static boolean[] computeUpsertKeyColumn(
            final int[] outputIndices, final int[] upsertKey) {
        final Set<Integer> keepInputIndices = new HashSet<>();
        for (final int key : upsertKey) {
            keepInputIndices.add(key);
        }
        final boolean[] mask = new boolean[outputIndices.length];
        for (int i = 0; i < outputIndices.length; i++) {
            mask[i] = keepInputIndices.contains(outputIndices[i]);
        }
        return mask;
    }

    @Override
    public void open(final FunctionContext context) throws Exception {
        super.open(context);
        opMap = new EnumMap<>(RowKind.class);
        rawOpMap.forEach((kind, code) -> opMap.put(kind, StringData.fromString(code)));
        opRow = new GenericRowData(1);
        output = new JoinedRowData();
        projectedOutput = ProjectedRowData.from(outputIndices);
        partialDeletePayload = new GenericRowData(outputIndices.length);
        preservedFieldGetters = new RowData.FieldGetter[outputIndices.length];
        final List<LogicalType> inputFieldTypes = inputRowType.getChildren();
        for (int i = 0; i < outputIndices.length; i++) {
            if (upsertKeyColumn[i]) {
                preservedFieldGetters[i] =
                        RowData.createFieldGetter(
                                inputFieldTypes.get(outputIndices[i]), outputIndices[i]);
            }
        }
    }

    /**
     * Builds a RowKind-to-output-code map. Keys may be comma-separated (e.g., "INSERT,
     * UPDATE_AFTER") to map multiple RowKinds to the same output code.
     */
    private static Map<RowKind, String> buildOpMap(@Nullable final Map<String, String> opMapping) {
        if (opMapping == null) {
            return new EnumMap<>(DEFAULT_OP_MAPPING);
        }
        final Map<RowKind, String> result = new EnumMap<>(RowKind.class);
        opMapping.forEach(
                (commaSeparatedRowKinds, outputCode) -> {
                    for (final String rawName : commaSeparatedRowKinds.split(",")) {
                        result.put(RowKind.valueOf(rawName.trim()), outputCode);
                    }
                });
        return result;
    }

    /**
     * Rejects user-provided mappings that reference change operations the input changelog cannot
     * produce. Without this check the extra entries are dead code: the corresponding rows never
     * arrive. E.g., if the input is INSERT-only, then UPDATE_BEFORE and DELETE mappings are
     * ignored, which is likely a user mistake.
     *
     * <p>Lives here rather than in the input type strategy because {@link
     * TableSemantics#changelogMode()} returns empty during type inference and is only populated at
     * specialization time, which is when this constructor runs.
     */
    private static void validateOpMap(
            final Map<RowKind, String> mapping, final TableSemantics tableSemantics) {
        final ChangelogMode inputMode = tableSemantics.changelogMode().orElse(null);
        if (inputMode == null) {
            return;
        }
        final List<RowKind> unsupported =
                mapping.keySet().stream()
                        .filter(kind -> !inputMode.contains(kind))
                        .collect(Collectors.toList());
        if (!unsupported.isEmpty()) {
            throw new ValidationException(
                    String.format(
                            "Invalid 'op_mapping' for TO_CHANGELOG: the input table only produces "
                                    + "%s and does not produce %s. Remove those entries from the "
                                    + "mapping.",
                            inputMode.getContainedKinds(), unsupported));
        }
    }

    /**
     * Validates an explicit {@code produces_full_deletes} argument against the input.
     *
     * <p>For {@code produces_full_deletes=true}, the input changelog must emit DELETE rows;
     * otherwise the parameter is dead. For {@code produces_full_deletes=false}, the input must
     * declare an upsert key or the call must use {@code PARTITION BY}; otherwise the function has
     * no identifying columns to preserve when nulling the rest.
     *
     * <p>No validation runs when the argument is absent, since the default (full deletes) is safe
     * for any input.
     *
     * <p>Lives here rather than in the input type strategy because {@link
     * TableSemantics#changelogMode()} and {@link TableSemantics#upsertKeyColumns()} are only
     * populated at specialization time.
     */
    private static void validateProducesFullDeletes(
            final boolean producesFullDeletesArg,
            final boolean isExplicit,
            final TableSemantics tableSemantics) {
        if (!isExplicit) {
            return;
        }
        if (producesFullDeletesArg) {
            final ChangelogMode inputMode = tableSemantics.changelogMode().orElse(null);
            if (inputMode != null && !inputMode.contains(RowKind.DELETE)) {
                throw new ValidationException(
                        String.format(
                                "Invalid 'produces_full_deletes' for TO_CHANGELOG: the input "
                                        + "table only produces %s and never emits DELETE rows. "
                                        + "Remove the 'produces_full_deletes' argument.",
                                inputMode.getContainedKinds()));
            }
            return;
        }
        final boolean hasPartitionBy = tableSemantics.partitionByColumns().length > 0;
        final boolean hasUpsertKey = !tableSemantics.upsertKeyColumns().isEmpty();
        if (!hasPartitionBy && !hasUpsertKey) {
            throw new ValidationException(
                    "Invalid 'produces_full_deletes=false' for TO_CHANGELOG: the input has no "
                            + "upsert key and the call has no PARTITION BY, so the function has "
                            + "no identifying columns to preserve on DELETE rows. Remove the "
                            + "argument (the default emits full DELETE rows) or add a "
                            + "PARTITION BY.");
        }
    }

    public void eval(
            final Context ctx,
            final RowData input,
            @Nullable final ColumnList op,
            @Nullable final MapData opMapping,
            @Nullable final Boolean producesFullDeletes) {
        final StringData opCode = opMap.get(input.getRowKind());
        if (opCode == null) {
            return;
        }

        opRow.setField(0, opCode);
        final RowData payload;
        if (input.getRowKind() == RowKind.DELETE && !producesFullDelete) {
            payload = buildPartialDeletePayload(input);
        } else {
            payload = projectedOutput.replaceRow(input);
        }
        collect(output.replace(opRow, payload));
    }

    /**
     * Builds the payload for a partial DELETE row: upsert-key columns are copied from the input,
     * all other columns are emitted as {@code null}. Partition-key columns are not included here
     * since the framework prepends them outside the function's projected output.
     */
    private RowData buildPartialDeletePayload(final RowData input) {
        for (int i = 0; i < outputIndices.length; i++) {
            if (upsertKeyColumn[i]) {
                partialDeletePayload.setField(i, preservedFieldGetters[i].getFieldOrNull(input));
            } else {
                partialDeletePayload.setField(i, null);
            }
        }
        return partialDeletePayload;
    }
}
