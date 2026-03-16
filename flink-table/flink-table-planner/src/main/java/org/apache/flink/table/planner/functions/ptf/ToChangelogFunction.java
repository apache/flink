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

package org.apache.flink.table.planner.functions.ptf;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.ProcessTableFunction;
import org.apache.flink.table.functions.SpecializedFunction.SpecializedContext;
import org.apache.flink.table.functions.TableSemantics;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.types.ColumnList;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import javax.annotation.Nullable;

import java.util.EnumMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Runtime implementation of {@link BuiltInFunctionDefinitions#TO_CHANGELOG}.
 *
 * <p>Converts each input row into an INSERT-only output row with an operation code column. The
 * output schema is {@code [op_column, ...non_partition_columns...]} - the framework prepends
 * partition key columns automatically.
 */
@Internal
public class ToChangelogFunction extends ProcessTableFunction<Row> {

    private static final long serialVersionUID = 1L;

    private static final Map<RowKind, String> DEFAULT_OP_MAPPING =
            Map.of(
                    RowKind.INSERT, "INSERT",
                    RowKind.UPDATE_BEFORE, "UPDATE_BEFORE",
                    RowKind.UPDATE_AFTER, "UPDATE_AFTER",
                    RowKind.DELETE, "DELETE");

    private final Map<RowKind, String> opMap;
    private final Set<Integer> partitionKeySet;
    private final int partitionKeyCount;

    @SuppressWarnings("unchecked")
    public ToChangelogFunction(final SpecializedContext context) {
        final CallContext callContext = context.getCallContext();

        final TableSemantics semantics =
                callContext
                        .getTableSemantics(0)
                        .orElseThrow(() -> new IllegalStateException("Table argument expected."));
        final int[] partitionKeys = semantics.partitionByColumns();
        this.partitionKeySet = IntStream.of(partitionKeys).boxed().collect(Collectors.toSet());
        this.partitionKeyCount = partitionKeys.length;

        final Map<String, String> opMapping =
                callContext.getArgumentValue(2, Map.class).orElse(null);
        this.opMap = buildOpMap(opMapping);
    }

    @Override
    public TypeInference getTypeInference(final DataTypeFactory typeFactory) {
        return BuiltInFunctionDefinitions.TO_CHANGELOG.getTypeInference(typeFactory);
    }

    public void eval(
            final Context ctx,
            final Row input,
            @Nullable final ColumnList op,
            @Nullable final Map<String, String> opMapping) {

        final String opCode = opMap.get(input.getKind());
        if (opCode == null) {
            // TODO Gustavo Error handling still to be implemented in following ticket
            return;
        }

        collect(buildOutputRow(input, opCode));
    }

    private Row buildOutputRow(final Row input, final String opCode) {
        final Object[] fields = new Object[input.getArity() - partitionKeyCount + 1];
        fields[0] = opCode;
        int outputIdx = 1;
        for (int i = 0; i < input.getArity(); i++) {
            if (!partitionKeySet.contains(i)) {
                fields[outputIdx++] = input.getField(i);
            }
        }
        return Row.ofKind(RowKind.INSERT, fields);
    }

    private static Map<RowKind, String> buildOpMap(@Nullable final Map<String, String> opMapping) {
        if (opMapping == null) {
            return new EnumMap<>(DEFAULT_OP_MAPPING);
        }
        final Map<RowKind, String> map = new EnumMap<>(RowKind.class);
        opMapping.forEach((name, code) -> map.put(RowKind.valueOf(name), code));
        return map;
    }
}
