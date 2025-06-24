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

package org.apache.flink.table.operations;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.internal.TableResultInternal;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ContextResolvedModel;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;

import org.apache.flink.shaded.guava33.com.google.common.collect.Streams;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.table.api.internal.TableResultUtils.buildTableResult;

/**
 * Operation to describe a DESCRIBE MODEL [EXTENDED] [[catalogName.] dataBasesName.]sqlIdentifier
 * statement.
 */
@Internal
public class DescribeModelOperation implements Operation, ExecutableOperation {

    private final ObjectIdentifier sqlIdentifier;
    private final boolean isExtended;

    public DescribeModelOperation(ObjectIdentifier sqlIdentifier, boolean isExtended) {
        this.sqlIdentifier = sqlIdentifier;
        this.isExtended = isExtended;
    }

    public ObjectIdentifier getSqlIdentifier() {
        return sqlIdentifier;
    }

    public boolean isExtended() {
        return isExtended;
    }

    @Override
    public String asSummaryString() {
        Map<String, Object> params = new LinkedHashMap<>();
        params.put("identifier", sqlIdentifier);
        params.put("isExtended", isExtended);
        return OperationUtils.formatWithChildren(
                "DESCRIBE MODEL", params, Collections.emptyList(), Operation::asSummaryString);
    }

    @Override
    public TableResultInternal execute(Context ctx) {
        // DESCRIBE MODEL <model> shows input/output schema if any.
        Optional<ContextResolvedModel> result = ctx.getCatalogManager().getModel(sqlIdentifier);
        if (result.isEmpty()) {
            throw new ValidationException(
                    String.format(
                            "Model with the identifier '%s' doesn't exist.",
                            sqlIdentifier.asSummaryString()));
        }

        ResolvedSchema inputSchema = result.get().getResolvedModel().getResolvedInputSchema();
        ResolvedSchema outputSchema = result.get().getResolvedModel().getResolvedOutputSchema();
        Object[][] rows = generateModelColumnsRows(inputSchema, outputSchema);

        boolean nonComments = isSchemaNonColumnComments(inputSchema, outputSchema);
        return buildTableResult(
                generateTableColumnsNames(nonComments),
                generateTableColumnsDataTypes(nonComments),
                rows);
    }

    private Object[][] generateModelColumnsRows(
            ResolvedSchema inputSchema, ResolvedSchema outputSchema) {
        boolean nonComments = isSchemaNonColumnComments(inputSchema, outputSchema);

        return Streams.concat(
                        inputSchema.getColumns().stream()
                                .map((c) -> buildSingleRow(c, nonComments, true)),
                        outputSchema.getColumns().stream()
                                .map((c) -> buildSingleRow(c, nonComments, false)))
                .toArray(Object[][]::new);
    }

    private Object[] buildSingleRow(Column c, boolean nonComments, boolean isInput) {
        final LogicalType logicalType = c.getDataType().getLogicalType();
        final ArrayList<Object> result =
                new ArrayList<>(
                        Arrays.asList(
                                c.getName(),
                                logicalType.copy(true).asSummaryString(),
                                logicalType.isNullable(),
                                isInput));
        if (!nonComments) {
            result.add(c.getComment().orElse(null));
        }
        return result.toArray();
    }

    private boolean isSchemaNonColumnComments(
            ResolvedSchema inputSchema, ResolvedSchema outputSchema) {
        return inputSchema.getColumns().stream()
                        .map(Column::getComment)
                        .noneMatch(Optional::isPresent)
                && outputSchema.getColumns().stream()
                        .map(Column::getComment)
                        .noneMatch(Optional::isPresent);
    }

    private String[] generateTableColumnsNames(boolean nonComments) {
        final ArrayList<String> result =
                new ArrayList<>(Arrays.asList("name", "type", "null", "is input"));
        if (!nonComments) {
            result.add("comment");
        }
        return result.toArray(new String[0]);
    }

    private DataType[] generateTableColumnsDataTypes(boolean nonComments) {
        final ArrayList<DataType> result =
                new ArrayList<>(
                        Arrays.asList(
                                DataTypes.STRING(),
                                DataTypes.STRING(),
                                DataTypes.BOOLEAN(),
                                DataTypes.BOOLEAN()));
        if (!nonComments) {
            result.add(DataTypes.STRING());
        }
        return result.toArray(new DataType[0]);
    }
}
