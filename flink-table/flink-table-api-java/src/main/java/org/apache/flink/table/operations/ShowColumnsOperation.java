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
import org.apache.flink.table.catalog.ContextResolvedTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.WatermarkSpec;
import org.apache.flink.table.functions.SqlLikeUtils;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.flink.table.api.internal.TableResultUtils.buildTableResult;

/** Show columns from [[catalog.]database.]table. */
@Internal
public class ShowColumnsOperation implements ShowOperation {

    private final ObjectIdentifier tableIdentifier;
    private final boolean useLike;
    private final boolean notLike;
    private final @Nullable String likePattern;
    private final String preposition;

    public ShowColumnsOperation(
            ObjectIdentifier tableIdentifier,
            @Nullable String likePattern,
            boolean useLike,
            boolean notLike,
            String preposition) {
        this.tableIdentifier = tableIdentifier;
        this.likePattern = likePattern;
        this.useLike = useLike;
        this.notLike = notLike;
        this.preposition = preposition;
    }

    public String getLikePattern() {
        return likePattern;
    }

    public String getPreposition() {
        return preposition;
    }

    public boolean isUseLike() {
        return useLike;
    }

    public boolean isNotLike() {
        return notLike;
    }

    public ObjectIdentifier getTableIdentifier() {
        return tableIdentifier;
    }

    @Override
    public String asSummaryString() {
        if (useLike) {
            if (notLike) {
                return String.format(
                        "SHOW COLUMNS %s %s %s LIKE '%s'",
                        preposition, tableIdentifier.asSummaryString(), "NOT", likePattern);
            }
            return String.format(
                    "SHOW COLUMNS %s %s LIKE '%s'",
                    preposition, tableIdentifier.asSummaryString(), likePattern);
        }
        return String.format("SHOW COLUMNS %s %s", preposition, tableIdentifier.asSummaryString());
    }

    @Override
    public TableResultInternal execute(Context ctx) {
        Optional<ContextResolvedTable> result = ctx.getCatalogManager().getTable(tableIdentifier);
        if (!result.isPresent()) {
            throw new ValidationException(
                    String.format(
                            "Tables or views with the identifier '%s' doesn't exist.",
                            tableIdentifier.asSummaryString()));
        }

        ResolvedSchema schema = result.get().getResolvedSchema();
        Object[][] rows = generateTableColumnsRows(schema);
        if (useLike) {
            rows =
                    Arrays.stream(rows)
                            .filter(
                                    row ->
                                            notLike
                                                    != SqlLikeUtils.like(
                                                            row[0].toString(), likePattern, "\\"))
                            .toArray(Object[][]::new);
        }
        boolean nonComments = isSchemaNonColumnComments(schema);
        return buildTableResult(
                generateTableColumnsNames(nonComments),
                generateTableColumnsDataTypes(nonComments),
                rows);
    }

    private Object[][] generateTableColumnsRows(ResolvedSchema schema) {
        Map<String, String> fieldToWatermark =
                schema.getWatermarkSpecs().stream()
                        .collect(
                                Collectors.toMap(
                                        WatermarkSpec::getRowtimeAttribute,
                                        spec -> spec.getWatermarkExpression().asSummaryString()));

        Map<String, String> fieldToPrimaryKey = new HashMap<>();
        schema.getPrimaryKey()
                .ifPresent(
                        (p) -> {
                            List<String> columns = p.getColumns();
                            columns.forEach(
                                    (c) ->
                                            fieldToPrimaryKey.put(
                                                    c,
                                                    String.format(
                                                            "PRI(%s)",
                                                            String.join(", ", columns))));
                        });
        boolean nonComments = isSchemaNonColumnComments(schema);
        return schema.getColumns().stream()
                .map(
                        (c) -> {
                            final LogicalType logicalType = c.getDataType().getLogicalType();
                            final ArrayList<Object> result =
                                    new ArrayList<>(
                                            Arrays.asList(
                                                    c.getName(),
                                                    logicalType.copy(true).asSummaryString(),
                                                    logicalType.isNullable(),
                                                    fieldToPrimaryKey.getOrDefault(
                                                            c.getName(), null),
                                                    c.explainExtras().orElse(null),
                                                    fieldToWatermark.getOrDefault(
                                                            c.getName(), null)));
                            if (!nonComments) {
                                result.add(c.getComment().orElse(null));
                            }
                            return result.toArray();
                        })
                .toArray(Object[][]::new);
    }

    private boolean isSchemaNonColumnComments(ResolvedSchema schema) {
        return schema.getColumns().stream().map(Column::getComment).noneMatch(Optional::isPresent);
    }

    private String[] generateTableColumnsNames(boolean nonComments) {
        final ArrayList<String> result =
                new ArrayList<>(
                        Arrays.asList("name", "type", "null", "key", "extras", "watermark"));
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
                                DataTypes.STRING(),
                                DataTypes.STRING(),
                                DataTypes.STRING()));
        if (!nonComments) {
            result.add(DataTypes.STRING());
        }
        return result.toArray(new DataType[0]);
    }
}
