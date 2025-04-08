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

package org.apache.flink.table.api.internal;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDescriptor;
import org.apache.flink.table.catalog.CatalogView;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.QueryOperationCatalogView;
import org.apache.flink.table.catalog.ResolvedCatalogBaseTable;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.TableDistribution;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.expressions.SqlFactory;
import org.apache.flink.table.utils.EncodingUtils;

import org.apache.commons.lang3.StringUtils;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/** SHOW CREATE statement Util. */
@Internal
public class ShowCreateUtil {

    private static final String PRINT_INDENT = "  ";

    private ShowCreateUtil() {}

    public static String buildShowCreateTableRow(
            ResolvedCatalogBaseTable<?> table,
            ObjectIdentifier tableIdentifier,
            boolean isTemporary,
            SqlFactory sqlFactory) {
        if (table.getTableKind() == CatalogBaseTable.TableKind.VIEW) {
            throw new TableException(
                    String.format(
                            "SHOW CREATE TABLE is only supported for tables, but %s is a view. Please use SHOW CREATE VIEW instead.",
                            tableIdentifier.asSerializableString()));
        }
        StringBuilder sb =
                new StringBuilder()
                        .append(buildCreateFormattedPrefix("TABLE", isTemporary, tableIdentifier));
        sb.append(extractFormattedColumns(table, PRINT_INDENT));
        extractFormattedWatermarkSpecs(table, PRINT_INDENT, sqlFactory)
                .ifPresent(watermarkSpecs -> sb.append(",\n").append(watermarkSpecs));
        extractFormattedPrimaryKey(table, PRINT_INDENT)
                .ifPresent(pk -> sb.append(",\n").append(pk));
        sb.append("\n)\n");
        extractComment(table).ifPresent(c -> sb.append(formatComment(c)).append("\n"));
        extractFormattedDistributedInfo((ResolvedCatalogTable) table).ifPresent(sb::append);
        extractFormattedPartitionedInfo((ResolvedCatalogTable) table)
                .ifPresent(
                        partitionedInfoFormatted ->
                                sb.append("PARTITIONED BY (")
                                        .append(partitionedInfoFormatted)
                                        .append(")\n"));
        extractFormattedOptions(table.getOptions(), PRINT_INDENT)
                .ifPresent(v -> sb.append("WITH (\n").append(v).append("\n)\n"));
        return sb.toString();
    }

    /** Show create view statement only for views. */
    public static String buildShowCreateViewRow(
            ResolvedCatalogBaseTable<?> view,
            ObjectIdentifier viewIdentifier,
            boolean isTemporary) {
        if (view.getTableKind() != CatalogBaseTable.TableKind.VIEW) {
            throw new TableException(
                    String.format(
                            "SHOW CREATE VIEW is only supported for views, but %s is a table. Please use SHOW CREATE TABLE instead.",
                            viewIdentifier.asSerializableString()));
        }
        final CatalogBaseTable origin = view.getOrigin();
        if (origin instanceof QueryOperationCatalogView
                && !((QueryOperationCatalogView) origin).supportsShowCreateView()) {
            throw new TableException(
                    "SHOW CREATE VIEW is not supported for views registered by Table API.");
        }
        StringBuilder sb =
                new StringBuilder()
                        .append(buildCreateFormattedPrefix("VIEW", isTemporary, viewIdentifier));
        sb.append(extractFormattedColumnNames(view, PRINT_INDENT)).append("\n)\n");
        extractComment(view).ifPresent(c -> sb.append(formatComment(c)).append("\n"));
        sb.append("AS ").append(((CatalogView) origin).getExpandedQuery()).append("\n");

        return sb.toString();
    }

    public static String buildShowCreateCatalogRow(CatalogDescriptor catalogDescriptor) {
        final Optional<String> comment = catalogDescriptor.getComment();
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE CATALOG ")
                .append(EncodingUtils.escapeIdentifier(catalogDescriptor.getCatalogName()))
                .append("\n");
        comment.ifPresent(c -> sb.append(formatComment(c)).append("\n"));
        extractFormattedOptions(catalogDescriptor.getConfiguration().toMap(), PRINT_INDENT)
                .ifPresent(o -> sb.append("WITH (\n").append(o).append("\n)\n"));
        return sb.toString();
    }

    static String buildCreateFormattedPrefix(
            String tableType, boolean isTemporary, ObjectIdentifier identifier) {
        return String.format(
                "CREATE %s%s %s (%s",
                isTemporary ? "TEMPORARY " : "",
                tableType,
                identifier.asSerializableString(),
                System.lineSeparator());
    }

    static Optional<String> extractFormattedPrimaryKey(
            ResolvedCatalogBaseTable<?> table, String printIndent) {
        Optional<UniqueConstraint> primaryKey = table.getResolvedSchema().getPrimaryKey();
        return primaryKey.map(
                uniqueConstraint -> String.format("%s%s", printIndent, uniqueConstraint));
    }

    static String getColumnString(Column column) {
        final StringBuilder sb = new StringBuilder();
        sb.append(EncodingUtils.escapeIdentifier(column.getName()));
        sb.append(" ");
        // skip data type for computed column
        if (column instanceof Column.ComputedColumn) {
            sb.append(
                    column.explainExtras()
                            .orElseThrow(
                                    () ->
                                            new TableException(
                                                    String.format(
                                                            "Column expression can not be null for computed column '%s'",
                                                            column.getName()))));
        } else {
            sb.append(column.getDataType().getLogicalType().asSerializableString());
            column.explainExtras()
                    .ifPresent(
                            e -> {
                                sb.append(" ");
                                sb.append(e);
                            });
        }
        column.getComment()
                .ifPresent(
                        comment -> {
                            if (StringUtils.isNotEmpty(comment)) {
                                sb.append(" ");
                                sb.append(formatComment(comment));
                            }
                        });
        return sb.toString();
    }

    static String extractFormattedColumns(ResolvedCatalogBaseTable<?> table, String printIndent) {
        return table.getResolvedSchema().getColumns().stream()
                .map(column -> String.format("%s%s", printIndent, getColumnString(column)))
                .collect(Collectors.joining(",\n"));
    }

    static Optional<String> extractFormattedWatermarkSpecs(
            ResolvedCatalogBaseTable<?> table, String printIndent, SqlFactory sqlFactory) {
        if (table.getResolvedSchema().getWatermarkSpecs().isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(
                table.getResolvedSchema().getWatermarkSpecs().stream()
                        .map(
                                watermarkSpec ->
                                        String.format(
                                                "%sWATERMARK FOR %s AS %s",
                                                printIndent,
                                                EncodingUtils.escapeIdentifier(
                                                        watermarkSpec.getRowtimeAttribute()),
                                                watermarkSpec
                                                        .getWatermarkExpression()
                                                        .asSerializableString(sqlFactory)))
                        .collect(Collectors.joining("\n")));
    }

    private static String formatComment(String comment) {
        return String.format("COMMENT '%s'", EncodingUtils.escapeSingleQuotes(comment));
    }

    static Optional<String> extractComment(ResolvedCatalogBaseTable<?> table) {
        return StringUtils.isEmpty(table.getComment())
                ? Optional.empty()
                : Optional.of(table.getComment());
    }

    static Optional<String> extractFormattedDistributedInfo(ResolvedCatalogTable catalogTable) {
        return catalogTable.getDistribution().map(TableDistribution::toString);
    }

    static Optional<String> extractFormattedPartitionedInfo(ResolvedCatalogTable catalogTable) {
        if (!catalogTable.isPartitioned()) {
            return Optional.empty();
        }
        return Optional.of(
                catalogTable.getPartitionKeys().stream()
                        .map(EncodingUtils::escapeIdentifier)
                        .collect(Collectors.joining(", ")));
    }

    static Optional<String> extractFormattedOptions(Map<String, String> conf, String printIndent) {
        if (Objects.isNull(conf) || conf.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(
                conf.entrySet().stream()
                        .map(
                                entry ->
                                        String.format(
                                                "%s'%s' = '%s'",
                                                printIndent,
                                                EncodingUtils.escapeSingleQuotes(entry.getKey()),
                                                EncodingUtils.escapeSingleQuotes(entry.getValue())))
                        .sorted()
                        .collect(Collectors.joining("," + System.lineSeparator())));
    }

    static String extractFormattedColumnNames(
            ResolvedCatalogBaseTable<?> baseTable, String printIndent) {
        return baseTable.getResolvedSchema().getColumns().stream()
                .map(
                        column ->
                                String.format(
                                        "%s%s",
                                        printIndent,
                                        EncodingUtils.escapeIdentifier(column.getName())))
                .collect(Collectors.joining(",\n"));
    }
}
