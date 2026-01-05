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

package org.apache.flink.table.planner.utils;

import org.apache.flink.sql.parser.SqlParseUtils;
import org.apache.flink.sql.parser.ddl.SqlDistribution;
import org.apache.flink.sql.parser.ddl.resource.SqlResource;
import org.apache.flink.sql.parser.ddl.resource.SqlResourceType;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.FunctionLanguage;
import org.apache.flink.table.catalog.ResolvedCatalogBaseTable;
import org.apache.flink.table.catalog.TableChange;
import org.apache.flink.table.catalog.TableDistribution;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.planner.calcite.FlinkPlannerImpl;
import org.apache.flink.table.planner.operations.converters.SchemaReferencesManager;
import org.apache.flink.table.resource.ResourceType;
import org.apache.flink.table.resource.ResourceUri;
import org.apache.flink.util.StringUtils;

import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.dialect.CalciteSqlDialect;
import org.apache.calcite.sql.parser.SqlParser;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/** Utils methods for converting sql to operations. */
public class OperationConverterUtils {

    private OperationConverterUtils() {}

    public static TableDistribution getDistributionFromSqlDistribution(
            SqlDistribution distribution) {
        TableDistribution.Kind kind =
                TableDistribution.Kind.valueOf(
                        distribution
                                .getDistributionKind()
                                .orElse(TableDistribution.Kind.UNKNOWN.toString()));
        Integer bucketCount = null;
        SqlNumericLiteral count = distribution.getBucketCount();
        if (count != null && count.isInteger()) {
            bucketCount = ((BigDecimal) (count).getValue()).intValue();
        }

        SqlNodeList columns = distribution.getBucketColumns();
        List<String> bucketColumns =
                SqlParseUtils.extractList(columns, p -> ((SqlIdentifier) p).getSimple());
        return TableDistribution.of(kind, bucketCount, bucketColumns);
    }

    public static String getQuotedSqlString(SqlNode sqlNode, FlinkPlannerImpl flinkPlanner) {
        SqlParser.Config parserConfig = flinkPlanner.config().getParserConfig();
        SqlDialect dialect =
                new CalciteSqlDialect(
                        SqlDialect.EMPTY_CONTEXT
                                .withQuotedCasing(parserConfig.unquotedCasing())
                                .withConformance(parserConfig.conformance())
                                .withUnquotedCasing(parserConfig.unquotedCasing())
                                .withIdentifierQuoteString(parserConfig.quoting().string));
        return sqlNode.toSqlString(dialect).getSql();
    }

    public static Set<String> getColumnNames(SqlNodeList sqlNodeList, String errMsgPrefix) {
        Set<String> distinctNames = new HashSet<>();
        for (SqlNode sqlNode : sqlNodeList) {
            String name = extractSimpleColumnName((SqlIdentifier) sqlNode, errMsgPrefix);
            if (!distinctNames.add(name)) {
                throw new ValidationException(
                        String.format("%sDuplicate column `%s`.", errMsgPrefix, name));
            }
        }
        return distinctNames;
    }

    public static String extractSimpleColumnName(SqlIdentifier identifier, String exMsgPrefix) {
        if (!identifier.isSimple()) {
            throw new UnsupportedOperationException(
                    String.format(
                            "%sAltering the nested row type `%s` is not supported yet.",
                            exMsgPrefix, String.join("`.`", identifier.names)));
        }
        return identifier.getSimple();
    }

    public static List<TableChange> validateAndGatherDropWatermarkChanges(
            ResolvedCatalogBaseTable<?> oldTable, String exMsgPrefix, String tableKindStr) {
        if (oldTable.getResolvedSchema().getWatermarkSpecs().isEmpty()) {
            throw new ValidationException(
                    String.format(
                            "%sThe current %s does not define any watermark strategy.",
                            exMsgPrefix, tableKindStr));
        }

        return List.of(TableChange.dropWatermark());
    }

    public static List<TableChange> validateAndGatherDropConstraintChanges(
            ResolvedCatalogBaseTable<?> oldTable,
            SqlIdentifier constraint,
            String exMsgPrefix,
            String tableKindStr) {
        Optional<UniqueConstraint> pkConstraint = oldTable.getResolvedSchema().getPrimaryKey();
        if (pkConstraint.isEmpty()) {
            throw new ValidationException(
                    String.format(
                            "%sThe current %s does not define any primary key.",
                            exMsgPrefix, tableKindStr));
        }
        String constraintName = pkConstraint.get().getName();
        if (constraint != null && !constraint.getSimple().equals(constraintName)) {
            throw new ValidationException(
                    String.format(
                            "%sThe current %s does not define a primary key constraint named '%s'. "
                                    + "Available constraint name: ['%s'].",
                            exMsgPrefix, tableKindStr, constraint.getSimple(), constraintName));
        }

        return List.of(TableChange.dropConstraint(constraintName));
    }

    public static List<TableChange> validateAndGatherDropPrimaryKey(
            ResolvedCatalogBaseTable<?> oldTable, String exMsgPrefix, String tableKindStr) {
        Optional<UniqueConstraint> pkConstraint = oldTable.getResolvedSchema().getPrimaryKey();

        if (pkConstraint.isEmpty()) {
            throw new ValidationException(
                    String.format(
                            "%sThe current %s does not define any primary key.",
                            exMsgPrefix, tableKindStr));
        }

        return List.of(TableChange.dropConstraint(pkConstraint.get().getName()));
    }

    public static List<TableChange> validateAndGatherDropColumn(
            ResolvedCatalogBaseTable<?> oldTable, Set<String> columnsToDrop, String exMsgPrefix) {
        SchemaReferencesManager referencesManager = SchemaReferencesManager.create(oldTable);
        // Sort by dependencies count from smallest to largest. For example, when dropping
        // column a,
        // b(b as a+1), the order should be: [b, a] after sort.
        Comparator<Object> comparator =
                Comparator.comparingInt(
                                col -> referencesManager.getColumnDependencyCount((String) col))
                        .reversed();
        List<String> sortedColumnsToDrop =
                columnsToDrop.stream().sorted(comparator).collect(Collectors.toList());
        List<TableChange> tableChanges = new ArrayList<>(sortedColumnsToDrop.size());
        for (String columnToDrop : sortedColumnsToDrop) {
            referencesManager.dropColumn(columnToDrop, () -> exMsgPrefix);
            tableChanges.add(TableChange.dropColumn(columnToDrop));
        }

        return tableChanges;
    }

    public static List<ResourceUri> getFunctionResources(List<SqlNode> sqlResources) {
        return sqlResources.stream()
                .map(SqlResource.class::cast)
                .map(
                        sqlResource -> {
                            // get resource type
                            SqlResourceType sqlResourceType =
                                    sqlResource.getResourceType().getValueAs(SqlResourceType.class);
                            ResourceType resourceType;
                            switch (sqlResourceType) {
                                case FILE:
                                    resourceType = ResourceType.FILE;
                                    break;
                                case JAR:
                                    resourceType = ResourceType.JAR;
                                    break;
                                case ARCHIVE:
                                    resourceType = ResourceType.ARCHIVE;
                                    break;
                                default:
                                    throw new ValidationException(
                                            String.format(
                                                    "Unsupported resource type: .",
                                                    sqlResourceType));
                            }
                            // get resource path
                            String path = sqlResource.getResourcePath().getValueAs(String.class);
                            return new ResourceUri(resourceType, path);
                        })
                .collect(Collectors.toList());
    }

    /**
     * Converts language string to the FunctionLanguage.
     *
     * @param languageString the language string from SQL parser
     * @return supported FunctionLanguage otherwise raise UnsupportedOperationException.
     * @throws UnsupportedOperationException if the languageString is not parsable or language is
     *     not supported
     */
    public static FunctionLanguage parseLanguage(String languageString) {
        if (StringUtils.isNullOrWhitespaceOnly(languageString)) {
            return FunctionLanguage.JAVA;
        }

        FunctionLanguage language;
        try {
            language = FunctionLanguage.valueOf(languageString);
        } catch (IllegalArgumentException e) {
            throw new UnsupportedOperationException(
                    String.format("Unrecognized function language string %s", languageString), e);
        }

        return language;
    }
}
