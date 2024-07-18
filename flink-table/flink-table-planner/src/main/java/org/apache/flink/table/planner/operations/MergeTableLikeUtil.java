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

package org.apache.flink.table.planner.operations;

import org.apache.flink.sql.parser.ddl.SqlCreateTable;
import org.apache.flink.sql.parser.ddl.SqlTableColumn;
import org.apache.flink.sql.parser.ddl.SqlTableColumn.SqlComputedColumn;
import org.apache.flink.sql.parser.ddl.SqlTableColumn.SqlMetadataColumn;
import org.apache.flink.sql.parser.ddl.SqlTableColumn.SqlRegularColumn;
import org.apache.flink.sql.parser.ddl.SqlTableLike.FeatureOption;
import org.apache.flink.sql.parser.ddl.SqlTableLike.MergingStrategy;
import org.apache.flink.sql.parser.ddl.SqlTableLike.SqlTableLikeOption;
import org.apache.flink.sql.parser.ddl.SqlWatermark;
import org.apache.flink.sql.parser.ddl.constraint.SqlTableConstraint;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Schema.UnresolvedColumn;
import org.apache.flink.table.api.Schema.UnresolvedComputedColumn;
import org.apache.flink.table.api.Schema.UnresolvedMetadataColumn;
import org.apache.flink.table.api.Schema.UnresolvedPhysicalColumn;
import org.apache.flink.table.api.Schema.UnresolvedWatermarkSpec;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.catalog.TableDistribution;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.types.logical.LogicalType;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.validate.SqlValidator;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

/** A utility class with logic for handling the {@code CREATE TABLE ... LIKE} clause. */
class MergeTableLikeUtil {
    /** Default merging strategy if given option was not provided explicitly by the user. */
    private static final HashMap<FeatureOption, MergingStrategy> defaultMergingStrategies =
            new HashMap<>();

    static {
        defaultMergingStrategies.put(FeatureOption.OPTIONS, MergingStrategy.OVERWRITING);
        defaultMergingStrategies.put(FeatureOption.WATERMARKS, MergingStrategy.INCLUDING);
        defaultMergingStrategies.put(FeatureOption.GENERATED, MergingStrategy.INCLUDING);
        defaultMergingStrategies.put(FeatureOption.METADATA, MergingStrategy.INCLUDING);
        defaultMergingStrategies.put(FeatureOption.CONSTRAINTS, MergingStrategy.INCLUDING);
        defaultMergingStrategies.put(FeatureOption.DISTRIBUTION, MergingStrategy.INCLUDING);
        defaultMergingStrategies.put(FeatureOption.PARTITIONS, MergingStrategy.INCLUDING);
    }

    private final SqlValidator validator;
    private final Function<SqlNode, String> escapeExpression;
    private final DataTypeFactory dataTypeFactory;

    MergeTableLikeUtil(
            SqlValidator validator,
            Function<SqlNode, String> escapeExpression,
            DataTypeFactory dataTypeFactory) {
        this.validator = validator;
        this.escapeExpression = escapeExpression;
        this.dataTypeFactory = dataTypeFactory;
    }

    /**
     * Calculates merging strategies for all options. It applies options given by a user to the
     * {@link #defaultMergingStrategies}. The {@link MergingStrategy} specified for {@link
     * FeatureOption#ALL} overwrites all the default options. Those can be further changed with a
     * specific {@link FeatureOption}.
     */
    public Map<FeatureOption, MergingStrategy> computeMergingStrategies(
            List<SqlTableLikeOption> mergingOptions) {

        Map<FeatureOption, MergingStrategy> result = new HashMap<>(defaultMergingStrategies);

        Optional<SqlTableLikeOption> maybeAllOption =
                mergingOptions.stream()
                        .filter(option -> option.getFeatureOption() == FeatureOption.ALL)
                        .findFirst();

        maybeAllOption.ifPresent(
                (allOption) -> {
                    MergingStrategy strategy = allOption.getMergingStrategy();
                    for (FeatureOption featureOption : FeatureOption.values()) {
                        if (featureOption != FeatureOption.ALL) {
                            result.put(featureOption, strategy);
                        }
                    }
                });

        for (SqlTableLikeOption mergingOption : mergingOptions) {
            result.put(mergingOption.getFeatureOption(), mergingOption.getMergingStrategy());
        }

        return result;
    }

    /**
     * Merges the schema part of {@code CREATE TABLE} statement. It merges
     *
     * <ul>
     *   <li>columns
     *   <li>computed columns
     *   <li>watermarks
     *   <li>primary key
     * </ul>
     *
     * <p>Additionally it performs validation of the features of the derived table. This is not done
     * in the {@link SqlCreateTable#validate()} anymore because the validation should be done on top
     * of the merged properties. E.g. Some of the columns used in computed columns of the derived
     * table can be defined in the source table.
     */
    public Schema mergeTables(
            Map<FeatureOption, MergingStrategy> mergingStrategies,
            Schema sourceSchema,
            List<SqlNode> derivedColumns,
            List<SqlWatermark> derivedWatermarkSpecs,
            SqlTableConstraint derivedPrimaryKey) {

        SchemaBuilder schemaBuilder =
                new SchemaBuilder(
                        mergingStrategies,
                        sourceSchema,
                        (FlinkTypeFactory) validator.getTypeFactory(),
                        dataTypeFactory,
                        validator,
                        escapeExpression);
        schemaBuilder.appendDerivedColumns(mergingStrategies, derivedColumns);
        schemaBuilder.appendDerivedWatermarks(mergingStrategies, derivedWatermarkSpecs);
        schemaBuilder.appendDerivedPrimaryKey(derivedPrimaryKey);

        return schemaBuilder.build();
    }

    /**
     * Merges the distribution part of {@code CREATE TABLE} statement.
     *
     * <p>Distribution is a single property of a Table, thus there can be at most a single instance
     * of it. Therefore, it is not possible to use {@link MergingStrategy#INCLUDING} with a
     * distribution defined in both source and derived table.
     */
    public Optional<TableDistribution> mergeDistribution(
            MergingStrategy mergingStrategy,
            Optional<TableDistribution> sourceTableDistribution,
            Optional<TableDistribution> derivedTabledDistribution) {

        if (derivedTabledDistribution.isPresent()
                && sourceTableDistribution.isPresent()
                && mergingStrategy != MergingStrategy.EXCLUDING) {
            throw new ValidationException(
                    "The base table already has a distribution defined. You might want to specify "
                            + "EXCLUDING DISTRIBUTION.");
        }

        if (derivedTabledDistribution.isPresent()) {
            return derivedTabledDistribution;
        }
        return sourceTableDistribution;
    }

    /**
     * Merges the partitions part of {@code CREATE TABLE} statement.
     *
     * <p>Partitioning is a single property of a Table, thus there can be at most a single instance
     * of partitioning. Therefore, it is not possible to use {@link MergingStrategy#INCLUDING} with
     * partitioning defined in both source and derived table.
     */
    public List<String> mergePartitions(
            MergingStrategy mergingStrategy,
            List<String> sourcePartitions,
            List<String> derivedPartitions) {

        if (!derivedPartitions.isEmpty()
                && !sourcePartitions.isEmpty()
                && mergingStrategy != MergingStrategy.EXCLUDING) {
            throw new ValidationException(
                    "The base table already has partitions defined. You might want to specify "
                            + "EXCLUDING PARTITIONS.");
        }

        if (!derivedPartitions.isEmpty()) {
            return derivedPartitions;
        }
        return sourcePartitions;
    }

    /** Merges the options part of {@code CREATE TABLE} statement. */
    public Map<String, String> mergeOptions(
            MergingStrategy mergingStrategy,
            Map<String, String> sourceOptions,
            Map<String, String> derivedOptions) {
        Map<String, String> options = new HashMap<>();
        if (mergingStrategy != MergingStrategy.EXCLUDING) {
            options.putAll(sourceOptions);
        }

        derivedOptions.forEach(
                (key, value) -> {
                    if (mergingStrategy != MergingStrategy.OVERWRITING
                            && options.containsKey(key)) {
                        throw new ValidationException(
                                String.format(
                                        "There already exists an option ['%s' -> '%s']  in the "
                                                + "base table. You might want to specify EXCLUDING OPTIONS or OVERWRITING OPTIONS.",
                                        key, options.get(key)));
                    }

                    options.put(key, value);
                });
        return options;
    }

    private static class SchemaBuilder extends SchemaBuilderUtil {
        // Intermediate state
        Map<String, RelDataType> physicalFieldNamesToTypes = new LinkedHashMap<>();
        Map<String, RelDataType> metadataFieldNamesToTypes = new LinkedHashMap<>();
        Map<String, RelDataType> computedFieldNamesToTypes = new LinkedHashMap<>();

        FlinkTypeFactory typeFactory;

        SchemaBuilder(
                Map<FeatureOption, MergingStrategy> mergingStrategies,
                Schema sourceSchema,
                FlinkTypeFactory typeFactory,
                DataTypeFactory dataTypeFactory,
                SqlValidator sqlValidator,
                Function<SqlNode, String> escapeExpressions) {
            super(sqlValidator, escapeExpressions, dataTypeFactory);
            this.typeFactory = typeFactory;

            populateColumnsFromSourceTable(mergingStrategies, sourceSchema);
            populateWatermarksFromSourceTable(mergingStrategies, sourceSchema);
            populatePrimaryKeyFromSourceTable(mergingStrategies, sourceSchema);
        }

        private void populateColumnsFromSourceTable(
                Map<FeatureOption, MergingStrategy> mergingStrategies, Schema sourceSchema) {
            for (UnresolvedColumn sourceColumn : sourceSchema.getColumns()) {
                if (sourceColumn instanceof UnresolvedPhysicalColumn) {
                    LogicalType columnType =
                            getLogicalType((UnresolvedPhysicalColumn) sourceColumn);
                    physicalFieldNamesToTypes.put(
                            sourceColumn.getName(),
                            typeFactory.createFieldTypeFromLogicalType(columnType));
                    columns.put(sourceColumn.getName(), sourceColumn);
                } else if (sourceColumn instanceof UnresolvedComputedColumn) {
                    if (mergingStrategies.get(FeatureOption.GENERATED)
                            != MergingStrategy.EXCLUDING) {
                        columns.put(sourceColumn.getName(), sourceColumn);
                    }
                } else if (sourceColumn instanceof UnresolvedMetadataColumn) {
                    if (mergingStrategies.get(FeatureOption.METADATA)
                            != MergingStrategy.EXCLUDING) {
                        columns.put(sourceColumn.getName(), sourceColumn);
                    }
                }
            }
        }

        private void populateWatermarksFromSourceTable(
                Map<FeatureOption, MergingStrategy> mergingStrategies, Schema sourceSchema) {
            for (UnresolvedWatermarkSpec sourceWatermarkSpec : sourceSchema.getWatermarkSpecs()) {
                if (mergingStrategies.get(FeatureOption.WATERMARKS) != MergingStrategy.EXCLUDING) {
                    watermarkSpecs.put(sourceWatermarkSpec.getColumnName(), sourceWatermarkSpec);
                }
            }
        }

        private void populatePrimaryKeyFromSourceTable(
                Map<FeatureOption, MergingStrategy> mergingStrategies, Schema sourceSchema) {
            if (sourceSchema.getPrimaryKey().isPresent()
                    && mergingStrategies.get(FeatureOption.CONSTRAINTS)
                            == MergingStrategy.INCLUDING) {
                primaryKey = sourceSchema.getPrimaryKey().get();
            }
        }

        private void appendDerivedPrimaryKey(@Nullable SqlTableConstraint derivedPrimaryKey) {
            if (derivedPrimaryKey != null && primaryKey != null) {
                throw new ValidationException(
                        "The base table already has a primary key. You might "
                                + "want to specify EXCLUDING CONSTRAINTS.");
            } else if (derivedPrimaryKey != null) {
                setPrimaryKey(derivedPrimaryKey);
            }
        }

        private void appendDerivedWatermarks(
                Map<FeatureOption, MergingStrategy> mergingStrategies,
                List<SqlWatermark> derivedWatermarkSpecs) {
            if (mergingStrategies.get(FeatureOption.WATERMARKS) != MergingStrategy.OVERWRITING) {
                for (SqlWatermark derivedWatermarkSpec : derivedWatermarkSpecs) {
                    SqlIdentifier eventTimeColumnName =
                            derivedWatermarkSpec.getEventTimeColumnName();
                    String rowtimeAttribute = eventTimeColumnName.toString();

                    if (watermarkSpecs.containsKey(rowtimeAttribute)) {
                        throw new ValidationException(
                                String.format(
                                        "There already exists a watermark spec for column '%s' in the base table. You "
                                                + "might want to specify EXCLUDING WATERMARKS or OVERWRITING WATERMARKS.",
                                        rowtimeAttribute));
                    }
                }
            }

            HashMap<String, RelDataType> nameToTypeMap = new LinkedHashMap<>();
            nameToTypeMap.putAll(physicalFieldNamesToTypes);
            nameToTypeMap.putAll(metadataFieldNamesToTypes);
            nameToTypeMap.putAll(computedFieldNamesToTypes);

            addWatermarks(derivedWatermarkSpecs, nameToTypeMap, true);
        }

        private void appendDerivedColumns(
                Map<FeatureOption, MergingStrategy> mergingStrategies,
                List<SqlNode> derivedColumns) {

            collectPhysicalFieldsTypes(derivedColumns);

            for (SqlNode derivedColumn : derivedColumns) {
                final String name = ((SqlTableColumn) derivedColumn).getName().getSimple();

                final UnresolvedColumn column;
                if (derivedColumn instanceof SqlRegularColumn) {
                    column = toUnresolvedPhysicalColumn((SqlRegularColumn) derivedColumn);
                } else if (derivedColumn instanceof SqlComputedColumn) {
                    final SqlComputedColumn computedColumn = (SqlComputedColumn) derivedColumn;
                    if (physicalFieldNamesToTypes.containsKey(name)) {
                        throw new ValidationException(
                                String.format(
                                        "A column named '%s' already exists in the table. "
                                                + "Duplicate columns exist in the compute column and regular column. ",
                                        name));
                    }
                    if (columns.containsKey(name)) {
                        if (!(columns.get(name) instanceof UnresolvedComputedColumn)) {
                            throw new ValidationException(
                                    String.format(
                                            "A column named '%s' already exists in the base table. "
                                                    + "Computed columns can only overwrite other computed columns.",
                                            name));
                        }

                        if (mergingStrategies.get(FeatureOption.GENERATED)
                                != MergingStrategy.OVERWRITING) {
                            throw new ValidationException(
                                    String.format(
                                            "A generated column named '%s' already exists in the base table. You "
                                                    + "might want to specify EXCLUDING GENERATED or OVERWRITING GENERATED.",
                                            name));
                        }
                    }

                    final Map<String, RelDataType> accessibleFieldNamesToTypes = new HashMap<>();
                    accessibleFieldNamesToTypes.putAll(physicalFieldNamesToTypes);
                    accessibleFieldNamesToTypes.putAll(metadataFieldNamesToTypes);

                    final SqlNode validatedExpr =
                            sqlValidator.validateParameterizedExpression(
                                    computedColumn.getExpr(), accessibleFieldNamesToTypes);
                    final RelDataType validatedType =
                            sqlValidator.getValidatedNodeType(validatedExpr);
                    column =
                            toUnresolvedComputedColumn(
                                    (SqlComputedColumn) derivedColumn, validatedExpr);
                    computedFieldNamesToTypes.put(name, validatedType);
                } else if (derivedColumn instanceof SqlMetadataColumn) {
                    final SqlMetadataColumn metadataColumn = (SqlMetadataColumn) derivedColumn;
                    if (physicalFieldNamesToTypes.containsKey(name)) {
                        throw new ValidationException(
                                String.format(
                                        "A column named '%s' already exists in the table. "
                                                + "Duplicate columns exist in the metadata column and regular column. ",
                                        name));
                    }
                    if (columns.containsKey(name)) {
                        if (!(columns.get(name) instanceof UnresolvedMetadataColumn)) {
                            throw new ValidationException(
                                    String.format(
                                            "A column named '%s' already exists in the base table. "
                                                    + "Metadata columns can only overwrite other metadata columns.",
                                            name));
                        }

                        if (mergingStrategies.get(FeatureOption.METADATA)
                                != MergingStrategy.OVERWRITING) {
                            throw new ValidationException(
                                    String.format(
                                            "A metadata column named '%s' already exists in the base table. You "
                                                    + "might want to specify EXCLUDING METADATA or OVERWRITING METADATA.",
                                            name));
                        }
                    }

                    RelDataType relType = toRelDataType(metadataColumn.getType());
                    column = toUnresolvedMetadataColumn((SqlMetadataColumn) derivedColumn);
                    metadataFieldNamesToTypes.put(name, relType);
                } else {
                    throw new ValidationException("Unsupported column type: " + derivedColumn);
                }
                columns.put(column.getName(), column);
            }
        }

        private void collectPhysicalFieldsTypes(List<SqlNode> derivedColumns) {
            for (SqlNode derivedColumn : derivedColumns) {
                if (derivedColumn instanceof SqlRegularColumn) {
                    SqlRegularColumn regularColumn = (SqlRegularColumn) derivedColumn;
                    String name = regularColumn.getName().getSimple();
                    if (columns.containsKey(name)) {
                        throw new ValidationException(
                                String.format(
                                        "A column named '%s' already exists in the base table.",
                                        name));
                    }
                    RelDataType relType = toRelDataType(regularColumn.getType());
                    // add field name and field type to physical field list
                    RelDataType oldType = physicalFieldNamesToTypes.put(name, relType);
                    if (oldType != null) {
                        throw new ValidationException(
                                String.format(
                                        "A regular Column named '%s' already exists in the table.",
                                        name));
                    }
                }
            }
        }
    }
}
