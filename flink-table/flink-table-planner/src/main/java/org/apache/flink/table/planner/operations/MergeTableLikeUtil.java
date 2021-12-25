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
import org.apache.flink.sql.parser.ddl.SqlTableLike;
import org.apache.flink.sql.parser.ddl.SqlTableLike.FeatureOption;
import org.apache.flink.sql.parser.ddl.SqlTableLike.MergingStrategy;
import org.apache.flink.sql.parser.ddl.SqlWatermark;
import org.apache.flink.sql.parser.ddl.constraint.SqlTableConstraint;
import org.apache.flink.table.api.TableColumn;
import org.apache.flink.table.api.TableColumn.ComputedColumn;
import org.apache.flink.table.api.TableColumn.MetadataColumn;
import org.apache.flink.table.api.TableColumn.PhysicalColumn;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.WatermarkSpec;
import org.apache.flink.table.api.constraints.UniqueConstraint;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.utils.TypeConversions;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.validate.SqlValidator;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.flink.table.planner.calcite.FlinkTypeFactory.toLogicalType;
import static org.apache.flink.table.types.utils.TypeConversions.fromLogicalToDataType;

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
        defaultMergingStrategies.put(FeatureOption.PARTITIONS, MergingStrategy.INCLUDING);
    }

    private final SqlValidator validator;
    private final Function<SqlNode, String> escapeExpression;

    MergeTableLikeUtil(SqlValidator validator, Function<SqlNode, String> escapeExpression) {
        this.validator = validator;
        this.escapeExpression = escapeExpression;
    }

    /**
     * Calculates merging strategies for all options. It applies options given by a user to the
     * {@link #defaultMergingStrategies}. The {@link MergingStrategy} specified for {@link
     * FeatureOption#ALL} overwrites all the default options. Those can be further changed with a
     * specific {@link FeatureOption}.
     */
    public Map<FeatureOption, MergingStrategy> computeMergingStrategies(
            List<SqlTableLike.SqlTableLikeOption> mergingOptions) {

        Map<FeatureOption, MergingStrategy> result = new HashMap<>(defaultMergingStrategies);

        Optional<SqlTableLike.SqlTableLikeOption> maybeAllOption =
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

        for (SqlTableLike.SqlTableLikeOption mergingOption : mergingOptions) {
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
    public TableSchema mergeTables(
            Map<FeatureOption, MergingStrategy> mergingStrategies,
            TableSchema sourceSchema,
            List<SqlNode> derivedColumns,
            List<SqlWatermark> derivedWatermarkSpecs,
            SqlTableConstraint derivedPrimaryKey) {

        SchemaBuilder schemaBuilder =
                new SchemaBuilder(
                        mergingStrategies,
                        sourceSchema,
                        (FlinkTypeFactory) validator.getTypeFactory(),
                        validator,
                        escapeExpression);
        schemaBuilder.appendDerivedColumns(mergingStrategies, derivedColumns);
        schemaBuilder.appendDerivedWatermarks(mergingStrategies, derivedWatermarkSpecs);
        schemaBuilder.appendDerivedPrimaryKey(derivedPrimaryKey);

        return schemaBuilder.build();
    }

    /**
     * Merges the partitions part of {@code CREATE TABLE} statement.
     *
     * <p>Partitioning is a single property of a Table, thus there can be at most a single instance
     * of partitioning. Therefore it is not possible to use {@link MergingStrategy#INCLUDING} with
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

    private static class SchemaBuilder {

        Map<String, TableColumn> columns = new LinkedHashMap<>();
        Map<String, WatermarkSpec> watermarkSpecs = new HashMap<>();
        UniqueConstraint primaryKey = null;

        // Intermediate state
        Map<String, RelDataType> physicalFieldNamesToTypes = new LinkedHashMap<>();
        Map<String, RelDataType> metadataFieldNamesToTypes = new LinkedHashMap<>();
        Map<String, RelDataType> computedFieldNamesToTypes = new LinkedHashMap<>();

        Function<SqlNode, String> escapeExpressions;
        FlinkTypeFactory typeFactory;
        SqlValidator sqlValidator;

        SchemaBuilder(
                Map<FeatureOption, MergingStrategy> mergingStrategies,
                TableSchema sourceSchema,
                FlinkTypeFactory typeFactory,
                SqlValidator sqlValidator,
                Function<SqlNode, String> escapeExpressions) {
            this.typeFactory = typeFactory;
            this.sqlValidator = sqlValidator;
            this.escapeExpressions = escapeExpressions;
            populateColumnsFromSourceTable(mergingStrategies, sourceSchema);
            populateWatermarksFromSourceTable(mergingStrategies, sourceSchema);
            populatePrimaryKeyFromSourceTable(mergingStrategies, sourceSchema);
        }

        private void populateColumnsFromSourceTable(
                Map<FeatureOption, MergingStrategy> mergingStrategies, TableSchema sourceSchema) {
            for (TableColumn sourceColumn : sourceSchema.getTableColumns()) {
                if (sourceColumn instanceof PhysicalColumn) {
                    physicalFieldNamesToTypes.put(
                            sourceColumn.getName(),
                            typeFactory.createFieldTypeFromLogicalType(
                                    sourceColumn.getType().getLogicalType()));
                    columns.put(sourceColumn.getName(), sourceColumn);
                } else if (sourceColumn instanceof ComputedColumn) {
                    if (mergingStrategies.get(FeatureOption.GENERATED)
                            != MergingStrategy.EXCLUDING) {
                        columns.put(sourceColumn.getName(), sourceColumn);
                    }
                } else if (sourceColumn instanceof MetadataColumn) {
                    if (mergingStrategies.get(FeatureOption.METADATA)
                            != MergingStrategy.EXCLUDING) {
                        columns.put(sourceColumn.getName(), sourceColumn);
                    }
                }
            }
        }

        private void populateWatermarksFromSourceTable(
                Map<FeatureOption, MergingStrategy> mergingStrategies, TableSchema sourceSchema) {
            for (WatermarkSpec sourceWatermarkSpec : sourceSchema.getWatermarkSpecs()) {
                if (mergingStrategies.get(FeatureOption.WATERMARKS) != MergingStrategy.EXCLUDING) {
                    watermarkSpecs.put(
                            sourceWatermarkSpec.getRowtimeAttribute(), sourceWatermarkSpec);
                }
            }
        }

        private void populatePrimaryKeyFromSourceTable(
                Map<FeatureOption, MergingStrategy> mergingStrategies, TableSchema sourceSchema) {
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
                List<String> primaryKeyColumns = new ArrayList<>();
                for (SqlNode primaryKeyNode : derivedPrimaryKey.getColumns()) {
                    String primaryKey = ((SqlIdentifier) primaryKeyNode).getSimple();
                    if (!columns.containsKey(primaryKey)) {
                        throw new ValidationException(
                                String.format(
                                        "Primary key column '%s' is not defined in the schema at %s",
                                        primaryKey, primaryKeyNode.getParserPosition()));
                    }
                    if (!columns.get(primaryKey).isPhysical()) {
                        throw new ValidationException(
                                String.format(
                                        "Could not create a PRIMARY KEY with column '%s' at %s.\n"
                                                + "A PRIMARY KEY constraint must be declared on physical columns.",
                                        primaryKey, primaryKeyNode.getParserPosition()));
                    }
                    primaryKeyColumns.add(primaryKey);
                }
                primaryKey =
                        UniqueConstraint.primaryKey(
                                derivedPrimaryKey
                                        .getConstraintName()
                                        .orElseGet(() -> "PK_" + primaryKeyColumns.hashCode()),
                                primaryKeyColumns);
            }
        }

        private void appendDerivedWatermarks(
                Map<FeatureOption, MergingStrategy> mergingStrategies,
                List<SqlWatermark> derivedWatermarkSpecs) {
            for (SqlWatermark derivedWatermarkSpec : derivedWatermarkSpecs) {
                SqlIdentifier eventTimeColumnName = derivedWatermarkSpec.getEventTimeColumnName();

                HashMap<String, RelDataType> nameToTypeMap = new LinkedHashMap<>();
                nameToTypeMap.putAll(physicalFieldNamesToTypes);
                nameToTypeMap.putAll(metadataFieldNamesToTypes);
                nameToTypeMap.putAll(computedFieldNamesToTypes);

                verifyRowtimeAttribute(mergingStrategies, eventTimeColumnName, nameToTypeMap);
                String rowtimeAttribute = eventTimeColumnName.toString();

                SqlNode expression = derivedWatermarkSpec.getWatermarkStrategy();

                // this will validate and expand function identifiers.
                SqlNode validated =
                        sqlValidator.validateParameterizedExpression(expression, nameToTypeMap);
                RelDataType validatedType = sqlValidator.getValidatedNodeType(validated);
                DataType exprDataType = fromLogicalToDataType(toLogicalType(validatedType));

                watermarkSpecs.put(
                        rowtimeAttribute,
                        new WatermarkSpec(
                                rowtimeAttribute,
                                escapeExpressions.apply(validated),
                                exprDataType));
            }
        }

        private void verifyRowtimeAttribute(
                Map<FeatureOption, MergingStrategy> mergingStrategies,
                SqlIdentifier eventTimeColumnName,
                Map<String, RelDataType> allFieldsTypes) {
            String fullRowtimeExpression = eventTimeColumnName.toString();
            boolean specAlreadyExists = watermarkSpecs.containsKey(fullRowtimeExpression);

            if (specAlreadyExists
                    && mergingStrategies.get(FeatureOption.WATERMARKS)
                            != MergingStrategy.OVERWRITING) {
                throw new ValidationException(
                        String.format(
                                "There already exists a watermark spec for column '%s' in the base table. You "
                                        + "might want to specify EXCLUDING WATERMARKS or OVERWRITING WATERMARKS.",
                                fullRowtimeExpression));
            }

            List<String> components = eventTimeColumnName.names;
            if (!allFieldsTypes.containsKey(components.get(0))) {
                throw new ValidationException(
                        String.format(
                                "The rowtime attribute field '%s' is not defined in the table schema, at %s\n"
                                        + "Available fields: [%s]",
                                fullRowtimeExpression,
                                eventTimeColumnName.getParserPosition(),
                                allFieldsTypes.keySet().stream()
                                        .collect(Collectors.joining("', '", "'", "'"))));
            }

            if (components.size() > 1) {
                RelDataType componentType = allFieldsTypes.get(components.get(0));
                for (int i = 1; i < components.size(); i++) {
                    RelDataTypeField field = componentType.getField(components.get(i), true, false);
                    if (field == null) {
                        throw new ValidationException(
                                String.format(
                                        "The rowtime attribute field '%s' is not defined in the table schema, at %s\n"
                                                + "Nested field '%s' was not found in a composite type: %s.",
                                        fullRowtimeExpression,
                                        eventTimeColumnName.getComponent(i).getParserPosition(),
                                        components.get(i),
                                        FlinkTypeFactory.toLogicalType(
                                                allFieldsTypes.get(components.get(0)))));
                    }
                    componentType = field.getType();
                }
            }
        }

        private void appendDerivedColumns(
                Map<FeatureOption, MergingStrategy> mergingStrategies,
                List<SqlNode> derivedColumns) {

            collectPhysicalFieldsTypes(derivedColumns);

            for (SqlNode derivedColumn : derivedColumns) {
                final String name = ((SqlTableColumn) derivedColumn).getName().getSimple();
                final TableColumn column;
                if (derivedColumn instanceof SqlRegularColumn) {
                    final LogicalType logicalType =
                            FlinkTypeFactory.toLogicalType(physicalFieldNamesToTypes.get(name));
                    column =
                            TableColumn.physical(
                                    name, TypeConversions.fromLogicalToDataType(logicalType));
                } else if (derivedColumn instanceof SqlComputedColumn) {
                    final SqlComputedColumn computedColumn = (SqlComputedColumn) derivedColumn;
                    if (columns.containsKey(name)) {
                        if (!(columns.get(name) instanceof ComputedColumn)) {
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
                            TableColumn.computed(
                                    name,
                                    fromLogicalToDataType(toLogicalType(validatedType)),
                                    escapeExpressions.apply(validatedExpr));
                    computedFieldNamesToTypes.put(name, validatedType);
                } else if (derivedColumn instanceof SqlMetadataColumn) {
                    final SqlMetadataColumn metadataColumn = (SqlMetadataColumn) derivedColumn;
                    if (columns.containsKey(name)) {
                        if (!(columns.get(name) instanceof MetadataColumn)) {
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

                    SqlDataTypeSpec type = metadataColumn.getType();
                    boolean nullable = type.getNullable() == null ? true : type.getNullable();
                    RelDataType relType = type.deriveType(sqlValidator, nullable);
                    column =
                            TableColumn.metadata(
                                    name,
                                    fromLogicalToDataType(toLogicalType(relType)),
                                    metadataColumn.getMetadataAlias().orElse(null),
                                    metadataColumn.isVirtual());
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
                    SqlDataTypeSpec type = regularColumn.getType();
                    boolean nullable = type.getNullable() == null ? true : type.getNullable();
                    RelDataType relType = type.deriveType(sqlValidator, nullable);
                    // add field name and field type to physical field list
                    physicalFieldNamesToTypes.put(name, relType);
                }
            }
        }

        public TableSchema build() {
            TableSchema.Builder resultBuilder = TableSchema.builder();
            for (TableColumn column : columns.values()) {
                resultBuilder.add(column);
            }
            for (WatermarkSpec watermarkSpec : watermarkSpecs.values()) {
                resultBuilder.watermark(watermarkSpec);
            }
            if (primaryKey != null) {
                resultBuilder.primaryKey(
                        primaryKey.getName(), primaryKey.getColumns().toArray(new String[0]));
            }
            return resultBuilder.build();
        }
    }
}
