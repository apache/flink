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
import org.apache.flink.sql.parser.ddl.SqlTableLike;
import org.apache.flink.sql.parser.ddl.SqlTableLike.FeatureOption;
import org.apache.flink.sql.parser.ddl.SqlTableLike.MergingStrategy;
import org.apache.flink.sql.parser.ddl.SqlWatermark;
import org.apache.flink.sql.parser.ddl.constraint.SqlTableConstraint;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Schema.UnresolvedColumn;
import org.apache.flink.table.api.Schema.UnresolvedComputedColumn;
import org.apache.flink.table.api.Schema.UnresolvedMetadataColumn;
import org.apache.flink.table.api.Schema.UnresolvedPhysicalColumn;
import org.apache.flink.table.api.Schema.UnresolvedPrimaryKey;
import org.apache.flink.table.api.Schema.UnresolvedWatermarkSpec;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.types.logical.LogicalType;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.validate.SqlValidator;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

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
    public Schema mergeTables(
            Map<FeatureOption, MergingStrategy> mergingStrategies,
            Schema sourceSchema,
            List<SqlNode> derivedColumns,
            List<SqlWatermark> derivedWatermarkSpecs,
            @Nullable SqlTableConstraint derivedPrimaryKey) {

        SchemaBuilder schemaBuilder =
                new SchemaBuilder(
                        mergingStrategies,
                        sourceSchema,
                        (FlinkTypeFactory) validator.getTypeFactory(),
                        validator,
                        escapeExpression);

        Schema derivedSchema =
                buildDerivedSchema(derivedColumns, derivedWatermarkSpecs, derivedPrimaryKey);
        schemaBuilder.appendDerivedColumns(mergingStrategies, derivedSchema.getColumns());
        schemaBuilder.appendDerivedWatermarks(mergingStrategies, derivedSchema.getWatermarkSpecs());
        schemaBuilder.appendDerivedPrimaryKey(derivedSchema.getPrimaryKey().orElse(null));

        return schemaBuilder.build();
    }

    private Schema buildDerivedSchema(
            List<SqlNode> derivedColumns,
            List<SqlWatermark> derivedWatermarkSpecs,
            SqlTableConstraint derivedPrimaryKey) {
        Schema.Builder builder = Schema.newBuilder();

        // build column
        for (SqlNode derivedColumn : derivedColumns) {
            final String name = ((SqlTableColumn) derivedColumn).getName().getSimple();
            if (derivedColumn instanceof SqlTableColumn.SqlRegularColumn) {
                final SqlTableColumn.SqlRegularColumn regularColumn =
                        (SqlTableColumn.SqlRegularColumn) derivedColumn;
                SqlDataTypeSpec type = regularColumn.getType();
                boolean nullable = type.getNullable() == null ? true : type.getNullable();
                RelDataType relType = type.deriveType(validator, nullable);
                LogicalType logicalType = FlinkTypeFactory.toLogicalType(relType);
                builder.column(name, fromLogicalToDataType(logicalType));
            } else if (derivedColumn instanceof SqlTableColumn.SqlComputedColumn) {
                final SqlTableColumn.SqlComputedColumn computedColumn =
                        (SqlTableColumn.SqlComputedColumn) derivedColumn;
                builder.columnByExpression(name, escapeExpression.apply(computedColumn.getExpr()));
            } else if (derivedColumn instanceof SqlTableColumn.SqlMetadataColumn) {
                final SqlTableColumn.SqlMetadataColumn metadataColumn =
                        (SqlTableColumn.SqlMetadataColumn) derivedColumn;
                SqlDataTypeSpec type = metadataColumn.getType();
                boolean nullable = type.getNullable() == null ? true : type.getNullable();
                RelDataType relType = type.deriveType(validator, nullable);
                LogicalType logicalType = FlinkTypeFactory.toLogicalType(relType);
                builder.columnByMetadata(
                        name,
                        fromLogicalToDataType(logicalType),
                        metadataColumn.getMetadataAlias().orElse(null),
                        metadataColumn.isVirtual());
            } else {
                throw new ValidationException("Unsupported column type: " + derivedColumn);
            }
        }

        // build watermark
        for (SqlWatermark sqlWatermark : derivedWatermarkSpecs) {
            builder.watermark(
                    sqlWatermark.getEventTimeColumnName().toString(),
                    escapeExpression.apply(sqlWatermark.getWatermarkStrategy()));
        }

        // build primary key
        if (derivedPrimaryKey != null) {
            List<String> columnNames = Arrays.asList(derivedPrimaryKey.getColumnNames());
            String primaryKeyName =
                    derivedPrimaryKey
                            .getConstraintName()
                            .orElseGet(() -> "PK_" + columnNames.hashCode());
            builder.primaryKeyNamed(primaryKeyName, columnNames);
        }
        return builder.build();
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

        Map<String, UnresolvedColumn> columns = new LinkedHashMap<>();
        Map<String, UnresolvedWatermarkSpec> watermarkSpecs = new HashMap<>();
        UnresolvedPrimaryKey primaryKey = null;

        // Intermediate state
        //        Map<String, RelDataType> physicalFieldNamesToTypes = new LinkedHashMap<>();
        //        Map<String, RelDataType> metadataFieldNamesToTypes = new LinkedHashMap<>();
        //        Map<String, RelDataType> computedFieldNamesToTypes = new LinkedHashMap<>();

        Function<SqlNode, String> escapeExpressions;
        FlinkTypeFactory typeFactory;
        SqlValidator sqlValidator;

        SchemaBuilder(
                Map<FeatureOption, MergingStrategy> mergingStrategies,
                Schema sourceSchema,
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
                Map<FeatureOption, MergingStrategy> mergingStrategies, Schema sourceSchema) {
            for (UnresolvedColumn sourceColumn : sourceSchema.getColumns()) {
                if (sourceColumn instanceof UnresolvedPhysicalColumn) {
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
            for (Schema.UnresolvedWatermarkSpec sourceWatermarkSpec :
                    sourceSchema.getWatermarkSpecs()) {
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

        private void appendDerivedPrimaryKey(@Nullable UnresolvedPrimaryKey derivedPrimaryKey) {
            if (derivedPrimaryKey != null && primaryKey != null) {
                throw new ValidationException(
                        "The base table already has a primary key. You might "
                                + "want to specify EXCLUDING CONSTRAINTS.");
            } else if (derivedPrimaryKey != null) {
                List<String> primaryKeyColumns = new ArrayList<>();
                for (String primaryKey : derivedPrimaryKey.getColumnNames()) {
                    if (!columns.containsKey(primaryKey)) {
                        throw new ValidationException(
                                String.format(
                                        "Primary key column '%s' is not defined in the schema",
                                        primaryKey));
                    }
                    if (!(columns.get(primaryKey) instanceof UnresolvedPhysicalColumn)) {
                        throw new ValidationException(
                                String.format(
                                        "Could not create a PRIMARY KEY with column '%s'.\n"
                                                + "A PRIMARY KEY constraint must be declared on physical columns.",
                                        primaryKey));
                    }
                    primaryKeyColumns.add(primaryKey);
                }
                primaryKey = derivedPrimaryKey;
            }
        }

        private void appendDerivedWatermarks(
                Map<FeatureOption, MergingStrategy> mergingStrategies,
                List<UnresolvedWatermarkSpec> derivedWatermarkSpecs) {
            for (UnresolvedWatermarkSpec derivedWatermarkSpec : derivedWatermarkSpecs) {
                String eventTimeColumnName = derivedWatermarkSpec.getColumnName();
                verifyRowtimeAttribute(mergingStrategies, eventTimeColumnName);
                String rowtimeAttribute = eventTimeColumnName;

                watermarkSpecs.put(rowtimeAttribute, derivedWatermarkSpec);
            }
        }

        private void verifyRowtimeAttribute(
                Map<FeatureOption, MergingStrategy> mergingStrategies, String eventTimeColumnName) {
            String fullRowtimeExpression = eventTimeColumnName;
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

            List<String> components = Arrays.asList(eventTimeColumnName.split("\\."));
            if (!columns.containsKey(components.get(0))) {
                throw new ValidationException(
                        String.format(
                                "The rowtime attribute field '%s' is not defined in the table schema, \n"
                                        + "Available fields: [%s]",
                                fullRowtimeExpression,
                                columns.keySet().stream()
                                        .collect(Collectors.joining("', '", "'", "'"))));
            }

            //            if (components.size() > 1) {
            //                RelDataType componentType = allFieldsTypes.get(components.get(0));
            //                for (int i = 1; i < components.size(); i++) {
            //                    RelDataTypeField field = componentType.getField(components.get(i),
            // true, false);
            //                    if (field == null) {
            //                        throw new ValidationException(
            //                                String.format(
            //                                        "The rowtime attribute field '%s' is not
            // defined in the table schema, at %s\n"
            //                                                + "Nested field '%s' was not found in
            // a composite type: %s.",
            //                                        fullRowtimeExpression,
            //
            // eventTimeColumnName.getComponent(i).getParserPosition(),
            //                                        components.get(i),
            //                                        FlinkTypeFactory.toLogicalType(
            //
            // allFieldsTypes.get(components.get(0)))));
            //                    }
            //                    componentType = field.getType();
            //                }
            //            }
        }

        private void appendDerivedColumns(
                Map<FeatureOption, MergingStrategy> mergingStrategies,
                List<UnresolvedColumn> derivedColumns) {

            for (UnresolvedColumn derivedColumn : derivedColumns) {
                final String name = derivedColumn.getName();
                if (derivedColumn instanceof UnresolvedPhysicalColumn) {
                    if (columns.containsKey(name)) {
                        throw new ValidationException(
                                String.format(
                                        "A column named '%s' already exists in the base table. "
                                                + "Computed columns can only overwrite other computed columns.",
                                        name));
                    }
                } else if (derivedColumn instanceof UnresolvedComputedColumn) {
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
                } else if (derivedColumn instanceof UnresolvedMetadataColumn) {
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
                } else {
                    throw new ValidationException("Unsupported column type: " + derivedColumn);
                }
                columns.put(derivedColumn.getName(), derivedColumn);
            }
        }

        public Schema build() {
            Schema.Builder resultBuilder = Schema.newBuilder();
            resultBuilder.fromColumns(new ArrayList<>(columns.values()));

            for (UnresolvedWatermarkSpec watermarkSpec : watermarkSpecs.values()) {
                resultBuilder.watermark(
                        watermarkSpec.getColumnName(), watermarkSpec.getWatermarkExpression());
            }
            if (primaryKey != null) {
                resultBuilder.primaryKeyNamed(
                        primaryKey.getConstraintName(), primaryKey.getColumnNames());
            }
            return resultBuilder.build();
        }
    }
}
