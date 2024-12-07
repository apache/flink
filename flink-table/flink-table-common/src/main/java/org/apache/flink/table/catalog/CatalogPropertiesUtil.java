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

package org.apache.flink.table.catalog;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Schema.Builder;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.catalog.Column.ComputedColumn;
import org.apache.flink.table.catalog.Column.MetadataColumn;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.util.StringUtils;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.apache.flink.table.utils.EncodingUtils.decodeBase64ToBytes;
import static org.apache.flink.table.utils.EncodingUtils.encodeBytesToBase64;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** Utilities for de/serializing {@link Catalog} objects into a map of string properties. */
@Internal
public final class CatalogPropertiesUtil {

    /**
     * Flag to distinguish if a meta-object is a generic Flink object or not.
     *
     * <p>It is used to distinguish between Flink's generic connector discovery logic or specialized
     * catalog connectors.
     */
    public static final String IS_GENERIC = "is_generic";

    /**
     * Globally reserved prefix for catalog properties. User-defined properties should not use this
     * prefix. E.g. it is used to distinguish properties created by Hive and Flink, as Hive
     * metastore has its own properties created upon table creation and migration between different
     * versions of metastore.
     */
    public static final String FLINK_PROPERTY_PREFIX = "flink.";

    /** Serializes the given {@link ResolvedCatalogTable} into a map of string properties. */
    public static Map<String, String> serializeCatalogTable(ResolvedCatalogTable resolvedTable) {
        try {
            final Map<String, String> properties = new HashMap<>();

            serializeResolvedSchema(properties, resolvedTable.getResolvedSchema());

            final String comment = resolvedTable.getComment();
            if (comment != null && !comment.isEmpty()) {
                properties.put(COMMENT, comment);
            }

            final Optional<Long> snapshot = resolvedTable.getSnapshot();
            snapshot.ifPresent(snapshotId -> properties.put(SNAPSHOT, Long.toString(snapshotId)));

            serializePartitionKeys(properties, resolvedTable.getPartitionKeys());

            properties.putAll(resolvedTable.getOptions());

            properties.remove(IS_GENERIC); // reserved option

            return properties;
        } catch (Exception e) {
            throw new CatalogException("Error in serializing catalog table.", e);
        }
    }

    /** Serializes the given {@link ResolvedCatalogView} into a map of string properties. */
    public static Map<String, String> serializeCatalogView(ResolvedCatalogView resolvedView) {
        try {
            final Map<String, String> properties = new HashMap<>();

            serializeResolvedSchema(properties, resolvedView.getResolvedSchema());

            final String comment = resolvedView.getComment();
            if (comment != null && !comment.isEmpty()) {
                properties.put(COMMENT, comment);
            }

            properties.putAll(resolvedView.getOptions());

            properties.remove(IS_GENERIC); // reserved option

            return properties;
        } catch (Exception e) {
            throw new CatalogException("Error in serializing catalog view.", e);
        }
    }

    /**
     * Serializes the given {@link ResolvedCatalogMaterializedTable} into a map of string
     * properties.
     */
    public static Map<String, String> serializeCatalogMaterializedTable(
            ResolvedCatalogMaterializedTable resolvedMaterializedTable) {
        try {
            final Map<String, String> properties = new HashMap<>();

            serializeResolvedSchema(properties, resolvedMaterializedTable.getResolvedSchema());

            final String comment = resolvedMaterializedTable.getComment();
            if (comment != null && comment.length() > 0) {
                properties.put(COMMENT, comment);
            }

            final Optional<Long> snapshot = resolvedMaterializedTable.getSnapshot();
            snapshot.ifPresent(snapshotId -> properties.put(SNAPSHOT, Long.toString(snapshotId)));

            serializePartitionKeys(properties, resolvedMaterializedTable.getPartitionKeys());

            properties.putAll(resolvedMaterializedTable.getOptions());

            properties.put(DEFINITION_QUERY, resolvedMaterializedTable.getDefinitionQuery());

            IntervalFreshness intervalFreshness =
                    resolvedMaterializedTable.getDefinitionFreshness();
            properties.put(FRESHNESS_INTERVAL, intervalFreshness.getInterval());
            properties.put(FRESHNESS_UNIT, intervalFreshness.getTimeUnit().name());

            properties.put(
                    LOGICAL_REFRESH_MODE, resolvedMaterializedTable.getLogicalRefreshMode().name());
            properties.put(REFRESH_MODE, resolvedMaterializedTable.getRefreshMode().name());
            properties.put(REFRESH_STATUS, resolvedMaterializedTable.getRefreshStatus().name());

            resolvedMaterializedTable
                    .getRefreshHandlerDescription()
                    .ifPresent(
                            refreshHandlerDesc ->
                                    properties.put(REFRESH_HANDLER_DESC, refreshHandlerDesc));
            if (resolvedMaterializedTable.getSerializedRefreshHandler() != null) {
                properties.put(
                        REFRESH_HANDLER_BYTES,
                        encodeBytesToBase64(
                                resolvedMaterializedTable.getSerializedRefreshHandler()));
            }

            return properties;
        } catch (Exception e) {
            throw new CatalogException("Error in serializing catalog materialized table.", e);
        }
    }

    /** Serializes the given {@link ResolvedCatalogModel} into a map of string properties. */
    public static Map<String, String> serializeResolvedCatalogModel(
            ResolvedCatalogModel resolvedModel) {
        try {
            final Map<String, String> properties = new HashMap<>();

            serializeResolvedModelSchema(
                    properties,
                    resolvedModel.getResolvedInputSchema(),
                    resolvedModel.getResolvedOutputSchema());

            final String comment = resolvedModel.getComment();
            if (comment != null && !comment.isEmpty()) {
                properties.put(COMMENT, comment);
            }

            properties.putAll(resolvedModel.getOptions());

            return properties;
        } catch (Exception e) {
            throw new CatalogException("Error in serializing catalog model.", e);
        }
    }

    /** Deserializes the given map of string properties into an unresolved {@link CatalogTable}. */
    public static CatalogTable deserializeCatalogTable(Map<String, String> properties) {
        return deserializeCatalogTable(properties, null);
    }

    /**
     * Deserializes the given map of string properties into an unresolved {@link CatalogTable}.
     *
     * @param properties The properties to deserialize from
     * @param fallbackKey The fallback key to get the schema properties. This is meant to support
     *     the old table (1.10) deserialization
     * @return a catalog table instance.
     */
    public static CatalogTable deserializeCatalogTable(
            Map<String, String> properties, @Nullable String fallbackKey) {
        try {
            int count = getCount(properties, SCHEMA, NAME);
            String schemaKey = SCHEMA;
            if (count == 0 && fallbackKey != null) {
                schemaKey = fallbackKey;
            }

            final Schema schema = deserializeSchema(properties, schemaKey);

            final @Nullable String comment = properties.get(COMMENT);

            final @Nullable Long snapshot =
                    properties.containsKey(SNAPSHOT)
                            ? getValue(properties, SNAPSHOT, Long::parseLong)
                            : null;

            final List<String> partitionKeys = deserializePartitionKeys(properties);

            final Map<String, String> options = deserializeOptions(properties, schemaKey);

            return CatalogTable.of(schema, comment, partitionKeys, options, snapshot);
        } catch (Exception e) {
            throw new CatalogException("Error in deserializing catalog table.", e);
        }
    }

    /**
     * Deserializes the given map of string properties into an unresolved {@link
     * CatalogMaterializedTable}.
     */
    public static CatalogMaterializedTable deserializeCatalogMaterializedTable(
            Map<String, String> properties) {
        try {
            final Schema schema = deserializeSchema(properties, SCHEMA);

            final @Nullable String comment = properties.get(COMMENT);

            final @Nullable Long snapshot =
                    properties.containsKey(SNAPSHOT)
                            ? getValue(properties, SNAPSHOT, Long::parseLong)
                            : null;

            final List<String> partitionKeys = deserializePartitionKeys(properties);

            final Map<String, String> options = deserializeOptions(properties, SCHEMA);

            final String definitionQuery = properties.get(DEFINITION_QUERY);

            final String freshnessInterval = properties.get(FRESHNESS_INTERVAL);
            final IntervalFreshness.TimeUnit timeUnit =
                    IntervalFreshness.TimeUnit.valueOf(properties.get(FRESHNESS_UNIT));
            final IntervalFreshness freshness = IntervalFreshness.of(freshnessInterval, timeUnit);

            final CatalogMaterializedTable.LogicalRefreshMode logicalRefreshMode =
                    CatalogMaterializedTable.LogicalRefreshMode.valueOf(
                            properties.get(LOGICAL_REFRESH_MODE));
            final CatalogMaterializedTable.RefreshMode refreshMode =
                    CatalogMaterializedTable.RefreshMode.valueOf(properties.get(REFRESH_MODE));
            final CatalogMaterializedTable.RefreshStatus refreshStatus =
                    CatalogMaterializedTable.RefreshStatus.valueOf(properties.get(REFRESH_STATUS));

            final @Nullable String refreshHandlerDesc = properties.get(REFRESH_HANDLER_DESC);
            final @Nullable String refreshHandlerStringBytes =
                    properties.get(REFRESH_HANDLER_BYTES);
            final @Nullable byte[] refreshHandlerBytes =
                    StringUtils.isNullOrWhitespaceOnly(refreshHandlerStringBytes)
                            ? null
                            : decodeBase64ToBytes(refreshHandlerStringBytes);

            CatalogMaterializedTable.Builder builder = CatalogMaterializedTable.newBuilder();
            builder.schema(schema)
                    .comment(comment)
                    .partitionKeys(partitionKeys)
                    .options(options)
                    .snapshot(snapshot)
                    .definitionQuery(definitionQuery)
                    .freshness(freshness)
                    .logicalRefreshMode(logicalRefreshMode)
                    .refreshMode(refreshMode)
                    .refreshStatus(refreshStatus)
                    .refreshHandlerDescription(refreshHandlerDesc)
                    .serializedRefreshHandler(refreshHandlerBytes);
            return builder.build();
        } catch (Exception e) {
            throw new CatalogException("Error in deserializing catalog materialized table.", e);
        }
    }

    /*
     * Deserializes the given map of string properties into an unresolved {@link CatalogModel}.
     *
     * @param properties The properties to deserialize from
     * @return {@link CatalogModel}
     */
    public static CatalogModel deserializeCatalogModel(Map<String, String> properties) {
        try {
            final Builder inputSchemaBuilder = Schema.newBuilder();
            deserializeColumns(properties, MODEL_INPUT_SCHEMA, inputSchemaBuilder);
            final Schema inputSchema = inputSchemaBuilder.build();

            final Builder outputSchemaBuilder = Schema.newBuilder();
            deserializeColumns(properties, MODEL_OUTPUT_SCHEMA, outputSchemaBuilder);
            final Schema outputSchema = outputSchemaBuilder.build();

            final Map<String, String> modelOptions =
                    deserializeOptions(
                            properties, Arrays.asList(MODEL_INPUT_SCHEMA, MODEL_OUTPUT_SCHEMA));

            final @Nullable String comment = properties.get(COMMENT);

            return CatalogModel.of(inputSchema, outputSchema, modelOptions, comment);
        } catch (Exception e) {
            throw new CatalogException("Error in deserializing catalog model.", e);
        }
    }

    // --------------------------------------------------------------------------------------------
    // Helper methods and constants
    // --------------------------------------------------------------------------------------------

    private static final String SEPARATOR = ".";

    private static final String SCHEMA = "schema";

    private static final String NAME = "name";

    private static final String DATA_TYPE = "data-type";

    private static final String EXPR = "expr";

    private static final String METADATA = "metadata";

    private static final String VIRTUAL = "virtual";

    private static final String PRIMARY_KEY = "primary-key";

    private static final String COLUMNS = "columns";

    private static final String PARTITION = "partition";

    private static final String KEYS = "keys";

    private static final String PARTITION_KEYS = compoundKey(PARTITION, KEYS);

    private static final String WATERMARK = "watermark";

    private static final String WATERMARK_ROWTIME = "rowtime";

    private static final String WATERMARK_STRATEGY = "strategy";

    private static final String WATERMARK_STRATEGY_EXPR = compoundKey(WATERMARK_STRATEGY, EXPR);

    private static final String WATERMARK_STRATEGY_DATA_TYPE =
            compoundKey(WATERMARK_STRATEGY, DATA_TYPE);

    private static final String PRIMARY_KEY_NAME = compoundKey(PRIMARY_KEY, NAME);

    private static final String PRIMARY_KEY_COLUMNS = compoundKey(PRIMARY_KEY, COLUMNS);

    private static final String COMMENT = "comment";

    private static final String SNAPSHOT = "snapshot";

    private static final String DEFINITION_QUERY = "definition-query";

    private static final String FRESHNESS_INTERVAL = "freshness-interval";

    private static final String FRESHNESS_UNIT = "freshness-unit";

    private static final String LOGICAL_REFRESH_MODE = "logical-refresh-mode";

    private static final String REFRESH_MODE = "refresh-mode";

    private static final String REFRESH_STATUS = "refresh-status";

    private static final String REFRESH_HANDLER_DESC = "refresh-handler-desc";

    private static final String REFRESH_HANDLER_BYTES = "refresh-handler-bytes";

    private static final String MODEL_INPUT_SCHEMA = "input-schema";

    private static final String MODEL_OUTPUT_SCHEMA = "output-schema";

    private static Map<String, String> deserializeOptions(
            Map<String, String> map, String schemaKey) {
        return deserializeOptions(map, Collections.singletonList(schemaKey));
    }

    private static Map<String, String> deserializeOptions(
            Map<String, String> map, List<String> schemaKeys) {
        return map.entrySet().stream()
                .filter(
                        e -> {
                            final String key = e.getKey();
                            return schemaKeys.stream()
                                            .noneMatch(
                                                    schemaKey ->
                                                            key.startsWith(schemaKey + SEPARATOR))
                                    && !key.startsWith(PARTITION_KEYS + SEPARATOR)
                                    && !key.equals(COMMENT)
                                    && !key.equals(SNAPSHOT)
                                    && !isMaterializedTableAttribute(key);
                        })
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    private static boolean isMaterializedTableAttribute(String key) {
        return key.equals(DEFINITION_QUERY)
                || key.equals(FRESHNESS_INTERVAL)
                || key.equals(FRESHNESS_UNIT)
                || key.equals(LOGICAL_REFRESH_MODE)
                || key.equals(REFRESH_MODE)
                || key.equals(REFRESH_STATUS)
                || key.equals(REFRESH_HANDLER_DESC)
                || key.equals(REFRESH_HANDLER_BYTES);
    }

    private static List<String> deserializePartitionKeys(Map<String, String> map) {
        final int partitionCount = getCount(map, PARTITION_KEYS, NAME);
        final List<String> partitionKeys = new ArrayList<>();
        for (int i = 0; i < partitionCount; i++) {
            final String partitionNameKey = compoundKey(PARTITION_KEYS, i, NAME);
            final String partitionName = getValue(map, partitionNameKey);
            partitionKeys.add(partitionName);
        }
        return partitionKeys;
    }

    private static Schema deserializeSchema(Map<String, String> map, String schemaKey) {
        final Builder builder = Schema.newBuilder();

        deserializeColumns(map, schemaKey, builder);

        deserializeWatermark(map, schemaKey, builder);

        deserializePrimaryKey(map, schemaKey, builder);

        return builder.build();
    }

    private static void deserializePrimaryKey(
            Map<String, String> map, String schemaKey, Builder builder) {
        final String constraintNameKey = compoundKey(schemaKey, PRIMARY_KEY_NAME);
        final String columnsKey = compoundKey(schemaKey, PRIMARY_KEY_COLUMNS);
        if (map.containsKey(constraintNameKey)) {
            final String constraintName = getValue(map, constraintNameKey);
            final String[] columns = getValue(map, columnsKey, s -> s.split(","));
            builder.primaryKeyNamed(constraintName, columns);
        }
    }

    private static void deserializeWatermark(
            Map<String, String> map, String schemaKey, Builder builder) {
        final String watermarkKey = compoundKey(schemaKey, WATERMARK);
        final int watermarkCount = getCount(map, watermarkKey, WATERMARK_ROWTIME);
        for (int i = 0; i < watermarkCount; i++) {
            final String rowtimeKey = compoundKey(watermarkKey, i, WATERMARK_ROWTIME);
            final String exprKey = compoundKey(watermarkKey, i, WATERMARK_STRATEGY_EXPR);

            final String rowtime = getValue(map, rowtimeKey);
            final String expr = getValue(map, exprKey);
            builder.watermark(rowtime, expr);
        }
    }

    private static void deserializeColumns(
            Map<String, String> map, String schemaKey, Builder builder) {
        final int fieldCount = getCount(map, schemaKey, NAME);

        for (int i = 0; i < fieldCount; i++) {
            final String nameKey = compoundKey(schemaKey, i, NAME);
            final String dataTypeKey = compoundKey(schemaKey, i, DATA_TYPE);
            final String exprKey = compoundKey(schemaKey, i, EXPR);
            final String metadataKey = compoundKey(schemaKey, i, METADATA);
            final String virtualKey = compoundKey(schemaKey, i, VIRTUAL);
            final String commentKey = compoundKey(schemaKey, i, COMMENT);

            final String name = getValue(map, nameKey);

            // computed column
            if (map.containsKey(exprKey)) {
                final String expr = getValue(map, exprKey);
                builder.columnByExpression(name, expr);
            }
            // metadata column
            else if (map.containsKey(metadataKey)) {
                final String metadata = getValue(map, metadataKey);
                final String dataType = getValue(map, dataTypeKey);
                final boolean isVirtual = getValue(map, virtualKey, Boolean::parseBoolean);
                if (metadata.equals(name)) {
                    builder.columnByMetadata(name, dataType, null, isVirtual);
                } else {
                    builder.columnByMetadata(name, dataType, metadata, isVirtual);
                }
            }
            // physical column
            else {
                final String dataType = getValue(map, dataTypeKey);
                builder.column(name, dataType);
            }

            // column comment
            if (map.containsKey(commentKey)) {
                final String comment = getValue(map, commentKey);
                builder.withComment(comment);
            }
        }
    }

    private static void serializePartitionKeys(Map<String, String> map, List<String> keys) {
        checkNotNull(keys);

        putIndexedProperties(
                map,
                PARTITION_KEYS,
                Collections.singletonList(NAME),
                keys.stream().map(Collections::singletonList).collect(Collectors.toList()));
    }

    private static void serializeResolvedModelSchema(
            Map<String, String> map, ResolvedSchema inputSchema, ResolvedSchema outputSchema) {
        checkNotNull(inputSchema);
        checkNotNull(outputSchema);
        serializeColumnsWithKey(map, inputSchema.getColumns(), MODEL_INPUT_SCHEMA);
        serializeColumnsWithKey(map, outputSchema.getColumns(), MODEL_OUTPUT_SCHEMA);
    }

    private static void serializeResolvedSchema(Map<String, String> map, ResolvedSchema schema) {
        checkNotNull(schema);

        serializeColumns(map, schema.getColumns());

        serializeWatermarkSpecs(map, schema.getWatermarkSpecs());

        schema.getPrimaryKey().ifPresent(pk -> serializePrimaryKey(map, pk));
    }

    private static void serializePrimaryKey(Map<String, String> map, UniqueConstraint constraint) {
        map.put(compoundKey(SCHEMA, PRIMARY_KEY_NAME), constraint.getName());
        map.put(
                compoundKey(SCHEMA, PRIMARY_KEY_COLUMNS),
                String.join(",", constraint.getColumns()));
    }

    private static void serializeWatermarkSpecs(
            Map<String, String> map, List<WatermarkSpec> specs) {
        if (!specs.isEmpty()) {
            final List<List<String>> watermarkValues = new ArrayList<>();
            for (WatermarkSpec spec : specs) {
                watermarkValues.add(
                        Arrays.asList(
                                spec.getRowtimeAttribute(),
                                serializeResolvedExpression(spec.getWatermarkExpression()),
                                serializeDataType(
                                        spec.getWatermarkExpression().getOutputDataType())));
            }
            putIndexedProperties(
                    map,
                    compoundKey(SCHEMA, WATERMARK),
                    Arrays.asList(
                            WATERMARK_ROWTIME,
                            WATERMARK_STRATEGY_EXPR,
                            WATERMARK_STRATEGY_DATA_TYPE),
                    watermarkValues);
        }
    }

    private static void serializeColumns(Map<String, String> map, List<Column> columns) {
        serializeColumnsWithKey(map, columns, SCHEMA);
    }

    private static void serializeColumnsWithKey(
            Map<String, String> map, List<Column> columns, String schemaKey) {
        final String[] names = serializeColumnNames(columns);
        final String[] dataTypes = serializeColumnDataTypes(columns);
        final String[] expressions = serializeColumnComputations(columns);
        final String[] metadata = serializeColumnMetadataKeys(columns);
        final String[] virtual = serializeColumnVirtuality(columns);
        final String[] comments = serializeColumnComments(columns);

        final List<List<String>> values = new ArrayList<>();
        for (int i = 0; i < columns.size(); i++) {
            values.add(
                    Arrays.asList(
                            names[i],
                            dataTypes[i],
                            expressions[i],
                            metadata[i],
                            virtual[i],
                            comments[i]));
        }

        putIndexedProperties(
                map,
                schemaKey,
                Arrays.asList(NAME, DATA_TYPE, EXPR, METADATA, VIRTUAL, COMMENT),
                values);
    }

    private static String serializeResolvedExpression(ResolvedExpression resolvedExpression) {
        try {
            return resolvedExpression.asSerializableString();
        } catch (TableException e) {
            throw new TableException(
                    String.format(
                            "Expression '%s' cannot be stored in a durable catalog. "
                                    + "Currently, only SQL expressions have a well-defined string "
                                    + "representation that is used to serialize a catalog object "
                                    + "into a map of string-based properties.",
                            resolvedExpression.asSummaryString()),
                    e);
        }
    }

    private static String serializeDataType(DataType dataType) {
        final LogicalType type = dataType.getLogicalType();
        try {
            return type.asSerializableString();
        } catch (TableException e) {
            throw new TableException(
                    String.format(
                            "Data type '%s' cannot be stored in a durable catalog. Only data types "
                                    + "that have a well-defined string representation can be used "
                                    + "when serializing a catalog object into a map of string-based "
                                    + "properties. This excludes anonymously defined, unregistered "
                                    + "types such as structured types in particular.",
                            type.asSummaryString()),
                    e);
        }
    }

    private static String[] serializeColumnNames(List<Column> columns) {
        return columns.stream().map(Column::getName).toArray(String[]::new);
    }

    private static String[] serializeColumnDataTypes(List<Column> columns) {
        return columns.stream()
                .map(Column::getDataType)
                .map(CatalogPropertiesUtil::serializeDataType)
                .toArray(String[]::new);
    }

    private static String[] serializeColumnComputations(List<Column> columns) {
        return columns.stream()
                .map(
                        column -> {
                            if (column instanceof ComputedColumn) {
                                final ComputedColumn c = (ComputedColumn) column;
                                return serializeResolvedExpression(c.getExpression());
                            }
                            return null;
                        })
                .toArray(String[]::new);
    }

    private static String[] serializeColumnMetadataKeys(List<Column> columns) {
        return columns.stream()
                .map(
                        column -> {
                            if (column instanceof MetadataColumn) {
                                final MetadataColumn c = (MetadataColumn) column;
                                return c.getMetadataKey().orElse(c.getName());
                            }
                            return null;
                        })
                .toArray(String[]::new);
    }

    private static String[] serializeColumnVirtuality(List<Column> columns) {
        return columns.stream()
                .map(
                        column -> {
                            if (column instanceof MetadataColumn) {
                                final MetadataColumn c = (MetadataColumn) column;
                                return Boolean.toString(c.isVirtual());
                            }
                            return null;
                        })
                .toArray(String[]::new);
    }

    private static String[] serializeColumnComments(List<Column> columns) {
        return columns.stream().map(c -> c.getComment().orElse(null)).toArray(String[]::new);
    }

    /**
     * Adds an indexed sequence of properties (with sub-properties) under a common key. It supports
     * the property's value to be null, in which case it would be ignored. The sub-properties should
     * at least have one non-null value.
     *
     * <p>For example:
     *
     * <pre>
     *     schema.fields.0.type = INT, schema.fields.0.name = test
     *     schema.fields.1.type = LONG, schema.fields.1.name = test2
     *     schema.fields.2.type = LONG, schema.fields.2.name = test3, schema.fields.2.expr = test2 + 1
     * </pre>
     *
     * <p>The arity of each subKeyValues must match the arity of propertyKeys.
     */
    private static void putIndexedProperties(
            Map<String, String> map,
            String key,
            List<String> subKeys,
            List<List<String>> subKeyValues) {
        checkNotNull(key);
        checkNotNull(subKeys);
        checkNotNull(subKeyValues);
        for (int idx = 0; idx < subKeyValues.size(); idx++) {
            final List<String> values = subKeyValues.get(idx);
            if (values == null || values.size() != subKeys.size()) {
                throw new IllegalArgumentException("Values must have same arity as keys.");
            }
            if (values.stream().allMatch(Objects::isNull)) {
                throw new IllegalArgumentException("Values must have at least one non-null value.");
            }
            for (int keyIdx = 0; keyIdx < values.size(); keyIdx++) {
                String value = values.get(keyIdx);
                if (value != null) {
                    map.put(compoundKey(key, idx, subKeys.get(keyIdx)), values.get(keyIdx));
                }
            }
        }
    }

    /**
     * Extracts the property count under the given key and suffix.
     *
     * <p>For example:
     *
     * <pre>
     *     schema.0.name, schema.1.name -> 2
     * </pre>
     */
    private static int getCount(Map<String, String> map, String key, String suffix) {
        final String escapedKey = Pattern.quote(key);
        final String escapedSuffix = Pattern.quote(suffix);
        final String escapedSeparator = Pattern.quote(SEPARATOR);
        final Pattern pattern =
                Pattern.compile(
                        "^"
                                + escapedKey
                                + escapedSeparator
                                + "(\\d+)"
                                + escapedSeparator
                                + escapedSuffix);
        final IntStream indexes =
                map.keySet().stream()
                        .flatMapToInt(
                                k -> {
                                    final Matcher matcher = pattern.matcher(k);
                                    if (matcher.find()) {
                                        return IntStream.of(Integer.parseInt(matcher.group(1)));
                                    }
                                    return IntStream.empty();
                                });

        return indexes.max().orElse(-1) + 1;
    }

    private static String getValue(Map<String, String> map, String key) {
        return getValue(map, key, Function.identity());
    }

    private static <T> T getValue(Map<String, String> map, String key, Function<String, T> parser) {
        final String value = map.get(key);
        if (value == null) {
            throw new IllegalArgumentException(
                    String.format("Could not find property key '%s'.", key));
        }
        try {
            return parser.apply(value);
        } catch (Exception e) {
            throw new IllegalArgumentException(
                    String.format("Could not parse value for property key '%s': %s", key, value));
        }
    }

    private static String compoundKey(Object... components) {
        return Stream.of(components).map(Object::toString).collect(Collectors.joining(SEPARATOR));
    }

    private CatalogPropertiesUtil() {
        // no instantiation
    }
}
