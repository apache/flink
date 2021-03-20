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

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

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
            if (comment != null && comment.length() > 0) {
                properties.put(COMMENT, comment);
            }

            serializePartitionKeys(properties, resolvedTable.getPartitionKeys());

            properties.putAll(resolvedTable.getOptions());

            properties.remove(IS_GENERIC); // reserved option

            return properties;
        } catch (Exception e) {
            throw new CatalogException("Error in serializing catalog table.", e);
        }
    }

    /** Deserializes the given map of string properties into an unresolved {@link CatalogTable}. */
    public static CatalogTable deserializeCatalogTable(Map<String, String> properties) {
        try {
            final Schema schema = deserializeSchema(properties);

            final @Nullable String comment = properties.get(COMMENT);

            final List<String> partitionKeys = deserializePartitionKeys(properties);

            final Map<String, String> options = deserializeOptions(properties);

            return CatalogTable.of(schema, comment, partitionKeys, options);
        } catch (Exception e) {
            throw new CatalogException("Error in deserializing catalog table.", e);
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

    private static Map<String, String> deserializeOptions(Map<String, String> map) {
        return map.entrySet().stream()
                .filter(
                        e -> {
                            final String key = e.getKey();
                            return !key.startsWith(SCHEMA + SEPARATOR)
                                    && !key.startsWith(PARTITION_KEYS + SEPARATOR)
                                    && !key.equals(COMMENT);
                        })
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
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

    private static Schema deserializeSchema(Map<String, String> map) {
        final Builder builder = Schema.newBuilder();

        deserializeColumns(map, builder);

        deserializeWatermark(map, builder);

        deserializePrimaryKey(map, builder);

        return builder.build();
    }

    private static void deserializePrimaryKey(Map<String, String> map, Builder builder) {
        final String constraintNameKey = compoundKey(SCHEMA, PRIMARY_KEY_NAME);
        final String columnsKey = compoundKey(SCHEMA, PRIMARY_KEY_COLUMNS);
        if (map.containsKey(constraintNameKey)) {
            final String constraintName = getValue(map, constraintNameKey);
            final String[] columns = getValue(map, columnsKey, s -> s.split(","));
            builder.primaryKeyNamed(constraintName, columns);
        }
    }

    private static void deserializeWatermark(Map<String, String> map, Builder builder) {
        final String watermarkKey = compoundKey(SCHEMA, WATERMARK);
        final int watermarkCount = getCount(map, watermarkKey, WATERMARK_ROWTIME);
        for (int i = 0; i < watermarkCount; i++) {
            final String rowtimeKey = compoundKey(watermarkKey, i, WATERMARK_ROWTIME);
            final String exprKey = compoundKey(watermarkKey, i, WATERMARK_STRATEGY_EXPR);

            final String rowtime = getValue(map, rowtimeKey);
            final String expr = getValue(map, exprKey);
            builder.watermark(rowtime, expr);
        }
    }

    private static void deserializeColumns(Map<String, String> map, Builder builder) {
        final int fieldCount = getCount(map, SCHEMA, NAME);

        for (int i = 0; i < fieldCount; i++) {
            final String nameKey = compoundKey(SCHEMA, i, NAME);
            final String dataTypeKey = compoundKey(SCHEMA, i, DATA_TYPE);
            final String exprKey = compoundKey(SCHEMA, i, EXPR);
            final String metadataKey = compoundKey(SCHEMA, i, METADATA);
            final String virtualKey = compoundKey(SCHEMA, i, VIRTUAL);

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
                    builder.columnByMetadata(name, dataType, isVirtual);
                } else {
                    builder.columnByMetadata(name, dataType, metadata, isVirtual);
                }
            }
            // physical column
            else {
                final String dataType = getValue(map, dataTypeKey);
                builder.column(name, dataType);
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
        final String[] names = serializeColumnNames(columns);
        final String[] dataTypes = serializeColumnDataTypes(columns);
        final String[] expressions = serializeColumnComputations(columns);
        final String[] metadata = serializeColumnMetadataKeys(columns);
        final String[] virtual = serializeColumnVirtuality(columns);

        final List<List<String>> values = new ArrayList<>();
        for (int i = 0; i < columns.size(); i++) {
            values.add(
                    Arrays.asList(names[i], dataTypes[i], expressions[i], metadata[i], virtual[i]));
        }

        putIndexedProperties(
                map, SCHEMA, Arrays.asList(NAME, DATA_TYPE, EXPR, METADATA, VIRTUAL), values);
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
                        escapedKey
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
