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

package org.apache.flink.table.descriptors;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.table.api.TableColumn;
import org.apache.flink.table.api.TableColumn.ComputedColumn;
import org.apache.flink.table.api.TableColumn.MetadataColumn;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.WatermarkSpec;
import org.apache.flink.table.catalog.CatalogPropertiesUtil;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.utils.LogicalTypeParser;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.table.utils.EncodingUtils;
import org.apache.flink.table.utils.TypeStringUtils;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.TimeUtils;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Utility class for having a unified string-based representation of Table API related classes such
 * as TableSchema, TypeInformation, etc.
 *
 * <p>Note to implementers: Please try to reuse key names as much as possible. Key-names should be
 * hierarchical and lower case. Use "-" instead of dots or camel case. E.g.,
 * connector.schema.start-from = from-earliest. Try not to use the higher level in a key-name. E.g.,
 * instead of connector.kafka.kafka-version use connector.kafka.version.
 *
 * <p>Properties with key normalization enabled contain only lower-case keys.
 *
 * @deprecated This utility will be dropped soon. {@link DynamicTableFactory} is based on {@link
 *     ConfigOption} and catalogs use {@link CatalogPropertiesUtil}.
 */
@Deprecated
@Internal
public class DescriptorProperties {

    public static final String NAME = "name";

    public static final String TYPE = "type";

    public static final String DATA_TYPE = "data-type";

    public static final String EXPR = "expr";

    public static final String METADATA = "metadata";

    public static final String VIRTUAL = "virtual";

    public static final String PARTITION_KEYS = "partition.keys";

    public static final String WATERMARK = "watermark";

    public static final String WATERMARK_ROWTIME = "rowtime";

    public static final String WATERMARK_STRATEGY = "strategy";

    public static final String WATERMARK_STRATEGY_EXPR = WATERMARK_STRATEGY + '.' + EXPR;

    public static final String WATERMARK_STRATEGY_DATA_TYPE = WATERMARK_STRATEGY + '.' + DATA_TYPE;

    public static final String PRIMARY_KEY_NAME = "primary-key.name";

    public static final String PRIMARY_KEY_COLUMNS = "primary-key.columns";

    public static final String COMMENT = "comment";

    private static final Pattern SCHEMA_COLUMN_NAME_SUFFIX = Pattern.compile("\\d+\\.name");

    private static final Consumer<String> EMPTY_CONSUMER = (value) -> {};

    private final boolean normalizeKeys;

    private final Map<String, String> properties;

    public DescriptorProperties(boolean normalizeKeys) {
        this.properties = new HashMap<>();
        this.normalizeKeys = normalizeKeys;
    }

    public DescriptorProperties() {
        this(true);
    }

    /** Adds a set of properties. */
    public void putProperties(Map<String, String> properties) {
        for (Map.Entry<String, String> property : properties.entrySet()) {
            put(property.getKey(), property.getValue());
        }
    }

    /** Adds a set of descriptor properties. */
    public void putProperties(DescriptorProperties otherProperties) {
        for (Map.Entry<String, String> otherProperty : otherProperties.properties.entrySet()) {
            put(otherProperty.getKey(), otherProperty.getValue());
        }
    }

    /**
     * Adds a properties map by appending the given prefix to element keys with a dot.
     *
     * <p>For example: for prefix "flink" and a map of a single property with key "k" and value "v".
     * The added property will be as key "flink.k" and value "v".
     */
    public void putPropertiesWithPrefix(String prefix, Map<String, String> prop) {
        checkNotNull(prefix);
        checkNotNull(prop);

        for (Map.Entry<String, String> e : prop.entrySet()) {
            put(String.format("%s.%s", prefix, e.getKey()), e.getValue());
        }
    }

    /** Adds a class under the given key. */
    public void putClass(String key, Class<?> clazz) {
        checkNotNull(key);
        checkNotNull(clazz);
        final String error = InstantiationUtil.checkForInstantiationError(clazz);
        if (error != null) {
            throw new ValidationException(
                    "Class '" + clazz.getName() + "' is not supported: " + error);
        }
        put(key, clazz.getName());
    }

    /** Adds a string under the given key. */
    public void putString(String key, String str) {
        checkNotNull(key);
        checkNotNull(str);
        put(key, str);
    }

    /** Adds a boolean under the given key. */
    public void putBoolean(String key, boolean b) {
        checkNotNull(key);
        put(key, Boolean.toString(b));
    }

    /** Adds a long under the given key. */
    public void putLong(String key, long l) {
        checkNotNull(key);
        put(key, Long.toString(l));
    }

    /** Adds an integer under the given key. */
    public void putInt(String key, int i) {
        checkNotNull(key);
        put(key, Integer.toString(i));
    }

    /** Adds a character under the given key. */
    public void putCharacter(String key, char c) {
        checkNotNull(key);
        put(key, Character.toString(c));
    }

    /** Adds a table schema under the given key. */
    public void putTableSchema(String key, TableSchema schema) {
        checkNotNull(key);
        checkNotNull(schema);

        final String[] fieldNames = schema.getFieldNames();
        final DataType[] fieldTypes = schema.getFieldDataTypes();
        final String[] fieldExpressions =
                schema.getTableColumns().stream()
                        .map(
                                column -> {
                                    if (column instanceof ComputedColumn) {
                                        return ((ComputedColumn) column).getExpression();
                                    }
                                    return null;
                                })
                        .toArray(String[]::new);
        final String[] fieldMetadata =
                schema.getTableColumns().stream()
                        .map(
                                column -> {
                                    if (column instanceof MetadataColumn) {
                                        return ((MetadataColumn) column)
                                                .getMetadataAlias()
                                                .orElse(column.getName());
                                    }
                                    return null;
                                })
                        .toArray(String[]::new);
        final String[] fieldVirtual =
                schema.getTableColumns().stream()
                        .map(
                                column -> {
                                    if (column instanceof MetadataColumn) {
                                        return Boolean.toString(
                                                ((MetadataColumn) column).isVirtual());
                                    }
                                    return null;
                                })
                        .toArray(String[]::new);

        final List<List<String>> values = new ArrayList<>();
        for (int i = 0; i < schema.getFieldCount(); i++) {
            values.add(
                    Arrays.asList(
                            fieldNames[i],
                            fieldTypes[i].getLogicalType().asSerializableString(),
                            fieldExpressions[i],
                            fieldMetadata[i],
                            fieldVirtual[i]));
        }

        putIndexedOptionalProperties(
                key, Arrays.asList(NAME, DATA_TYPE, EXPR, METADATA, VIRTUAL), values);

        if (!schema.getWatermarkSpecs().isEmpty()) {
            final List<List<String>> watermarkValues = new ArrayList<>();
            for (WatermarkSpec spec : schema.getWatermarkSpecs()) {
                watermarkValues.add(
                        Arrays.asList(
                                spec.getRowtimeAttribute(),
                                spec.getWatermarkExpr(),
                                spec.getWatermarkExprOutputType()
                                        .getLogicalType()
                                        .asSerializableString()));
            }
            putIndexedFixedProperties(
                    key + '.' + WATERMARK,
                    Arrays.asList(
                            WATERMARK_ROWTIME,
                            WATERMARK_STRATEGY_EXPR,
                            WATERMARK_STRATEGY_DATA_TYPE),
                    watermarkValues);
        }

        schema.getPrimaryKey()
                .ifPresent(
                        pk -> {
                            putString(key + '.' + PRIMARY_KEY_NAME, pk.getName());
                            putString(
                                    key + '.' + PRIMARY_KEY_COLUMNS,
                                    String.join(",", pk.getColumns()));
                        });
    }

    /** Adds table partition keys. */
    public void putPartitionKeys(List<String> keys) {
        checkNotNull(keys);

        putIndexedFixedProperties(
                PARTITION_KEYS,
                Collections.singletonList(NAME),
                keys.stream().map(Collections::singletonList).collect(Collectors.toList()));
    }

    /** Adds a Flink {@link MemorySize} under the given key. */
    public void putMemorySize(String key, MemorySize size) {
        checkNotNull(key);
        checkNotNull(size);
        put(key, size.toString());
    }

    /**
     * Adds an indexed sequence of properties (with sub-properties) under a common key.
     *
     * <p>For example:
     *
     * <pre>
     *     schema.fields.0.type = INT, schema.fields.0.name = test
     *     schema.fields.1.type = LONG, schema.fields.1.name = test2
     * </pre>
     *
     * <p>The arity of each subKeyValues must match the arity of propertyKeys.
     */
    public void putIndexedFixedProperties(
            String key, List<String> subKeys, List<List<String>> subKeyValues) {
        checkNotNull(key);
        checkNotNull(subKeys);
        checkNotNull(subKeyValues);
        for (int idx = 0; idx < subKeyValues.size(); idx++) {
            final List<String> values = subKeyValues.get(idx);
            if (values == null || values.size() != subKeys.size()) {
                throw new ValidationException("Values must have same arity as keys.");
            }
            for (int keyIdx = 0; keyIdx < values.size(); keyIdx++) {
                put(key + '.' + idx + '.' + subKeys.get(keyIdx), values.get(keyIdx));
            }
        }
    }

    /**
     * Adds an indexed sequence of properties (with sub-properties) under a common key. Different
     * with {@link #putIndexedFixedProperties}, this method supports the properties value to be
     * null, which would be ignore. The sub-properties should at least have one non-null value.
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
    public void putIndexedOptionalProperties(
            String key, List<String> subKeys, List<List<String>> subKeyValues) {
        checkNotNull(key);
        checkNotNull(subKeys);
        checkNotNull(subKeyValues);
        for (int idx = 0; idx < subKeyValues.size(); idx++) {
            final List<String> values = subKeyValues.get(idx);
            if (values == null || values.size() != subKeys.size()) {
                throw new ValidationException("Values must have same arity as keys.");
            }
            if (values.stream().allMatch(Objects::isNull)) {
                throw new ValidationException("Values must have at least one non-null value.");
            }
            for (int keyIdx = 0; keyIdx < values.size(); keyIdx++) {
                String value = values.get(keyIdx);
                if (value != null) {
                    put(key + '.' + idx + '.' + subKeys.get(keyIdx), values.get(keyIdx));
                }
            }
        }
    }

    /**
     * Adds an indexed mapping of properties under a common key.
     *
     * <p>For example:
     *
     * <pre>
     *     schema.fields.0.type = INT, schema.fields.0.name = test
     *                                 schema.fields.1.name = test2
     * </pre>
     *
     * <p>The arity of the subKeyValues can differ.
     */
    public void putIndexedVariableProperties(String key, List<Map<String, String>> subKeyValues) {
        checkNotNull(key);
        checkNotNull(subKeyValues);
        for (int idx = 0; idx < subKeyValues.size(); idx++) {
            final Map<String, String> values = subKeyValues.get(idx);
            for (Map.Entry<String, String> value : values.entrySet()) {
                put(key + '.' + idx + '.' + value.getKey(), value.getValue());
            }
        }
    }

    // --------------------------------------------------------------------------------------------

    /** Returns a string value under the given key if it exists. */
    public Optional<String> getOptionalString(String key) {
        return optionalGet(key);
    }

    /** Returns a string value under the given existing key. */
    public String getString(String key) {
        return getOptionalString(key).orElseThrow(exceptionSupplier(key));
    }

    /** Returns a character value under the given key if it exists. */
    public Optional<Character> getOptionalCharacter(String key) {
        return optionalGet(key)
                .map(
                        (c) -> {
                            if (c.length() != 1) {
                                throw new ValidationException(
                                        "The value of '"
                                                + key
                                                + "' must only contain one character.");
                            }
                            return c.charAt(0);
                        });
    }

    /** Returns a character value under the given existing key. */
    public Character getCharacter(String key) {
        return getOptionalCharacter(key).orElseThrow(exceptionSupplier(key));
    }

    /** Returns a class value under the given key if it exists. */
    @SuppressWarnings("unchecked")
    public <T> Optional<Class<T>> getOptionalClass(String key, Class<T> superClass) {
        return optionalGet(key)
                .map(
                        (name) -> {
                            final Class<?> clazz;
                            try {
                                clazz =
                                        Class.forName(
                                                name,
                                                true,
                                                Thread.currentThread().getContextClassLoader());
                                if (!superClass.isAssignableFrom(clazz)) {
                                    throw new ValidationException(
                                            "Class '"
                                                    + name
                                                    + "' does not extend from the required class '"
                                                    + superClass.getName()
                                                    + "' for key '"
                                                    + key
                                                    + "'.");
                                }
                                return (Class<T>) clazz;
                            } catch (Exception e) {
                                throw new ValidationException(
                                        "Could not get class '" + name + "' for key '" + key + "'.",
                                        e);
                            }
                        });
    }

    /** Returns a class value under the given existing key. */
    public <T> Class<T> getClass(String key, Class<T> superClass) {
        return getOptionalClass(key, superClass).orElseThrow(exceptionSupplier(key));
    }

    /** Returns a big decimal value under the given key if it exists. */
    public Optional<BigDecimal> getOptionalBigDecimal(String key) {
        return optionalGet(key)
                .map(
                        (value) -> {
                            try {
                                return new BigDecimal(value);
                            } catch (Exception e) {
                                throw new ValidationException(
                                        "Invalid decimal value for key '" + key + "'.", e);
                            }
                        });
    }

    /** Returns a big decimal value under the given existing key. */
    public BigDecimal getBigDecimal(String key) {
        return getOptionalBigDecimal(key).orElseThrow(exceptionSupplier(key));
    }

    /** Returns a boolean value under the given key if it exists. */
    public Optional<Boolean> getOptionalBoolean(String key) {
        return optionalGet(key)
                .map(
                        (value) -> {
                            try {
                                return Boolean.valueOf(value);
                            } catch (Exception e) {
                                throw new ValidationException(
                                        "Invalid boolean value for key '" + key + "'.", e);
                            }
                        });
    }

    /** Returns a boolean value under the given existing key. */
    public boolean getBoolean(String key) {
        return getOptionalBoolean(key).orElseThrow(exceptionSupplier(key));
    }

    /** Returns a byte value under the given key if it exists. */
    public Optional<Byte> getOptionalByte(String key) {
        return optionalGet(key)
                .map(
                        (value) -> {
                            try {
                                return Byte.valueOf(value);
                            } catch (Exception e) {
                                throw new ValidationException(
                                        "Invalid byte value for key '" + key + "'.", e);
                            }
                        });
    }

    /** Returns a byte value under the given existing key. */
    public byte getByte(String key) {
        return getOptionalByte(key).orElseThrow(exceptionSupplier(key));
    }

    /** Returns a double value under the given key if it exists. */
    public Optional<Double> getOptionalDouble(String key) {
        return optionalGet(key)
                .map(
                        (value) -> {
                            try {
                                return Double.valueOf(value);
                            } catch (Exception e) {
                                throw new ValidationException(
                                        "Invalid double value for key '" + key + "'.", e);
                            }
                        });
    }

    /** Returns a double value under the given existing key. */
    public double getDouble(String key) {
        return getOptionalDouble(key).orElseThrow(exceptionSupplier(key));
    }

    /** Returns a float value under the given key if it exists. */
    public Optional<Float> getOptionalFloat(String key) {
        return optionalGet(key)
                .map(
                        (value) -> {
                            try {
                                return Float.valueOf(value);
                            } catch (Exception e) {
                                throw new ValidationException(
                                        "Invalid float value for key '" + key + "'.", e);
                            }
                        });
    }

    /** Returns a float value under the given given existing key. */
    public float getFloat(String key) {
        return getOptionalFloat(key).orElseThrow(exceptionSupplier(key));
    }

    /** Returns an integer value under the given key if it exists. */
    public Optional<Integer> getOptionalInt(String key) {
        return optionalGet(key)
                .map(
                        (value) -> {
                            try {
                                return Integer.valueOf(value);
                            } catch (Exception e) {
                                throw new ValidationException(
                                        "Invalid integer value for key '" + key + "'.", e);
                            }
                        });
    }

    /** Returns an integer value under the given existing key. */
    public int getInt(String key) {
        return getOptionalInt(key).orElseThrow(exceptionSupplier(key));
    }

    /** Returns a long value under the given key if it exists. */
    public Optional<Long> getOptionalLong(String key) {
        return optionalGet(key)
                .map(
                        (value) -> {
                            try {
                                return Long.valueOf(value);
                            } catch (Exception e) {
                                throw new ValidationException(
                                        "Invalid long value for key '" + key + "'.", e);
                            }
                        });
    }

    /** Returns a long value under the given existing key. */
    public long getLong(String key) {
        return getOptionalLong(key).orElseThrow(exceptionSupplier(key));
    }

    /** Returns a short value under the given key if it exists. */
    public Optional<Short> getOptionalShort(String key) {
        return optionalGet(key)
                .map(
                        (value) -> {
                            try {
                                return Short.valueOf(value);
                            } catch (Exception e) {
                                throw new ValidationException(
                                        "Invalid short value for key '" + key + "'.", e);
                            }
                        });
    }

    /** Returns a short value under the given existing key. */
    public short getShort(String key) {
        return getOptionalShort(key).orElseThrow(exceptionSupplier(key));
    }

    /** Returns the type information under the given key if it exists. */
    public Optional<TypeInformation<?>> getOptionalType(String key) {
        return optionalGet(key).map(TypeStringUtils::readTypeInfo);
    }

    /** Returns the type information under the given existing key. */
    public TypeInformation<?> getType(String key) {
        return getOptionalType(key).orElseThrow(exceptionSupplier(key));
    }

    /** Returns the DataType under the given key if it exists. */
    public Optional<DataType> getOptionalDataType(String key) {
        return optionalGet(key)
                .map(t -> TypeConversions.fromLogicalToDataType(LogicalTypeParser.parse(t)));
    }

    /** Returns the DataType under the given existing key. */
    public DataType getDataType(String key) {
        return getOptionalDataType(key).orElseThrow(exceptionSupplier(key));
    }

    /** Returns a table schema under the given key if it exists. */
    public Optional<TableSchema> getOptionalTableSchema(String key) {
        // filter for number of fields
        final int fieldCount =
                properties.keySet().stream()
                        .filter(
                                (k) ->
                                        k.startsWith(key)
                                                // "key." is the prefix.
                                                && SCHEMA_COLUMN_NAME_SUFFIX
                                                        .matcher(k.substring(key.length() + 1))
                                                        .matches())
                        .mapToInt((k) -> 1)
                        .sum();

        if (fieldCount == 0) {
            return Optional.empty();
        }

        // validate fields and build schema
        final TableSchema.Builder schemaBuilder = TableSchema.builder();
        for (int i = 0; i < fieldCount; i++) {
            final String nameKey = key + '.' + i + '.' + NAME;
            final String legacyTypeKey = key + '.' + i + '.' + TYPE;
            final String typeKey = key + '.' + i + '.' + DATA_TYPE;
            final String exprKey = key + '.' + i + '.' + EXPR;
            final String metadataKey = key + '.' + i + '.' + METADATA;
            final String virtualKey = key + '.' + i + '.' + VIRTUAL;

            final String name = optionalGet(nameKey).orElseThrow(exceptionSupplier(nameKey));

            final DataType type;
            if (containsKey(typeKey)) {
                type = getDataType(typeKey);
            } else if (containsKey(legacyTypeKey)) {
                type = TypeConversions.fromLegacyInfoToDataType(getType(legacyTypeKey));
            } else {
                throw exceptionSupplier(typeKey).get();
            }

            final Optional<String> expr = optionalGet(exprKey);
            final Optional<String> metadata = optionalGet(metadataKey);
            final boolean virtual = getOptionalBoolean(virtualKey).orElse(false);
            // computed column
            if (expr.isPresent()) {
                schemaBuilder.add(TableColumn.computed(name, type, expr.get()));
            }
            // metadata column
            else if (metadata.isPresent()) {
                final String metadataAlias = metadata.get();
                if (metadataAlias.equals(name)) {
                    schemaBuilder.add(TableColumn.metadata(name, type, virtual));
                } else {
                    schemaBuilder.add(TableColumn.metadata(name, type, metadataAlias, virtual));
                }
            }
            // physical column
            else {
                schemaBuilder.add(TableColumn.physical(name, type));
            }
        }

        // extract watermark information

        // filter for number of fields
        String watermarkPrefixKey = key + '.' + WATERMARK;
        final int watermarkCount =
                properties.keySet().stream()
                        .filter(
                                (k) ->
                                        k.startsWith(watermarkPrefixKey)
                                                && k.endsWith('.' + WATERMARK_ROWTIME))
                        .mapToInt((k) -> 1)
                        .sum();
        if (watermarkCount > 0) {
            for (int i = 0; i < watermarkCount; i++) {
                final String rowtimeKey = watermarkPrefixKey + '.' + i + '.' + WATERMARK_ROWTIME;
                final String exprKey = watermarkPrefixKey + '.' + i + '.' + WATERMARK_STRATEGY_EXPR;
                final String typeKey =
                        watermarkPrefixKey + '.' + i + '.' + WATERMARK_STRATEGY_DATA_TYPE;
                final String rowtime =
                        optionalGet(rowtimeKey).orElseThrow(exceptionSupplier(rowtimeKey));
                final String exprString =
                        optionalGet(exprKey).orElseThrow(exceptionSupplier(exprKey));
                final String typeString =
                        optionalGet(typeKey).orElseThrow(exceptionSupplier(typeKey));
                final DataType exprType =
                        TypeConversions.fromLogicalToDataType(LogicalTypeParser.parse(typeString));
                schemaBuilder.watermark(rowtime, exprString, exprType);
            }
        }

        // Extract unique constraints.
        String pkConstraintNameKey = key + '.' + PRIMARY_KEY_NAME;
        final Optional<String> pkConstraintNameOpt = optionalGet(pkConstraintNameKey);
        if (pkConstraintNameOpt.isPresent()) {
            final String pkColumnsKey = key + '.' + PRIMARY_KEY_COLUMNS;
            final String columns =
                    optionalGet(pkColumnsKey).orElseThrow(exceptionSupplier(pkColumnsKey));
            schemaBuilder.primaryKey(pkConstraintNameOpt.get(), columns.split(","));
        }
        return Optional.of(schemaBuilder.build());
    }

    /** Returns a table schema under the given existing key. */
    public TableSchema getTableSchema(String key) {
        return getOptionalTableSchema(key).orElseThrow(exceptionSupplier(key));
    }

    /** Returns partition keys. */
    public List<String> getPartitionKeys() {
        return getFixedIndexedProperties(PARTITION_KEYS, Collections.singletonList(NAME)).stream()
                .map(map -> map.values().iterator().next())
                .map(this::getString)
                .collect(Collectors.toList());
    }

    /** Returns a Flink {@link MemorySize} under the given key if it exists. */
    public Optional<MemorySize> getOptionalMemorySize(String key) {
        return optionalGet(key)
                .map(
                        (value) -> {
                            try {
                                return MemorySize.parse(value, MemorySize.MemoryUnit.BYTES);
                            } catch (Exception e) {
                                throw new ValidationException(
                                        "Invalid memory size value for key '" + key + "'.", e);
                            }
                        });
    }

    /** Returns a Flink {@link MemorySize} under the given existing key. */
    public MemorySize getMemorySize(String key) {
        return getOptionalMemorySize(key).orElseThrow(exceptionSupplier(key));
    }

    /** Returns a Java {@link Duration} under the given key if it exists. */
    public Optional<Duration> getOptionalDuration(String key) {
        return optionalGet(key)
                .map(
                        (value) -> {
                            try {
                                return TimeUtils.parseDuration(value);
                            } catch (Exception e) {
                                throw new ValidationException(
                                        "Invalid duration value for key '" + key + "'.", e);
                            }
                        });
    }

    /** Returns a java {@link Duration} under the given existing key. */
    public Duration getDuration(String key) {
        return getOptionalDuration(key).orElseThrow(exceptionSupplier(key));
    }

    /**
     * Returns the property keys of fixed indexed properties.
     *
     * <p>For example:
     *
     * <pre>
     *     schema.fields.0.type = INT, schema.fields.0.name = test
     *     schema.fields.1.type = LONG, schema.fields.1.name = test2
     * </pre>
     *
     * <p>getFixedIndexedProperties("schema.fields", List("type", "name")) leads to:
     *
     * <pre>
     *     0: Map("type" -> "schema.fields.0.type", "name" -> "schema.fields.0.name")
     *     1: Map("type" -> "schema.fields.1.type", "name" -> "schema.fields.1.name")
     * </pre>
     */
    public List<Map<String, String>> getFixedIndexedProperties(String key, List<String> subKeys) {
        // determine max index
        final int maxIndex = extractMaxIndex(key, "\\.(.*)");

        // validate and create result
        final List<Map<String, String>> list = new ArrayList<>();
        for (int i = 0; i <= maxIndex; i++) {
            final Map<String, String> map = new HashMap<>();

            for (String subKey : subKeys) {
                final String fullKey = key + '.' + i + '.' + subKey;
                // check for existence of full key
                if (!containsKey(fullKey)) {
                    throw exceptionSupplier(fullKey).get();
                }
                map.put(subKey, fullKey);
            }

            list.add(map);
        }
        return list;
    }

    /**
     * Returns the property keys of variable indexed properties.
     *
     * <p>For example:
     *
     * <pre>
     *     schema.fields.0.type = INT, schema.fields.0.name = test
     *     schema.fields.1.type = LONG
     * </pre>
     *
     * <p>getFixedIndexedProperties("schema.fields", List("type")) leads to:
     *
     * <pre>
     *     0: Map("type" -> "schema.fields.0.type", "name" -> "schema.fields.0.name")
     *     1: Map("type" -> "schema.fields.1.type")
     * </pre>
     */
    public List<Map<String, String>> getVariableIndexedProperties(
            String key, List<String> requiredSubKeys) {

        // determine max index
        final int maxIndex = extractMaxIndex(key, "(\\.)?(.*)");

        // determine optional properties
        final String escapedKey = Pattern.quote(key);
        final Pattern pattern = Pattern.compile(escapedKey + "\\.(\\d+)(\\.)?(.*)");
        final Set<String> optionalSubKeys =
                properties.keySet().stream()
                        .flatMap(
                                (k) -> {
                                    final Matcher matcher = pattern.matcher(k);
                                    if (matcher.find()) {
                                        return Stream.of(matcher.group(3));
                                    }
                                    return Stream.empty();
                                })
                        .filter((k) -> k.length() > 0)
                        .collect(Collectors.toSet());

        // validate and create result
        final List<Map<String, String>> list = new ArrayList<>();
        for (int i = 0; i <= maxIndex; i++) {
            final Map<String, String> map = new HashMap<>();

            // check and add required keys
            for (String subKey : requiredSubKeys) {
                final String fullKey = key + '.' + i + '.' + subKey;
                // check for existence of full key
                if (!containsKey(fullKey)) {
                    throw exceptionSupplier(fullKey).get();
                }
                map.put(subKey, fullKey);
            }

            // add optional keys
            for (String subKey : optionalSubKeys) {
                final String fullKey = key + '.' + i + '.' + subKey;
                optionalGet(fullKey).ifPresent(value -> map.put(subKey, fullKey));
            }

            list.add(map);
        }
        return list;
    }

    /**
     * Returns all properties under a given key that contains an index in between.
     *
     * <p>E.g. rowtime.0.name -> returns all rowtime.#.name properties
     */
    public Map<String, String> getIndexedProperty(String key, String subKey) {
        final String escapedKey = Pattern.quote(key);
        final String escapedSubKey = Pattern.quote(subKey);
        return properties.entrySet().stream()
                .filter(entry -> entry.getKey().matches(escapedKey + "\\.\\d+\\." + escapedSubKey))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    /**
     * Returns all array elements under a given key if it exists.
     *
     * <p>For example:
     *
     * <pre>
     *     primary-key.0 = field1
     *     primary-key.1 = field2
     * </pre>
     *
     * <p>leads to: List(field1, field2)
     *
     * <p>or:
     *
     * <pre>
     *     primary-key = field1
     * </pre>
     *
     * <p>leads to: List(field1)
     *
     * <p>The key mapper gets the key of the current value e.g. "primary-key.1".
     */
    public <E> Optional<List<E>> getOptionalArray(String key, Function<String, E> keyMapper) {
        // determine max index
        final int maxIndex = extractMaxIndex(key, "");

        if (maxIndex < 0) {
            // check for a single element array
            if (containsKey(key)) {
                return Optional.of(Collections.singletonList(keyMapper.apply(key)));
            } else {
                return Optional.empty();
            }
        } else {
            final List<E> list = new ArrayList<>();
            for (int i = 0; i < maxIndex + 1; i++) {
                final String fullKey = key + '.' + i;
                final E value = keyMapper.apply(fullKey);
                list.add(value);
            }
            return Optional.of(list);
        }
    }

    /** Returns all array elements under a given existing key. */
    public <E> List<E> getArray(String key, Function<String, E> keyMapper) {
        return getOptionalArray(key, keyMapper).orElseThrow(exceptionSupplier(key));
    }

    /** Returns if a value under key is exactly equal to the given value. */
    public boolean isValue(String key, String value) {
        return optionalGet(key).orElseThrow(exceptionSupplier(key)).equals(value);
    }

    /**
     * Returns a map of properties whose key starts with the given prefix, and the prefix is removed
     * upon return.
     *
     * <p>For example, for prefix "flink" and a map of a single property with key "flink.k" and
     * value "v", this method will return it as key "k" and value "v" by identifying and removing
     * the prefix "flink".
     */
    public Map<String, String> getPropertiesWithPrefix(String prefix) {
        String prefixWithDot = prefix + '.';

        return properties.entrySet().stream()
                .filter(e -> e.getKey().startsWith(prefixWithDot))
                .collect(
                        Collectors.toMap(
                                e -> e.getKey().substring(prefix.length() + 1),
                                Map.Entry::getValue));
    }

    // --------------------------------------------------------------------------------------------

    /** Validates a string property. */
    public void validateString(String key, boolean isOptional) {
        validateString(key, isOptional, 0, Integer.MAX_VALUE);
    }

    /** Validates a string property. The boundaries are inclusive. */
    public void validateString(String key, boolean isOptional, int minLen) {
        validateString(key, isOptional, minLen, Integer.MAX_VALUE);
    }

    /** Validates a string property. The boundaries are inclusive. */
    public void validateString(String key, boolean isOptional, int minLen, int maxLen) {
        validateOptional(
                key,
                isOptional,
                (value) -> {
                    final int length = value.length();
                    if (length < minLen || length > maxLen) {
                        throw new ValidationException(
                                "Property '"
                                        + key
                                        + "' must have a length between "
                                        + minLen
                                        + " and "
                                        + maxLen
                                        + " but was: "
                                        + value);
                    }
                });
    }

    /** Validates an integer property. */
    public void validateInt(String key, boolean isOptional) {
        validateInt(key, isOptional, Integer.MIN_VALUE, Integer.MAX_VALUE);
    }

    /** Validates an integer property. The boundaries are inclusive. */
    public void validateInt(String key, boolean isOptional, int min) {
        validateInt(key, isOptional, min, Integer.MAX_VALUE);
    }

    /** Validates an integer property. The boundaries are inclusive. */
    public void validateInt(String key, boolean isOptional, int min, int max) {
        validateComparable(key, isOptional, min, max, "integer", Integer::valueOf);
    }

    /** Validates an long property. */
    public void validateLong(String key, boolean isOptional) {
        validateLong(key, isOptional, Long.MIN_VALUE, Long.MAX_VALUE);
    }

    /** Validates an long property. The boundaries are inclusive. */
    public void validateLong(String key, boolean isOptional, long min) {
        validateLong(key, isOptional, min, Long.MAX_VALUE);
    }

    /** Validates an long property. The boundaries are inclusive. */
    public void validateLong(String key, boolean isOptional, long min, long max) {
        validateComparable(key, isOptional, min, max, "long", Long::valueOf);
    }

    /** Validates that a certain value is present under the given key. */
    public void validateValue(String key, String value, boolean isOptional) {
        validateOptional(
                key,
                isOptional,
                (v) -> {
                    if (!v.equals(value)) {
                        throw new ValidationException(
                                "Could not find required value '"
                                        + value
                                        + "' for property '"
                                        + key
                                        + "'.");
                    }
                });
    }

    /** Validates that a boolean value is present under the given key. */
    public void validateBoolean(String key, boolean isOptional) {
        validateOptional(
                key,
                isOptional,
                (value) -> {
                    if (!value.equalsIgnoreCase("true") && !value.equalsIgnoreCase("false")) {
                        throw new ValidationException(
                                "Property '"
                                        + key
                                        + "' must be a boolean value (true/false) but was: "
                                        + value);
                    }
                });
    }

    /** Validates a double property. */
    public void validateDouble(String key, boolean isOptional) {
        validateDouble(key, isOptional, Double.MIN_VALUE, Double.MAX_VALUE);
    }

    /** Validates a double property. The boundaries are inclusive. */
    public void validateDouble(String key, boolean isOptional, double min) {
        validateDouble(key, isOptional, min, Double.MAX_VALUE);
    }

    /** Validates a double property. The boundaries are inclusive. */
    public void validateDouble(String key, boolean isOptional, double min, double max) {
        validateComparable(key, isOptional, min, max, "double", Double::valueOf);
    }

    /** Validates a big decimal property. */
    public void validateBigDecimal(String key, boolean isOptional) {
        validateOptional(
                key,
                isOptional,
                (value) -> {
                    try {
                        new BigDecimal(value);
                    } catch (Exception e) {
                        throw new ValidationException(
                                "Property '"
                                        + key
                                        + "' must be a big decimal value but was: "
                                        + value);
                    }
                });
    }

    /** Validates a big decimal property. The boundaries are inclusive. */
    public void validateBigDecimal(String key, boolean isOptional, BigDecimal min, BigDecimal max) {
        validateComparable(key, isOptional, min, max, "decimal", BigDecimal::new);
    }

    /** Validates a byte property. */
    public void validateByte(String key, boolean isOptional) {
        validateByte(key, isOptional, Byte.MIN_VALUE, Byte.MAX_VALUE);
    }

    /** Validates a byte property. The boundaries are inclusive. */
    public void validateByte(String key, boolean isOptional, byte min) {
        validateByte(key, isOptional, min, Byte.MAX_VALUE);
    }

    /** Validates a byte property. The boundaries are inclusive. */
    public void validateByte(String key, boolean isOptional, byte min, byte max) {
        validateComparable(key, isOptional, min, max, "byte", Byte::valueOf);
    }

    /** Validates a float property. */
    public void validateFloat(String key, boolean isOptional) {
        validateFloat(key, isOptional, Float.MIN_VALUE, Float.MAX_VALUE);
    }

    /** Validates a float property. The boundaries are inclusive. */
    public void validateFloat(String key, boolean isOptional, float min) {
        validateFloat(key, isOptional, min, Float.MAX_VALUE);
    }

    /** Validates a float property. The boundaries are inclusive. */
    public void validateFloat(String key, boolean isOptional, float min, float max) {
        validateComparable(key, isOptional, min, max, "float", Float::valueOf);
    }

    /** Validates a short property. */
    public void validateShort(String key, boolean isOptional) {
        validateShort(key, isOptional, Short.MIN_VALUE, Short.MAX_VALUE);
    }

    /** Validates a short property. The boundaries are inclusive. */
    public void validateShort(String key, boolean isOptional, short min) {
        validateShort(key, isOptional, min, Short.MAX_VALUE);
    }

    /** Validates a short property. The boundaries are inclusive. */
    public void validateShort(String key, boolean isOptional, short min, short max) {
        validateComparable(key, isOptional, min, max, "short", Short::valueOf);
    }

    /**
     * Validation for fixed indexed properties.
     *
     * <p>For example:
     *
     * <pre>
     *     schema.fields.0.data-type = INT, schema.fields.0.name = test
     *     schema.fields.1.data-type = BIGINT, schema.fields.1.name = test2
     * </pre>
     *
     * <p>The subKeyValidation map must define e.g. "data-type" and "name" and a validation logic
     * for the given full key.
     */
    public void validateFixedIndexedProperties(
            String key, boolean allowEmpty, Map<String, Consumer<String>> subKeyValidation) {
        // determine max index
        final int maxIndex = extractMaxIndex(key, "\\.(.*)");

        if (maxIndex < 0 && !allowEmpty) {
            throw new ValidationException("Property key '" + key + "' must not be empty.");
        }

        // validate
        for (int i = 0; i <= maxIndex; i++) {
            for (Map.Entry<String, Consumer<String>> subKey : subKeyValidation.entrySet()) {
                final String fullKey = key + '.' + i + '.' + subKey.getKey();
                // run validation logic
                subKey.getValue().accept(fullKey);
            }
        }
    }

    /** Validates a table schema property. */
    public void validateTableSchema(String key, boolean isOptional) {
        final Consumer<String> nameValidation = (fullKey) -> validateString(fullKey, false, 1);
        final Consumer<String> typeValidation =
                (fullKey) -> {
                    String fallbackKey = fullKey.replace("." + DATA_TYPE, "." + TYPE);
                    validateDataType(fullKey, fallbackKey, false);
                };

        final Map<String, Consumer<String>> subKeys = new HashMap<>();
        subKeys.put(NAME, nameValidation);
        subKeys.put(DATA_TYPE, typeValidation);

        validateFixedIndexedProperties(key, isOptional, subKeys);
    }

    /**
     * Validates a Flink {@link MemorySize}.
     *
     * <p>The precision defines the allowed minimum unit in bytes (e.g. 1024 would only allow KB).
     */
    public void validateMemorySize(String key, boolean isOptional, int precision) {
        validateMemorySize(key, isOptional, precision, 0L, Long.MAX_VALUE);
    }

    /**
     * Validates a Flink {@link MemorySize}. The boundaries are inclusive and in bytes.
     *
     * <p>The precision defines the allowed minimum unit in bytes (e.g. 1024 would only allow KB).
     */
    public void validateMemorySize(String key, boolean isOptional, int precision, long min) {
        validateMemorySize(key, isOptional, precision, min, Long.MAX_VALUE);
    }

    /**
     * Validates a Flink {@link MemorySize}. The boundaries are inclusive and in bytes.
     *
     * <p>The precision defines the allowed minimum unit in bytes (e.g. 1024 would only allow KB).
     */
    public void validateMemorySize(
            String key, boolean isOptional, int precision, long min, long max) {
        Preconditions.checkArgument(precision > 0);

        validateComparable(
                key,
                isOptional,
                min,
                max,
                "memory size (in bytes)",
                (value) -> {
                    final long bytes =
                            MemorySize.parse(value, MemorySize.MemoryUnit.BYTES).getBytes();
                    if (bytes % precision != 0) {
                        throw new ValidationException(
                                "Memory size for key '"
                                        + key
                                        + "' must be a multiple of "
                                        + precision
                                        + " bytes but was: "
                                        + value);
                    }
                    return bytes;
                });
    }

    /**
     * Validates a Java {@link Duration}.
     *
     * <p>The precision defines the allowed minimum unit in milliseconds (e.g. 1000 would only allow
     * seconds).
     */
    public void validateDuration(String key, boolean isOptional, int precision) {
        validateDuration(key, isOptional, precision, 0L, Long.MAX_VALUE);
    }

    /**
     * Validates a Java {@link Duration}. The boundaries are inclusive and in milliseconds.
     *
     * <p>The precision defines the allowed minimum unit in milliseconds (e.g. 1000 would only allow
     * seconds).
     */
    public void validateDuration(String key, boolean isOptional, int precision, long min) {
        validateDuration(key, isOptional, precision, min, Long.MAX_VALUE);
    }

    /**
     * Validates a Java {@link Duration}. The boundaries are inclusive and in milliseconds.
     *
     * <p>The precision defines the allowed minimum unit in milliseconds (e.g. 1000 would only allow
     * seconds).
     */
    public void validateDuration(
            String key, boolean isOptional, int precision, long min, long max) {
        Preconditions.checkArgument(precision > 0);

        validateComparable(
                key,
                isOptional,
                min,
                max,
                "time interval (in milliseconds)",
                (value) -> {
                    final long ms = TimeUtils.parseDuration(value).toMillis();
                    if (ms % precision != 0) {
                        throw new ValidationException(
                                "Duration for key '"
                                        + key
                                        + "' must be a multiple of "
                                        + precision
                                        + " milliseconds but was: "
                                        + value);
                    }
                    return ms;
                });
    }

    /** Validates an enum property with a set of validation logic for each enum value. */
    public void validateEnum(
            String key, boolean isOptional, Map<String, Consumer<String>> enumValidation) {
        validateOptional(
                key,
                isOptional,
                (value) -> {
                    if (!enumValidation.containsKey(value)) {
                        throw new ValidationException(
                                "Unknown value for property '"
                                        + key
                                        + "'.\n"
                                        + "Supported values are "
                                        + enumValidation.keySet()
                                        + " but was: "
                                        + value);
                    } else {
                        // run validation logic
                        enumValidation.get(value).accept(key);
                    }
                });
    }

    /** Validates an enum property with a set of enum values. */
    public void validateEnumValues(String key, boolean isOptional, List<String> values) {
        validateEnum(
                key,
                isOptional,
                values.stream().collect(Collectors.toMap(v -> v, v -> noValidation())));
    }

    /** Validates a type property. */
    public void validateType(String key, boolean isOptional, boolean requireRow) {
        validateOptional(
                key,
                isOptional,
                (value) -> {
                    // we don't validate the string but let the parser do the work for us
                    // it throws a validation exception
                    final TypeInformation<?> typeInfo = TypeStringUtils.readTypeInfo(value);
                    if (requireRow && !(typeInfo instanceof RowTypeInfo)) {
                        throw new ValidationException(
                                "Row type information expected for key '"
                                        + key
                                        + "' but was: "
                                        + value);
                    }
                });
    }

    /** Validates a data type property. */
    public void validateDataType(String key, String fallbackKey, boolean isOptional) {
        if (properties.containsKey(key)) {
            validateOptional(
                    key,
                    isOptional,
                    // we don't validate the string but let the parser do the work for us
                    // it throws a validation exception
                    v -> {
                        LogicalType t = LogicalTypeParser.parse(v);
                        if (t.getTypeRoot() == LogicalTypeRoot.UNRESOLVED) {
                            throw new ValidationException(
                                    "Could not parse type string '" + v + "'.");
                        }
                    });
        } else if (fallbackKey != null && properties.containsKey(fallbackKey)) {
            validateOptional(
                    fallbackKey,
                    isOptional,
                    // we don't validate the string but let the parser do the work for us
                    // it throws a validation exception
                    TypeStringUtils::readTypeInfo);
        } else {
            if (!isOptional) {
                throw new ValidationException("Could not find required property '" + key + "'.");
            }
        }
    }

    /**
     * Validates an array of values.
     *
     * <p>For example:
     *
     * <pre>
     *     primary-key.0 = field1
     *     primary-key.1 = field2
     * </pre>
     *
     * <p>leads to: List(field1, field2)
     *
     * <p>or:
     *
     * <pre>
     *     primary-key = field1
     * </pre>
     *
     * <p>The validation consumer gets the key of the current value e.g. "primary-key.1".
     */
    public void validateArray(String key, Consumer<String> elementValidation, int minLength) {
        validateArray(key, elementValidation, minLength, Integer.MAX_VALUE);
    }

    /**
     * Validates an array of values.
     *
     * <p>For example:
     *
     * <pre>
     *     primary-key.0 = field1
     *     primary-key.1 = field2
     * </pre>
     *
     * <p>leads to: List(field1, field2)
     *
     * <p>or:
     *
     * <pre>
     *     primary-key = field1
     * </pre>
     *
     * <p>The validation consumer gets the key of the current value e.g. "primary-key.1".
     */
    public void validateArray(
            String key, Consumer<String> elementValidation, int minLength, int maxLength) {

        // determine max index
        final int maxIndex = extractMaxIndex(key, "");

        if (maxIndex < 0) {
            // check for a single element array
            if (properties.containsKey(key)) {
                elementValidation.accept(key);
            } else if (minLength > 0) {
                throw new ValidationException(
                        "Could not find required property array for key '" + key + "'.");
            }
        } else {
            // do not allow a single element array
            if (properties.containsKey(key)) {
                throw new ValidationException("Invalid property array for key '" + key + "'.");
            }

            final int size = maxIndex + 1;
            if (size < minLength) {
                throw new ValidationException(
                        "Array for key '"
                                + key
                                + "' must not have less than "
                                + minLength
                                + " elements but was: "
                                + size);
            }

            if (size > maxLength) {
                throw new ValidationException(
                        "Array for key '"
                                + key
                                + "' must not have more than "
                                + maxLength
                                + " elements but was: "
                                + size);
            }
        }

        // validate array elements
        for (int i = 0; i <= maxIndex; i++) {
            final String fullKey = key + '.' + i;
            if (properties.containsKey(fullKey)) {
                // run validation logic
                elementValidation.accept(fullKey);
            } else {
                throw new ValidationException(
                        "Required array element at index '"
                                + i
                                + "' for key '"
                                + key
                                + "' is missing.");
            }
        }
    }

    /** Validates that the given prefix is not included in these properties. */
    public void validatePrefixExclusion(String prefix) {
        properties.keySet().stream()
                .filter(k -> k.startsWith(prefix))
                .findFirst()
                .ifPresent(
                        (k) -> {
                            throw new ValidationException(
                                    "Properties with prefix '"
                                            + prefix
                                            + "' are not allowed in this context. "
                                            + "But property '"
                                            + k
                                            + "' was found.");
                        });
    }

    /** Validates that the given key is not included in these properties. */
    public void validateExclusion(String key) {
        if (properties.containsKey(key)) {
            throw new ValidationException("Property '" + key + "' is not allowed in this context.");
        }
    }

    // --------------------------------------------------------------------------------------------

    /** Returns if the given key is contained. */
    public boolean containsKey(String key) {
        return properties.containsKey(key);
    }

    /** Returns if a given prefix exists in the properties. */
    public boolean hasPrefix(String prefix) {
        return properties.keySet().stream().anyMatch(k -> k.startsWith(prefix));
    }

    /** Returns the properties as a map copy. */
    public Map<String, String> asMap() {
        final Map<String, String> copy = new HashMap<>(properties);
        return Collections.unmodifiableMap(copy);
    }

    /** Returns the properties as a map copy with a prefix key. */
    public Map<String, String> asPrefixedMap(String prefix) {
        return properties.entrySet().stream()
                .collect(Collectors.toMap(e -> prefix + e.getKey(), Map.Entry::getValue));
    }

    /** Returns a new properties instance with the given keys removed. */
    public DescriptorProperties withoutKeys(List<String> keys) {
        final Set<String> keySet = new HashSet<>(keys);
        final DescriptorProperties copy = new DescriptorProperties(normalizeKeys);
        properties.entrySet().stream()
                .filter(e -> !keySet.contains(e.getKey()))
                .forEach(e -> copy.properties.put(e.getKey(), e.getValue()));
        return copy;
    }

    @Override
    public String toString() {
        return toString(properties);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DescriptorProperties that = (DescriptorProperties) o;
        return Objects.equals(properties, that.properties);
    }

    @Override
    public int hashCode() {
        return Objects.hash(properties);
    }

    // --------------------------------------------------------------------------------------------

    private void put(String key, String value) {
        if (properties.containsKey(key)) {
            throw new ValidationException("Property already present: " + key);
        }
        if (normalizeKeys) {
            properties.put(key.toLowerCase(), value);
        } else {
            properties.put(key, value);
        }
    }

    private Optional<String> optionalGet(String key) {
        return Optional.ofNullable(properties.get(key));
    }

    private void validateOptional(
            String key, boolean isOptional, Consumer<String> valueValidation) {
        if (!properties.containsKey(key)) {
            if (!isOptional) {
                throw new ValidationException("Could not find required property '" + key + "'.");
            }
        } else {
            final String value = properties.get(key);
            valueValidation.accept(value);
        }
    }

    private Supplier<TableException> exceptionSupplier(String key) {
        return () -> {
            throw new TableException(
                    "Property with key '"
                            + key
                            + "' could not be found. "
                            + "This is a bug because the validation logic should have checked that before.");
        };
    }

    private int extractMaxIndex(String key, String suffixPattern) {
        // extract index and property keys
        final String escapedKey = Pattern.quote(key);
        final Pattern pattern = Pattern.compile(escapedKey + "\\.(\\d+)" + suffixPattern);
        final IntStream indexes =
                properties.keySet().stream()
                        .flatMapToInt(
                                k -> {
                                    final Matcher matcher = pattern.matcher(k);
                                    if (matcher.find()) {
                                        return IntStream.of(Integer.valueOf(matcher.group(1)));
                                    }
                                    return IntStream.empty();
                                });

        // determine max index
        return indexes.max().orElse(-1);
    }

    /**
     * Validates a property by first parsing the string value to a comparable object. The boundaries
     * are inclusive.
     */
    private <T extends Comparable<T>> void validateComparable(
            String key,
            boolean isOptional,
            T min,
            T max,
            String typeName,
            Function<String, T> parseFunction) {
        if (!properties.containsKey(key)) {
            if (!isOptional) {
                throw new ValidationException("Could not find required property '" + key + "'.");
            }
        } else {
            final String value = properties.get(key);
            try {
                final T parsed = parseFunction.apply(value);
                if (parsed.compareTo(min) < 0 || parsed.compareTo(max) > 0) {
                    throw new ValidationException(
                            "Property '"
                                    + key
                                    + "' must be a "
                                    + typeName
                                    + " value between "
                                    + min
                                    + " and "
                                    + max
                                    + " but was: "
                                    + parsed);
                }
            } catch (Exception e) {
                throw new ValidationException(
                        "Property '"
                                + key
                                + "' must be a "
                                + typeName
                                + " value but was: "
                                + value);
            }
        }
    }

    // --------------------------------------------------------------------------------------------

    /** Returns an empty validation logic. */
    public static Consumer<String> noValidation() {
        return EMPTY_CONSUMER;
    }

    public static String toString(String str) {
        return EncodingUtils.escapeJava(str);
    }

    public static String toString(String key, String value) {
        return toString(key) + '=' + toString(value);
    }

    public static String toString(Map<String, String> propertyMap) {
        return propertyMap.entrySet().stream()
                .map(e -> toString(e.getKey(), e.getValue()))
                .sorted()
                .collect(Collectors.joining("\n"));
    }
}
