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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.UnresolvedUserDefinedType;
import org.apache.flink.table.types.logical.utils.LogicalTypeParser;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.table.utils.TypeStringUtils;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.flink.table.descriptors.FormatDescriptorValidator.FORMAT_DERIVE_SCHEMA;
import static org.apache.flink.table.descriptors.OldCsvValidator.FORMAT_COMMENT_PREFIX;
import static org.apache.flink.table.descriptors.OldCsvValidator.FORMAT_FIELDS;
import static org.apache.flink.table.descriptors.OldCsvValidator.FORMAT_FIELD_DELIMITER;
import static org.apache.flink.table.descriptors.OldCsvValidator.FORMAT_IGNORE_FIRST_LINE;
import static org.apache.flink.table.descriptors.OldCsvValidator.FORMAT_IGNORE_PARSE_ERRORS;
import static org.apache.flink.table.descriptors.OldCsvValidator.FORMAT_LINE_DELIMITER;
import static org.apache.flink.table.descriptors.OldCsvValidator.FORMAT_NUM_FILES;
import static org.apache.flink.table.descriptors.OldCsvValidator.FORMAT_QUOTE_CHARACTER;
import static org.apache.flink.table.descriptors.OldCsvValidator.FORMAT_TYPE_VALUE;
import static org.apache.flink.table.descriptors.OldCsvValidator.FORMAT_WRITE_MODE;

/**
 * Format descriptor for comma-separated values (CSV).
 *
 * <p>Note: This descriptor describes Flink's non-standard CSV table source/sink. In the future, the
 * descriptor will be replaced by a proper RFC-compliant version. Use the RFC-compliant {@code Csv}
 * format in the dedicated flink-formats/flink-csv module instead when writing to Kafka. Use the old
 * one for stream/batch filesystem operations for now.
 *
 * @deprecated Use the RFC-compliant {@code Csv} format in the dedicated flink-formats/flink-csv
 *     module instead when writing to Kafka.
 */
@Deprecated
@PublicEvolving
public class OldCsv extends FormatDescriptor {

    private Optional<String> fieldDelim = Optional.empty();
    private Optional<String> lineDelim = Optional.empty();
    private Map<String, String> schema = new LinkedHashMap<>();
    private Optional<Character> quoteCharacter = Optional.empty();
    private Optional<String> commentPrefix = Optional.empty();
    private Optional<Boolean> isIgnoreFirstLine = Optional.empty();
    private Optional<Boolean> lenient = Optional.empty();
    private Optional<Boolean> deriveSchema = Optional.empty();
    private Optional<String> writeMode = Optional.empty();
    private Optional<Integer> numFiles = Optional.empty();

    public OldCsv() {
        super(FORMAT_TYPE_VALUE, 1);
    }

    /**
     * Sets the field delimiter, "," by default.
     *
     * @param delim the field delimiter
     */
    public OldCsv fieldDelimiter(String delim) {
        this.fieldDelim = Optional.of(delim);
        return this;
    }

    /**
     * Sets the line delimiter, "\n" by default.
     *
     * @param delim the line delimiter
     */
    public OldCsv lineDelimiter(String delim) {
        this.lineDelim = Optional.of(delim);
        return this;
    }

    /**
     * Sets the format schema with field names and the types. Required. The table schema must not
     * contain nested fields.
     *
     * <p>This method overwrites existing fields added with [[field()]].
     *
     * @param schema the table schema
     * @deprecated {@link OldCsv} supports derive schema from table schema by default, it is no
     *     longer necessary to explicitly declare the format schema. This method will be removed in
     *     the future.
     */
    @Deprecated
    public OldCsv schema(TableSchema schema) {
        this.schema.clear();
        for (int i = 0; i < schema.getFieldCount(); ++i) {
            field(schema.getFieldNames()[i], schema.getFieldTypes()[i]);
        }
        return this;
    }

    /**
     * Adds a format field with the field name and the type information. Required. This method can
     * be called multiple times. The call order of this method defines also the order of the fields
     * in the format.
     *
     * @param fieldName the field name
     * @param fieldType the type information of the field
     * @deprecated {@link OldCsv} supports derive schema from table schema by default, it is no
     *     longer necessary to explicitly declare the format schema. This method will be removed in
     *     the future.
     */
    @Deprecated
    public OldCsv field(String fieldName, TypeInformation<?> fieldType) {
        field(fieldName, TypeConversions.fromLegacyInfoToDataType(fieldType));
        return this;
    }

    /**
     * Adds a format field with the field name and the type information. Required. This method can
     * be called multiple times. The call order of this method defines also the order of the fields
     * in the format.
     *
     * @param fieldName the field name
     * @param fieldType the type information of the field
     * @deprecated {@link OldCsv} supports derive schema from table schema by default, it is no
     *     longer necessary to explicitly declare the format schema. This method will be removed in
     *     the future.
     */
    @Deprecated
    public OldCsv field(String fieldName, DataType fieldType) {
        addField(fieldName, fieldType.getLogicalType().asSerializableString());
        return this;
    }

    /**
     * Adds a format field with the field name and the type string. Required. This method can be
     * called multiple times. The call order of this method defines also the order of the fields in
     * the format.
     *
     * <p>NOTE: the fieldType string should follow the type string defined in {@link
     * LogicalTypeParser}. This method also keeps compatible with old type string defined in {@link
     * TypeStringUtils} but will be dropped in future versions as it uses the old type system.
     *
     * @param fieldName the field name
     * @param fieldType the type string of the field
     * @deprecated {@link OldCsv} supports derive schema from table schema by default, it is no
     *     longer necessary to explicitly declare the format schema. This method will be removed in
     *     the future.
     */
    @Deprecated
    public OldCsv field(String fieldName, String fieldType) {
        if (isLegacyTypeString(fieldType)) {
            // fallback to legacy parser
            TypeInformation<?> typeInfo = TypeStringUtils.readTypeInfo(fieldType);
            return field(fieldName, TypeConversions.fromLegacyInfoToDataType(typeInfo));
        } else {
            return addField(fieldName, fieldType);
        }
    }

    private OldCsv addField(String fieldName, String fieldType) {
        if (schema.containsKey(fieldName)) {
            throw new ValidationException("Duplicate field name " + fieldName + ".");
        }
        schema.put(fieldName, fieldType);
        return this;
    }

    private static boolean isLegacyTypeString(String fieldType) {
        try {
            LogicalType type = LogicalTypeParser.parse(fieldType);
            return type instanceof UnresolvedUserDefinedType;
        } catch (Exception e) {
            // if the parsing failed, fallback to the legacy parser
            return true;
        }
    }

    /**
     * Sets a quote character for String values, null by default.
     *
     * @param quote the quote character
     */
    public OldCsv quoteCharacter(Character quote) {
        this.quoteCharacter = Optional.of(quote);
        return this;
    }

    /**
     * Sets a prefix to indicate comments, null by default.
     *
     * @param prefix the prefix to indicate comments
     */
    public OldCsv commentPrefix(String prefix) {
        this.commentPrefix = Optional.of(prefix);
        return this;
    }

    /** Ignore the first line. Not skip the first line by default. */
    public OldCsv ignoreFirstLine() {
        this.isIgnoreFirstLine = Optional.of(true);
        return this;
    }

    /** Skip records with parse error instead to fail. Throw an exception by default. */
    public OldCsv ignoreParseErrors() {
        this.lenient = Optional.of(true);
        return this;
    }

    /**
     * Set a writeMode. null by default.
     *
     * @param writeMode The write mode decides what happens if a file should be created, but already
     *     exists.
     */
    public OldCsv writeMode(String writeMode) {
        this.writeMode = Optional.of(writeMode);
        return this;
    }

    /**
     * Set the numFiles. -1 by default.
     *
     * @param numFiles The number of files to write to.
     */
    public OldCsv numFiles(int numFiles) {
        this.numFiles = Optional.of(numFiles);
        return this;
    }

    /**
     * Derives the format schema from the table's schema. Required if no format schema is defined.
     *
     * <p>This allows for defining schema information only once.
     *
     * <p>The names, types, and fields' order of the format are determined by the table's schema.
     *
     * @deprecated Derivation format schema from table's schema is the default behavior now. So
     *     there is no need to explicitly declare to derive schema.
     */
    @Deprecated
    public OldCsv deriveSchema() {
        this.deriveSchema = Optional.of(true);
        return this;
    }

    @Override
    protected Map<String, String> toFormatProperties() {
        DescriptorProperties properties = new DescriptorProperties();

        fieldDelim.ifPresent(s -> properties.putString(FORMAT_FIELD_DELIMITER, s));
        lineDelim.ifPresent(s -> properties.putString(FORMAT_LINE_DELIMITER, s));

        if (deriveSchema.isPresent() && deriveSchema.get()) {
            properties.putBoolean(FORMAT_DERIVE_SCHEMA, true);
        } else {
            List<String> subKeys =
                    Arrays.asList(DescriptorProperties.NAME, DescriptorProperties.DATA_TYPE);

            List<List<String>> subValues =
                    schema.entrySet().stream()
                            .map(e -> Arrays.asList(e.getKey(), e.getValue()))
                            .collect(Collectors.toList());

            properties.putIndexedFixedProperties(FORMAT_FIELDS, subKeys, subValues);
        }

        quoteCharacter.ifPresent(
                character -> properties.putCharacter(FORMAT_QUOTE_CHARACTER, character));
        commentPrefix.ifPresent(s -> properties.putString(FORMAT_COMMENT_PREFIX, s));
        isIgnoreFirstLine.ifPresent(
                aBoolean -> properties.putBoolean(FORMAT_IGNORE_FIRST_LINE, aBoolean));
        lenient.ifPresent(aBoolean -> properties.putBoolean(FORMAT_IGNORE_PARSE_ERRORS, aBoolean));
        writeMode.ifPresent(s -> properties.putString(FORMAT_WRITE_MODE, s));
        numFiles.ifPresent(i -> properties.putInt(FORMAT_NUM_FILES, i));

        return properties.asMap();
    }
}
