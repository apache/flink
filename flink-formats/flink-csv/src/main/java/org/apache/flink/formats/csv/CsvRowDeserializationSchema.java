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

package org.apache.flink.formats.csv;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectReader;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvSchema;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Objects;

import static org.apache.flink.formats.common.TimeFormats.SQL_TIMESTAMP_FORMAT;
import static org.apache.flink.formats.common.TimeFormats.SQL_TIMESTAMP_WITH_LOCAL_TIMEZONE_FORMAT;

/**
 * Deserialization schema from CSV to Flink types.
 *
 * <p>Deserializes a <code>byte[]</code> message as a {@link JsonNode} and converts it to {@link
 * Row}.
 *
 * <p>Failure during deserialization are forwarded as wrapped {@link IOException}s.
 */
@PublicEvolving
public final class CsvRowDeserializationSchema implements DeserializationSchema<Row> {

    private static final long serialVersionUID = 2135553495874539201L;

    /** Type information describing the result type. */
    private final TypeInformation<Row> typeInfo;

    /** Runtime instance that performs the actual work. */
    private final RuntimeConverter runtimeConverter;

    /** Schema describing the input CSV data. */
    private final CsvSchema csvSchema;

    /** Object reader used to read rows. It is configured by {@link CsvSchema}. */
    private final ObjectReader objectReader;

    /** Flag indicating whether to ignore invalid fields/rows (default: throw an exception). */
    private final boolean ignoreParseErrors;

    private CsvRowDeserializationSchema(
            RowTypeInfo typeInfo, CsvSchema csvSchema, boolean ignoreParseErrors) {
        this.typeInfo = typeInfo;
        this.runtimeConverter = createRowRuntimeConverter(typeInfo, ignoreParseErrors, true);
        this.csvSchema = csvSchema;
        this.objectReader = new CsvMapper().readerFor(JsonNode.class).with(csvSchema);
        this.ignoreParseErrors = ignoreParseErrors;
    }

    /** A builder for creating a {@link CsvRowDeserializationSchema}. */
    @PublicEvolving
    public static class Builder {

        private final RowTypeInfo typeInfo;
        private CsvSchema csvSchema;
        private boolean ignoreParseErrors;

        /**
         * Creates a CSV deserialization schema for the given {@link TypeInformation} with optional
         * parameters.
         */
        public Builder(TypeInformation<Row> typeInfo) {
            Preconditions.checkNotNull(typeInfo, "Type information must not be null.");

            if (!(typeInfo instanceof RowTypeInfo)) {
                throw new IllegalArgumentException("Row type information expected.");
            }

            this.typeInfo = (RowTypeInfo) typeInfo;
            this.csvSchema = CsvRowSchemaConverter.convert((RowTypeInfo) typeInfo);
        }

        public Builder setFieldDelimiter(char delimiter) {
            this.csvSchema = this.csvSchema.rebuild().setColumnSeparator(delimiter).build();
            return this;
        }

        public Builder setAllowComments(boolean allowComments) {
            this.csvSchema = this.csvSchema.rebuild().setAllowComments(allowComments).build();
            return this;
        }

        public Builder setArrayElementDelimiter(String delimiter) {
            Preconditions.checkNotNull(delimiter, "Array element delimiter must not be null.");
            this.csvSchema = this.csvSchema.rebuild().setArrayElementSeparator(delimiter).build();
            return this;
        }

        public Builder setQuoteCharacter(char c) {
            this.csvSchema = this.csvSchema.rebuild().setQuoteChar(c).build();
            return this;
        }

        public Builder setEscapeCharacter(char c) {
            this.csvSchema = this.csvSchema.rebuild().setEscapeChar(c).build();
            return this;
        }

        public Builder setNullLiteral(String nullLiteral) {
            Preconditions.checkNotNull(nullLiteral, "Null literal must not be null.");
            this.csvSchema = this.csvSchema.rebuild().setNullValue(nullLiteral).build();
            return this;
        }

        public Builder setIgnoreParseErrors(boolean ignoreParseErrors) {
            this.ignoreParseErrors = ignoreParseErrors;
            return this;
        }

        public CsvRowDeserializationSchema build() {
            return new CsvRowDeserializationSchema(typeInfo, csvSchema, ignoreParseErrors);
        }
    }

    @Override
    public Row deserialize(byte[] message) throws IOException {
        try {
            final JsonNode root = objectReader.readValue(message);
            return (Row) runtimeConverter.convert(root);
        } catch (Throwable t) {
            if (ignoreParseErrors) {
                return null;
            }
            throw new IOException(
                    "Failed to deserialize CSV row '" + new String(message) + "'.", t);
        }
    }

    @Override
    public boolean isEndOfStream(Row nextElement) {
        return false;
    }

    @Override
    public TypeInformation<Row> getProducedType() {
        return typeInfo;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || o.getClass() != this.getClass()) {
            return false;
        }
        final CsvRowDeserializationSchema that = (CsvRowDeserializationSchema) o;
        final CsvSchema otherSchema = that.csvSchema;

        return typeInfo.equals(that.typeInfo)
                && ignoreParseErrors == that.ignoreParseErrors
                && csvSchema.getColumnSeparator() == otherSchema.getColumnSeparator()
                && csvSchema.allowsComments() == otherSchema.allowsComments()
                && csvSchema
                        .getArrayElementSeparator()
                        .equals(otherSchema.getArrayElementSeparator())
                && csvSchema.getQuoteChar() == otherSchema.getQuoteChar()
                && csvSchema.getEscapeChar() == otherSchema.getEscapeChar()
                && Arrays.equals(csvSchema.getNullValue(), otherSchema.getNullValue());
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                typeInfo,
                ignoreParseErrors,
                csvSchema.getColumnSeparator(),
                csvSchema.allowsComments(),
                csvSchema.getArrayElementSeparator(),
                csvSchema.getQuoteChar(),
                csvSchema.getEscapeChar(),
                csvSchema.getNullValue());
    }

    // --------------------------------------------------------------------------------------------

    interface RuntimeConverter extends Serializable {
        Object convert(JsonNode node);
    }

    private static RuntimeConverter createRowRuntimeConverter(
            RowTypeInfo rowTypeInfo, boolean ignoreParseErrors, boolean isTopLevel) {
        final TypeInformation<?>[] fieldTypes = rowTypeInfo.getFieldTypes();
        final String[] fieldNames = rowTypeInfo.getFieldNames();

        final RuntimeConverter[] fieldConverters =
                createFieldRuntimeConverters(ignoreParseErrors, fieldTypes);

        return assembleRowRuntimeConverter(
                ignoreParseErrors, isTopLevel, fieldNames, fieldConverters);
    }

    static RuntimeConverter[] createFieldRuntimeConverters(
            boolean ignoreParseErrors, TypeInformation<?>[] fieldTypes) {
        final RuntimeConverter[] fieldConverters = new RuntimeConverter[fieldTypes.length];
        for (int i = 0; i < fieldTypes.length; i++) {
            fieldConverters[i] = createNullableRuntimeConverter(fieldTypes[i], ignoreParseErrors);
        }
        return fieldConverters;
    }

    private static RuntimeConverter assembleRowRuntimeConverter(
            boolean ignoreParseErrors,
            boolean isTopLevel,
            String[] fieldNames,
            RuntimeConverter[] fieldConverters) {
        final int rowArity = fieldNames.length;

        return (node) -> {
            final int nodeSize = node.size();

            if (nodeSize != 0) {
                validateArity(rowArity, nodeSize, ignoreParseErrors);
            } else {
                return null;
            }

            final Row row = new Row(rowArity);
            for (int i = 0; i < Math.min(rowArity, nodeSize); i++) {
                // Jackson only supports mapping by name in the first level
                if (isTopLevel) {
                    row.setField(i, fieldConverters[i].convert(node.get(fieldNames[i])));
                } else {
                    row.setField(i, fieldConverters[i].convert(node.get(i)));
                }
            }
            return row;
        };
    }

    private static RuntimeConverter createNullableRuntimeConverter(
            TypeInformation<?> info, boolean ignoreParseErrors) {
        final RuntimeConverter valueConverter = createRuntimeConverter(info, ignoreParseErrors);
        return (node) -> {
            if (node.isNull()) {
                return null;
            }
            try {
                return valueConverter.convert(node);
            } catch (Throwable t) {
                if (!ignoreParseErrors) {
                    throw t;
                }
                return null;
            }
        };
    }

    private static RuntimeConverter createRuntimeConverter(
            TypeInformation<?> info, boolean ignoreParseErrors) {
        if (info.equals(Types.VOID)) {
            return (node) -> null;
        } else if (info.equals(Types.STRING)) {
            return JsonNode::asText;
        } else if (info.equals(Types.BOOLEAN)) {
            return (node) -> Boolean.valueOf(node.asText().trim());
        } else if (info.equals(Types.BYTE)) {
            return (node) -> Byte.valueOf(node.asText().trim());
        } else if (info.equals(Types.SHORT)) {
            return (node) -> Short.valueOf(node.asText().trim());
        } else if (info.equals(Types.INT)) {
            return (node) -> Integer.valueOf(node.asText().trim());
        } else if (info.equals(Types.LONG)) {
            return (node) -> Long.valueOf(node.asText().trim());
        } else if (info.equals(Types.FLOAT)) {
            return (node) -> Float.valueOf(node.asText().trim());
        } else if (info.equals(Types.DOUBLE)) {
            return (node) -> Double.valueOf(node.asText().trim());
        } else if (info.equals(Types.BIG_DEC)) {
            return (node) -> new BigDecimal(node.asText().trim());
        } else if (info.equals(Types.BIG_INT)) {
            return (node) -> new BigInteger(node.asText().trim());
        } else if (info.equals(Types.SQL_DATE)) {
            return (node) -> Date.valueOf(node.asText());
        } else if (info.equals(Types.SQL_TIME)) {
            return (node) -> Time.valueOf(node.asText());
        } else if (info.equals(Types.SQL_TIMESTAMP)) {
            return (node) -> Timestamp.valueOf(node.asText());
        } else if (info.equals(Types.LOCAL_DATE)) {
            return (node) -> Date.valueOf(node.asText()).toLocalDate();
        } else if (info.equals(Types.LOCAL_TIME)) {
            return (node) -> Time.valueOf(node.asText()).toLocalTime();
        } else if (info.equals(Types.LOCAL_DATE_TIME)) {
            return (node) -> LocalDateTime.parse(node.asText().trim(), SQL_TIMESTAMP_FORMAT);
        } else if (info.equals(Types.INSTANT)) {
            return (node) ->
                    LocalDateTime.parse(node.asText(), SQL_TIMESTAMP_WITH_LOCAL_TIMEZONE_FORMAT)
                            .toInstant(ZoneOffset.UTC);
        } else if (info instanceof RowTypeInfo) {
            final RowTypeInfo rowTypeInfo = (RowTypeInfo) info;
            return createRowRuntimeConverter(rowTypeInfo, ignoreParseErrors, false);
        } else if (info instanceof BasicArrayTypeInfo) {
            return createObjectArrayRuntimeConverter(
                    ((BasicArrayTypeInfo<?, ?>) info).getComponentInfo(), ignoreParseErrors);
        } else if (info instanceof ObjectArrayTypeInfo) {
            return createObjectArrayRuntimeConverter(
                    ((ObjectArrayTypeInfo<?, ?>) info).getComponentInfo(), ignoreParseErrors);
        } else if (info instanceof PrimitiveArrayTypeInfo
                && ((PrimitiveArrayTypeInfo) info).getComponentType() == Types.BYTE) {
            return createByteArrayRuntimeConverter(ignoreParseErrors);
        } else {
            throw new RuntimeException("Unsupported type information '" + info + "'.");
        }
    }

    private static RuntimeConverter createObjectArrayRuntimeConverter(
            TypeInformation<?> elementType, boolean ignoreParseErrors) {
        final Class<?> elementClass = elementType.getTypeClass();
        final RuntimeConverter elementConverter =
                createNullableRuntimeConverter(elementType, ignoreParseErrors);

        return (node) -> {
            final int nodeSize = node.size();
            final Object[] array = (Object[]) Array.newInstance(elementClass, nodeSize);
            for (int i = 0; i < nodeSize; i++) {
                array[i] = elementConverter.convert(node.get(i));
            }
            return array;
        };
    }

    private static RuntimeConverter createByteArrayRuntimeConverter(boolean ignoreParseErrors) {
        return (node) -> {
            try {
                return node.binaryValue();
            } catch (IOException e) {
                if (!ignoreParseErrors) {
                    throw new RuntimeException("Unable to deserialize byte array.", e);
                }
                return null;
            }
        };
    }

    static void validateArity(int expected, int actual, boolean ignoreParseErrors) {
        if (expected != actual && !ignoreParseErrors) {
            throw new RuntimeException(
                    "Row length mismatch. "
                            + expected
                            + " fields expected but was "
                            + actual
                            + ".");
        }
    }
}
