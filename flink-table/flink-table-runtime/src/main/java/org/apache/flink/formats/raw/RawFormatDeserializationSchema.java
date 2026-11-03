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

package org.apache.flink.formats.raw;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.formats.raw.RawFormatOptions.VariantEncoding;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RawValueData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.types.DeserializationException;
import org.apache.flink.types.variant.BinaryVariant;
import org.apache.flink.types.variant.BinaryVariantInternalBuilder;
import org.apache.flink.types.variant.BinaryVariantUtil;
import org.apache.flink.types.variant.Variant;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.regex.Pattern;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Deserialization schema from raw (byte based) value to Flink Table/SQL internal data structure
 * {@link RowData}.
 */
@Internal
public class RawFormatDeserializationSchema implements DeserializationSchema<RowData> {

    private static final long serialVersionUID = 1L;

    private final LogicalType deserializedType;

    private final TypeInformation<RowData> producedTypeInfo;

    private final String charsetName;

    private final boolean isBigEndian;

    @Nullable private final String lineDelimiter;

    private final VariantEncoding variantEncoding;

    private final DeserializationRuntimeConverter converter;

    private final DataLengthValidator validator;

    public RawFormatDeserializationSchema(
            LogicalType deserializedType,
            TypeInformation<RowData> producedTypeInfo,
            String charsetName,
            boolean isBigEndian,
            @Nullable String lineDelimiter,
            VariantEncoding variantEncoding) {
        this.deserializedType = checkNotNull(deserializedType);
        this.producedTypeInfo = checkNotNull(producedTypeInfo);
        this.variantEncoding = checkNotNull(variantEncoding);
        this.converter =
                createConverter(
                        deserializedType, charsetName, isBigEndian, lineDelimiter, variantEncoding);
        this.validator = createDataLengthValidator(deserializedType);
        this.charsetName = charsetName;
        this.isBigEndian = isBigEndian;
        this.lineDelimiter = lineDelimiter;
    }

    @Override
    public void open(InitializationContext context) throws Exception {
        converter.open();
    }

    @Override
    public RowData deserialize(byte[] message) throws IOException {
        final Object field;
        if (message == null) {
            field = null;
        } else {
            validator.validate(message);
            field = converter.convert(message);
        }

        GenericRowData rowData = new GenericRowData(1);
        rowData.setField(0, field);
        return rowData;
    }

    @Override
    public void deserialize(byte[] message, Collector<RowData> out) throws IOException {
        if (lineDelimiter == null) {
            // no delimiter: default single-record behavior
            RowData row = deserialize(message);
            if (row != null) {
                out.collect(row);
            }
            return;
        }

        if (message == null) {
            return;
        }

        converter.convertLines(
                message,
                field -> {
                    final GenericRowData rowData = new GenericRowData(1);
                    rowData.setField(0, field);
                    out.collect(rowData);
                });
    }

    @Override
    public boolean isEndOfStream(RowData nextElement) {
        return false;
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return producedTypeInfo;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RawFormatDeserializationSchema that = (RawFormatDeserializationSchema) o;
        return producedTypeInfo.equals(that.producedTypeInfo)
                && deserializedType.equals(that.deserializedType)
                && charsetName.equals(that.charsetName)
                && isBigEndian == that.isBigEndian
                && Objects.equals(lineDelimiter, that.lineDelimiter)
                && variantEncoding == that.variantEncoding;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                producedTypeInfo,
                deserializedType,
                charsetName,
                isBigEndian,
                lineDelimiter,
                variantEncoding);
    }

    // ------------------------------------------------------------------------

    /** Runtime converter that convert byte[] to internal data structure object. */
    @FunctionalInterface
    private interface DeserializationRuntimeConverter extends Serializable {

        default void open() {}

        Object convert(byte[] data) throws IOException;

        /**
         * Converts a message that holds one value per line. Only a string column takes a line
         * delimiter, so only the string converter splits the message.
         */
        default void convertLines(byte[] message, Consumer<Object> out) throws IOException {
            out.accept(convert(message));
        }
    }

    /** Creates a runtime converter. */
    private static DeserializationRuntimeConverter createConverter(
            LogicalType type,
            String charsetName,
            boolean isBigEndian,
            @Nullable String lineDelimiter,
            VariantEncoding variantEncoding) {

        switch (type.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                return lineDelimiter == null
                        ? createStringConverter(charsetName)
                        : createDelimitedStringConverter(charsetName, lineDelimiter);

            case VARBINARY:
            case BINARY:
                return data -> data;

            case RAW:
                return RawValueData::fromBytes;

            case VARIANT:
                return variantEncoding == VariantEncoding.BINARY
                        ? createBinaryVariantConverter()
                        : createJsonVariantConverter();

            case BOOLEAN:
                return data -> data[0] != 0;

            case TINYINT:
                return data -> data[0];

            case SMALLINT:
                return createEndiannessAwareConverter(
                        isBigEndian,
                        segment -> segment.getShortBigEndian(0),
                        segment -> segment.getShortLittleEndian(0));

            case INTEGER:
                return createEndiannessAwareConverter(
                        isBigEndian,
                        segment -> segment.getIntBigEndian(0),
                        segment -> segment.getIntLittleEndian(0));

            case BIGINT:
                return createEndiannessAwareConverter(
                        isBigEndian,
                        segment -> segment.getLongBigEndian(0),
                        segment -> segment.getLongLittleEndian(0));

            case FLOAT:
                return createEndiannessAwareConverter(
                        isBigEndian,
                        segment -> segment.getFloatBigEndian(0),
                        segment -> segment.getFloatLittleEndian(0));

            case DOUBLE:
                return createEndiannessAwareConverter(
                        isBigEndian,
                        segment -> segment.getDoubleBigEndian(0),
                        segment -> segment.getDoubleLittleEndian(0));

            default:
                throw new UnsupportedOperationException(
                        "'raw' format currently doesn't support type: " + type);
        }
    }

    private static DeserializationRuntimeConverter createStringConverter(final String charsetName) {
        // this also checks the charsetName is valid
        Charset charset = Charset.forName(charsetName);
        if (charset == StandardCharsets.UTF_8) {
            // avoid UTF-8 decoding if the given charset is UTF-8
            // because the underlying bytes of StringData is in UTF-8 encoding
            return StringData::fromBytes;
        }

        return new DeserializationRuntimeConverter() {
            private static final long serialVersionUID = 1L;
            private transient Charset charset;

            @Override
            public void open() {
                charset = Charset.forName(charsetName);
            }

            @Override
            public Object convert(byte[] data) {
                String str = new String(data, charset);
                return StringData.fromString(str);
            }
        };
    }

    /**
     * Creates a converter for a string column with a line delimiter, which turns a message into one
     * string per line. In UTF-8 the bytes of a delimiter can only match at a character boundary, so
     * the message is split without decoding it. In other charsets they can match inside a
     * character, and an empty delimiter splits between characters, so both decode the message
     * first.
     */
    private static DeserializationRuntimeConverter createDelimitedStringConverter(
            final String charsetName, final String lineDelimiter) {
        final DeserializationRuntimeConverter stringConverter = createStringConverter(charsetName);
        final boolean splitBytes =
                !lineDelimiter.isEmpty()
                        && StandardCharsets.UTF_8.equals(Charset.forName(charsetName));
        final byte[] delimiter = lineDelimiter.getBytes(StandardCharsets.UTF_8);
        final Pattern pattern = Pattern.compile(Pattern.quote(lineDelimiter));
        return new DeserializationRuntimeConverter() {
            private static final long serialVersionUID = 1L;
            private transient Charset charset;

            @Override
            public void open() {
                stringConverter.open();
                charset = Charset.forName(charsetName);
            }

            @Override
            public Object convert(byte[] data) throws IOException {
                return stringConverter.convert(data);
            }

            @Override
            public void convertLines(byte[] message, Consumer<Object> out) throws IOException {
                if (splitBytes) {
                    int start = 0;
                    int end;
                    while ((end = indexOf(message, start)) >= 0) {
                        out.accept(convert(Arrays.copyOfRange(message, start, end)));
                        start = end + delimiter.length;
                    }
                    // Like the decoded split below, drop the empty part after a trailing delimiter.
                    if (start < message.length) {
                        out.accept(convert(Arrays.copyOfRange(message, start, message.length)));
                    }
                    return;
                }
                // Split with -1 to keep intentional empty middle segments, but strip the single
                // trailing empty string produced when the message ends with the delimiter (e.g. a
                // serializer that appends one delimiter per row).
                final String[] parts = pattern.split(new String(message, charset), -1);
                int count = parts.length;
                if (count > 0 && parts[count - 1].isEmpty()) {
                    count--;
                }
                for (int i = 0; i < count; i++) {
                    out.accept(convert(parts[i].getBytes(charset)));
                }
            }

            /** Returns the position of the next delimiter at or after {@code from}, or -1. */
            private int indexOf(byte[] message, int from) {
                for (int i = from; i <= message.length - delimiter.length; i++) {
                    int j = 0;
                    while (j < delimiter.length && message[i + j] == delimiter[j]) {
                        j++;
                    }
                    if (j == delimiter.length) {
                        return i;
                    }
                }
                return -1;
            }
        };
    }

    /**
     * Creates a converter that parses UTF-8 JSON bytes into a {@link Variant} like {@code
     * PARSE_JSON}, so invalid UTF-8 and duplicate object keys are rejected.
     */
    private static DeserializationRuntimeConverter createJsonVariantConverter() {
        return new DeserializationRuntimeConverter() {
            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(byte[] data) {
                try {
                    return BinaryVariantInternalBuilder.parseJson(data, false);
                } catch (Exception e) {
                    throw new DeserializationException(
                            "Failed to deserialize VARIANT type. "
                                    + "The received data is not a valid JSON document.",
                            e);
                }
            }
        };
    }

    /**
     * Creates a converter that reads the Variant binary encoding, the metadata followed by the
     * value. The bytes are only checked as far as needed to split them. A malformed value fails
     * when it is read.
     */
    private static DeserializationRuntimeConverter createBinaryVariantConverter() {
        return new DeserializationRuntimeConverter() {
            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(byte[] data) {
                try {
                    final int metadataSize = BinaryVariantUtil.metadataSize(data);
                    return new BinaryVariant(
                            Arrays.copyOfRange(data, metadataSize, data.length),
                            Arrays.copyOf(data, metadataSize));
                } catch (Exception e) {
                    throw new DeserializationException(
                            "Failed to deserialize VARIANT type. "
                                    + "The received data is not a valid VARIANT binary encoding.",
                            e);
                }
            }
        };
    }

    private static DeserializationRuntimeConverter createEndiannessAwareConverter(
            final boolean isBigEndian,
            final MemorySegmentConverter bigEndianConverter,
            final MemorySegmentConverter littleEndianConverter) {
        if (isBigEndian) {
            return new EndiannessAwareDeserializationConverter(bigEndianConverter);
        } else {
            return new EndiannessAwareDeserializationConverter(littleEndianConverter);
        }
    }

    // ------------------------------------------------------------------------------------
    // Utilities to check received size of data
    // ------------------------------------------------------------------------------------

    /** Creates a validator for the received data. */
    private static DataLengthValidator createDataLengthValidator(LogicalType type) {
        // please keep the order the same with createNotNullConverter()
        switch (type.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
            case VARBINARY:
            case BINARY:
            case RAW:
            case VARIANT:
                return data -> {};
            case BOOLEAN:
                return createDataLengthValidator(1, "BOOLEAN");
            case TINYINT:
                return createDataLengthValidator(1, "TINYINT");
            case SMALLINT:
                return createDataLengthValidator(2, "SMALLINT");
            case INTEGER:
                return createDataLengthValidator(4, "INT");
            case BIGINT:
                return createDataLengthValidator(8, "BIGINT");
            case FLOAT:
                return createDataLengthValidator(4, "FLOAT");
            case DOUBLE:
                return createDataLengthValidator(8, "DOUBLE");
            default:
                throw new UnsupportedOperationException(
                        "'raw' format currently doesn't support type: " + type);
        }
    }

    private static DataLengthValidator createDataLengthValidator(
            int expectedLength, String typeName) {
        return data -> {
            if (data.length != expectedLength) {
                throw new DeserializationException(
                        String.format(
                                "Size of data received for deserializing %s type is not %s.",
                                typeName, expectedLength));
            }
        };
    }

    /** Validator to checks the length of received data. */
    private interface DataLengthValidator extends Serializable {
        void validate(byte[] data);
    }

    // ------------------------------------------------------------------------------------
    // Utilities to deserialize endianness numeric
    // ------------------------------------------------------------------------------------

    private static final class EndiannessAwareDeserializationConverter
            implements DeserializationRuntimeConverter {

        private static final long serialVersionUID = 1L;

        private final MemorySegmentConverter innerConverter;

        private EndiannessAwareDeserializationConverter(MemorySegmentConverter innerConverter) {
            this.innerConverter = innerConverter;
        }

        @Override
        public Object convert(byte[] data) {
            MemorySegment segment = MemorySegmentFactory.wrap(data);
            return innerConverter.convert(segment);
        }
    }

    /** Runtime converter that convert {@link MemorySegment} to internal data structure object. */
    private interface MemorySegmentConverter extends Serializable {
        Object convert(MemorySegment segment);
    }
}
