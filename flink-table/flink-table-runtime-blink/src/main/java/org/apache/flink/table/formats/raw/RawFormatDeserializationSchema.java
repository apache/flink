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

package org.apache.flink.table.formats.raw;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RawValueData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.types.DeserializationException;

import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

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

    private final DeserializationRuntimeConverter converter;

    private final DataLengthValidator validator;

    private transient GenericRowData reuse;

    public RawFormatDeserializationSchema(
            LogicalType deserializedType,
            TypeInformation<RowData> producedTypeInfo,
            String charsetName,
            boolean isBigEndian) {
        this.deserializedType = checkNotNull(deserializedType);
        this.producedTypeInfo = checkNotNull(producedTypeInfo);
        this.converter = createConverter(deserializedType, charsetName, isBigEndian);
        this.validator = createDataLengthValidator(deserializedType);
        this.charsetName = charsetName;
        this.isBigEndian = isBigEndian;
    }

    @Override
    public void open(InitializationContext context) throws Exception {
        reuse = new GenericRowData(1);
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
        reuse.setField(0, field);
        return reuse;
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
                && isBigEndian == that.isBigEndian;
    }

    @Override
    public int hashCode() {
        return Objects.hash(producedTypeInfo, deserializedType, charsetName, isBigEndian);
    }

    // ------------------------------------------------------------------------

    /** Runtime converter that convert byte[] to internal data structure object. */
    @FunctionalInterface
    private interface DeserializationRuntimeConverter extends Serializable {

        default void open() {}

        Object convert(byte[] data) throws IOException;
    }

    /** Creates a runtime converter. */
    private static DeserializationRuntimeConverter createConverter(
            LogicalType type, String charsetName, boolean isBigEndian) {

        switch (type.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                return createStringConverter(charsetName);

            case VARBINARY:
            case BINARY:
                return data -> data;

            case RAW:
                return RawValueData::fromBytes;

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
