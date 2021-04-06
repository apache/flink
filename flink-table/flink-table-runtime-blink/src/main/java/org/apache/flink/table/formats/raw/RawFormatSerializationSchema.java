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
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RawType;

import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

/** Serialization schema that serializes an {@link RowData} object into raw (byte based) value. */
@Internal
public class RawFormatSerializationSchema implements SerializationSchema<RowData> {

    private static final long serialVersionUID = 1L;

    private final LogicalType serializedType;

    private final SerializationRuntimeConverter converter;

    private final String charsetName;

    private final boolean isBigEndian;

    public RawFormatSerializationSchema(
            LogicalType serializedType, String charsetName, boolean isBigEndian) {
        this.serializedType = serializedType;
        this.converter = createConverter(serializedType, charsetName, isBigEndian);
        this.charsetName = charsetName;
        this.isBigEndian = isBigEndian;
    }

    @Override
    public void open(InitializationContext context) throws Exception {
        converter.open();
    }

    @Override
    public byte[] serialize(RowData row) {
        try {
            return converter.convert(row);
        } catch (IOException e) {
            throw new RuntimeException("Could not serialize row '" + row + "'. ", e);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RawFormatSerializationSchema that = (RawFormatSerializationSchema) o;
        return serializedType.equals(that.serializedType)
                && charsetName.equals(that.charsetName)
                && isBigEndian == that.isBigEndian;
    }

    @Override
    public int hashCode() {
        return Objects.hash(serializedType, charsetName, isBigEndian);
    }

    // ------------------------------------------------------------------------

    /** Runtime converter that convert an object of internal data structure to byte[]. */
    @FunctionalInterface
    private interface SerializationRuntimeConverter extends Serializable {

        default void open() {}

        byte[] convert(RowData row) throws IOException;
    }

    /** Creates a runtime converter. */
    private SerializationRuntimeConverter createConverter(
            LogicalType type, String charsetName, boolean isBigEndian) {
        final SerializationRuntimeConverter converter =
                createNotNullConverter(type, charsetName, isBigEndian);

        return new SerializationRuntimeConverter() {
            private static final long serialVersionUID = 1L;

            @Override
            public void open() {
                converter.open();
            }

            @Override
            public byte[] convert(RowData row) throws IOException {
                if (row.isNullAt(0)) {
                    return null;
                }
                return converter.convert(row);
            }
        };
    }

    /** Creates a runtime converter. */
    private SerializationRuntimeConverter createNotNullConverter(
            LogicalType type, String charsetName, boolean isBigEndian) {
        switch (type.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                return createStringConverter(charsetName);

            case VARBINARY:
            case BINARY:
                return row -> row.getBinary(0);

            case RAW:
                return createRawValueConverter((RawType<?>) type);

            case BOOLEAN:
                return row -> {
                    byte b = (byte) (row.getBoolean(0) ? 1 : 0);
                    return new byte[] {b};
                };

            case TINYINT:
                return row -> new byte[] {row.getByte(0)};

            case SMALLINT:
                return new ShortSerializationConverter(isBigEndian);

            case INTEGER:
                return new IntegerSerializationConverter(isBigEndian);

            case BIGINT:
                return new LongSerializationConverter(isBigEndian);

            case FLOAT:
                return new FloatSerializationConverter(isBigEndian);

            case DOUBLE:
                return new DoubleSerializationConverter(isBigEndian);

            default:
                throw new UnsupportedOperationException(
                        "'single-format' currently doesn't support type: " + type);
        }
    }

    private static SerializationRuntimeConverter createStringConverter(final String charsetName) {
        // this also checks the charsetName is valid
        Charset charset = Charset.forName(charsetName);
        if (charset == StandardCharsets.UTF_8) {
            // avoid UTF-8 encoding if the given charset is UTF-8
            // because the underlying bytes of StringData is in UTF-8 encoding
            return row -> row.getString(0).toBytes();
        }

        return new SerializationRuntimeConverter() {
            private static final long serialVersionUID = 1L;
            private transient Charset charset;

            @Override
            public void open() {
                charset = Charset.forName(charsetName);
            }

            @Override
            public byte[] convert(RowData row) {
                String str = row.getString(0).toString();
                return str.getBytes(charset);
            }
        };
    }

    @SuppressWarnings("unchecked")
    private static SerializationRuntimeConverter createRawValueConverter(RawType<?> rawType) {
        final TypeSerializer<Object> serializer =
                (TypeSerializer<Object>) rawType.getTypeSerializer();
        return row -> row.getRawValue(0).toBytes(serializer);
    }

    // ------------------------------------------------------------------------------------
    // Utilities to serialize endianness numeric
    // ------------------------------------------------------------------------------------

    private static final class ShortSerializationConverter
            implements SerializationRuntimeConverter {

        private static final long serialVersionUID = 1L;
        private final boolean isBigEndian;

        private ShortSerializationConverter(boolean isBigEndian) {
            this.isBigEndian = isBigEndian;
        }

        @Override
        public byte[] convert(RowData row) {
            MemorySegment segment = MemorySegmentFactory.wrap(new byte[2]);
            if (isBigEndian) {
                segment.putShortBigEndian(0, row.getShort(0));
            } else {
                segment.putShortLittleEndian(0, row.getShort(0));
            }
            return segment.getArray();
        }
    }

    private static final class IntegerSerializationConverter
            implements SerializationRuntimeConverter {

        private static final long serialVersionUID = 1L;
        private final boolean isBigEndian;

        private IntegerSerializationConverter(boolean isBigEndian) {
            this.isBigEndian = isBigEndian;
        }

        @Override
        public byte[] convert(RowData row) {
            MemorySegment segment = MemorySegmentFactory.wrap(new byte[4]);
            if (isBigEndian) {
                segment.putIntBigEndian(0, row.getInt(0));
            } else {
                segment.putIntLittleEndian(0, row.getInt(0));
            }
            return segment.getArray();
        }
    }

    private static final class LongSerializationConverter implements SerializationRuntimeConverter {

        private static final long serialVersionUID = 1L;
        private final boolean isBigEndian;

        private LongSerializationConverter(boolean isBigEndian) {
            this.isBigEndian = isBigEndian;
        }

        @Override
        public byte[] convert(RowData row) {
            MemorySegment segment = MemorySegmentFactory.wrap(new byte[8]);
            if (isBigEndian) {
                segment.putLongBigEndian(0, row.getLong(0));
            } else {
                segment.putLongLittleEndian(0, row.getLong(0));
            }
            return segment.getArray();
        }
    }

    private static final class FloatSerializationConverter
            implements SerializationRuntimeConverter {

        private static final long serialVersionUID = 1L;
        private final boolean isBigEndian;

        private FloatSerializationConverter(boolean isBigEndian) {
            this.isBigEndian = isBigEndian;
        }

        @Override
        public byte[] convert(RowData row) {
            MemorySegment segment = MemorySegmentFactory.wrap(new byte[4]);
            if (isBigEndian) {
                segment.putFloatBigEndian(0, row.getFloat(0));
            } else {
                segment.putFloatLittleEndian(0, row.getFloat(0));
            }
            return segment.getArray();
        }
    }

    private static final class DoubleSerializationConverter
            implements SerializationRuntimeConverter {

        private static final long serialVersionUID = 1L;
        private final boolean isBigEndian;

        private DoubleSerializationConverter(boolean isBigEndian) {
            this.isBigEndian = isBigEndian;
        }

        @Override
        public byte[] convert(RowData row) {
            MemorySegment segment = MemorySegmentFactory.wrap(new byte[8]);
            if (isBigEndian) {
                segment.putDoubleBigEndian(0, row.getDouble(0));
            } else {
                segment.putDoubleLittleEndian(0, row.getDouble(0));
            }
            return segment.getArray();
        }
    }
}
