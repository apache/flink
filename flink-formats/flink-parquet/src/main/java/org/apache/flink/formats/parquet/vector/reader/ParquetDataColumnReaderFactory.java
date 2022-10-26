/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.formats.parquet.vector.reader;

import org.apache.flink.table.data.TimestampData;

import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.PrimitiveType;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Parquet file has self-describing schema which may differ from the user required schema (e.g.
 * schema evolution). This factory is used to retrieve user required typed data via corresponding
 * reader which reads the underlying data. Part of the code is referred from Apache Hive.
 */
public final class ParquetDataColumnReaderFactory {

    private ParquetDataColumnReaderFactory() {}

    /**
     * The default data column reader for existing Parquet page reader which works for both
     * dictionary or non dictionary types, Mirror from dictionary encoding path.
     */
    public static class DefaultParquetDataColumnReader implements ParquetDataColumnReader {
        protected final ValuesReader valuesReader;
        protected final Dictionary dict;

        public DefaultParquetDataColumnReader(ValuesReader valuesReader) {
            this.valuesReader = checkNotNull(valuesReader);
            this.dict = null;
        }

        public DefaultParquetDataColumnReader(Dictionary dict) {
            this.valuesReader = null;
            this.dict = checkNotNull(dict);
        }

        @Override
        public void initFromPage(int i, ByteBufferInputStream in) throws IOException {
            valuesReader.initFromPage(i, in);
        }

        @Override
        public boolean readBoolean() {
            return valuesReader.readBoolean();
        }

        @Override
        public boolean readBoolean(int id) {
            return dict.decodeToBoolean(id);
        }

        @Override
        public byte[] readBytes() {
            return valuesReader.readBytes().getBytesUnsafe();
        }

        @Override
        public byte[] readBytes(int id) {
            return dict.decodeToBinary(id).getBytesUnsafe();
        }

        @Override
        public float readFloat() {
            return valuesReader.readFloat();
        }

        @Override
        public float readFloat(int id) {
            return dict.decodeToFloat(id);
        }

        @Override
        public double readDouble() {
            return valuesReader.readDouble();
        }

        @Override
        public double readDouble(int id) {
            return dict.decodeToDouble(id);
        }

        @Override
        public TimestampData readTimestamp() {
            throw new RuntimeException("Unsupported operation");
        }

        @Override
        public TimestampData readTimestamp(int id) {
            throw new RuntimeException("Unsupported operation");
        }

        @Override
        public int readInteger() {
            return valuesReader.readInteger();
        }

        @Override
        public int readInteger(int id) {
            return dict.decodeToInt(id);
        }

        @Override
        public long readLong(int id) {
            return dict.decodeToLong(id);
        }

        @Override
        public long readLong() {
            return valuesReader.readLong();
        }

        @Override
        public int readSmallInt() {
            return valuesReader.readInteger();
        }

        @Override
        public int readSmallInt(int id) {
            return dict.decodeToInt(id);
        }

        @Override
        public int readTinyInt() {
            return valuesReader.readInteger();
        }

        @Override
        public int readTinyInt(int id) {
            return dict.decodeToInt(id);
        }

        @Override
        public int readValueDictionaryId() {
            return valuesReader.readValueDictionaryId();
        }

        public void skip() {
            valuesReader.skip();
        }

        @Override
        public Dictionary getDictionary() {
            return dict;
        }
    }

    /** The reader who reads from the underlying Timestamp value. */
    public static class TypesFromInt96PageReader extends DefaultParquetDataColumnReader {
        private final boolean isUtcTimestamp;

        public TypesFromInt96PageReader(ValuesReader realReader, boolean isUtcTimestamp) {
            super(realReader);
            this.isUtcTimestamp = isUtcTimestamp;
        }

        public TypesFromInt96PageReader(Dictionary dict, boolean isUtcTimestamp) {
            super(dict);
            this.isUtcTimestamp = isUtcTimestamp;
        }

        private TimestampData convert(Binary binary) {
            ByteBuffer buf = binary.toByteBuffer();
            buf.order(ByteOrder.LITTLE_ENDIAN);
            long timeOfDayNanos = buf.getLong();
            int julianDay = buf.getInt();
            return TimestampColumnReader.int96ToTimestamp(
                    isUtcTimestamp, timeOfDayNanos, julianDay);
        }

        @Override
        public TimestampData readTimestamp(int id) {
            return convert(dict.decodeToBinary(id));
        }

        @Override
        public TimestampData readTimestamp() {
            return convert(valuesReader.readBytes());
        }
    }

    private static ParquetDataColumnReader getDataColumnReaderByTypeHelper(
            boolean isDictionary,
            PrimitiveType parquetType,
            @Nullable Dictionary dictionary,
            @Nullable ValuesReader valuesReader,
            boolean isUtcTimestamp) {
        if (parquetType.getPrimitiveTypeName() == PrimitiveType.PrimitiveTypeName.INT96) {
            return isDictionary
                    ? new TypesFromInt96PageReader(dictionary, isUtcTimestamp)
                    : new TypesFromInt96PageReader(valuesReader, isUtcTimestamp);
        } else {
            return isDictionary
                    ? new DefaultParquetDataColumnReader(dictionary)
                    : new DefaultParquetDataColumnReader(valuesReader);
        }
    }

    public static ParquetDataColumnReader getDataColumnReaderByTypeOnDictionary(
            PrimitiveType parquetType, Dictionary realReader, boolean isUtcTimestamp) {
        return getDataColumnReaderByTypeHelper(true, parquetType, realReader, null, isUtcTimestamp);
    }

    public static ParquetDataColumnReader getDataColumnReaderByType(
            PrimitiveType parquetType, ValuesReader realReader, boolean isUtcTimestamp) {
        return getDataColumnReaderByTypeHelper(
                false, parquetType, null, realReader, isUtcTimestamp);
    }
}
