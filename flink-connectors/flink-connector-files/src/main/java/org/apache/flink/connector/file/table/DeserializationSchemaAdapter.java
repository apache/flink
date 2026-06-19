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

package org.apache.flink.connector.file.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.io.DelimitedInputFormat;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.reader.BulkFormat;
import org.apache.flink.connector.file.src.util.ArrayResultIterator;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.Path;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Collector;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.UserCodeClassLoader;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Queue;

import static org.apache.flink.connector.file.src.util.CheckpointedPosition.NO_OFFSET;
import static org.apache.flink.table.data.columnar.vector.VectorizedColumnBatch.DEFAULT_SIZE;

/** Adapter to turn a {@link DeserializationSchema} into a {@link BulkFormat}. */
@Internal
public class DeserializationSchemaAdapter implements BulkFormat<RowData, FileSourceSplit> {

    private static final int BATCH_SIZE = 100;

    private final DeserializationSchema<RowData> deserializationSchema;

    public DeserializationSchemaAdapter(DeserializationSchema<RowData> deserializationSchema) {
        this.deserializationSchema = deserializationSchema;
    }

    private DeserializationSchema<RowData> createDeserialization() throws IOException {
        try {
            DeserializationSchema<RowData> deserialization =
                    InstantiationUtil.clone(deserializationSchema);
            deserialization.open(
                    new DeserializationSchema.InitializationContext() {
                        @Override
                        public MetricGroup getMetricGroup() {
                            return new UnregisteredMetricsGroup();
                        }

                        @Override
                        public UserCodeClassLoader getUserCodeClassLoader() {
                            return (UserCodeClassLoader)
                                    Thread.currentThread().getContextClassLoader();
                        }
                    });
            return deserialization;
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    @Override
    public Reader createReader(Configuration config, FileSourceSplit split) throws IOException {
        return new Reader(config, split);
    }

    @Override
    public Reader restoreReader(Configuration config, FileSourceSplit split) throws IOException {
        Reader reader = new Reader(config, split);
        reader.seek(split.getReaderPosition().get().getRecordsAfterOffset());
        return reader;
    }

    @Override
    public boolean isSplittable() {
        return true;
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return deserializationSchema.getProducedType();
    }

    private class Reader implements BulkFormat.Reader<RowData> {

        private final LineBytesInputFormat inputFormat;
        private long numRead = 0;

        private Reader(Configuration config, FileSourceSplit split) throws IOException {
            this.inputFormat = new LineBytesInputFormat(split.path(), config);
            this.inputFormat.open(
                    new FileInputSplit(0, split.path(), split.offset(), split.length(), null));
        }

        @SuppressWarnings({"unchecked", "rawtypes"})
        @Nullable
        @Override
        public RecordIterator<RowData> readBatch() throws IOException {
            RowData[] records = new RowData[DEFAULT_SIZE];
            int num = 0;
            final long skipCount = numRead;
            for (int i = 0; i < BATCH_SIZE; i++) {
                RowData record = inputFormat.nextRecord(null);
                if (record == null) {
                    break;
                }
                records[num++] = record;
            }
            if (num == 0) {
                return null;
            }
            numRead += num;

            ArrayResultIterator<RowData> iterator = new ArrayResultIterator<>();
            iterator.set(records, num, NO_OFFSET, skipCount);
            return iterator;
        }

        private void seek(long toSkip) throws IOException {
            while (toSkip > 0) {
                inputFormat.nextRecord(null);
                toSkip--;
            }
        }

        @Override
        public void close() throws IOException {
            inputFormat.close();
        }
    }

    private class LineBytesInputFormat extends DelimitedInputFormat<RowData> {

        private static final long serialVersionUID = 1L;

        /** Code of \r, used to remove \r from a line when the line ends with \r\n. */
        private static final byte CARRIAGE_RETURN = (byte) '\r';

        /** Code of \n, used to identify if \n is used as delimiter. */
        private static final byte NEW_LINE = (byte) '\n';

        private final DeserializationSchema<RowData> deserializationSchema;

        private transient boolean end;
        private transient RecordCollector collector;

        public LineBytesInputFormat(Path path, Configuration config) throws IOException {
            super(path, config);
            this.deserializationSchema = createDeserialization();
        }

        @Override
        public void open(FileInputSplit split) throws IOException {
            super.open(split);
            this.end = false;
            this.collector = new RecordCollector();
        }

        @Override
        public boolean reachedEnd() {
            return end;
        }

        @Override
        public RowData readRecord(RowData reuse, byte[] bytes, int offset, int numBytes)
                throws IOException {
            // remove \r from a line when the line ends with \r\n
            if (this.getDelimiter() != null
                    && this.getDelimiter().length == 1
                    && this.getDelimiter()[0] == NEW_LINE
                    && offset + numBytes >= 1
                    && bytes[offset + numBytes - 1] == CARRIAGE_RETURN) {
                numBytes -= 1;
            }
            byte[] trimBytes = Arrays.copyOfRange(bytes, offset, offset + numBytes);
            deserializationSchema.deserialize(trimBytes, collector);
            return null;
        }

        @Override
        public RowData nextRecord(RowData reuse) throws IOException {
            while (true) {
                RowData record = collector.records.poll();
                if (record != null) {
                    return record;
                }

                if (readLine()) {
                    readRecord(reuse, this.currBuffer, this.currOffset, this.currLen);
                } else {
                    this.end = true;
                    return null;
                }
            }
        }

        private class RecordCollector implements Collector<RowData> {

            private final Queue<RowData> records = new ArrayDeque<>();

            @Override
            public void collect(RowData record) {
                records.add(record);
            }

            @Override
            public void close() {}
        }
    }
}
