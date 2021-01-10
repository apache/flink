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

package org.apache.flink.formats.parquet.row;

import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.formats.parquet.ParquetBuilder;
import org.apache.flink.formats.parquet.ParquetWriterFactory;
import org.apache.flink.formats.parquet.utils.SerializableConfiguration;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;
import java.util.HashMap;

import static org.apache.flink.formats.parquet.utils.ParquetSchemaConverter.convertToParquetMessageType;
import static org.apache.parquet.hadoop.ParquetOutputFormat.MAX_PADDING_BYTES;
import static org.apache.parquet.hadoop.ParquetOutputFormat.getBlockSize;
import static org.apache.parquet.hadoop.ParquetOutputFormat.getDictionaryPageSize;
import static org.apache.parquet.hadoop.ParquetOutputFormat.getEnableDictionary;
import static org.apache.parquet.hadoop.ParquetOutputFormat.getPageSize;
import static org.apache.parquet.hadoop.ParquetOutputFormat.getValidation;
import static org.apache.parquet.hadoop.ParquetOutputFormat.getWriterVersion;
import static org.apache.parquet.hadoop.codec.CodecConfig.getParquetCompressionCodec;

/** {@link RowData} of {@link ParquetWriter.Builder}. */
public class ParquetRowDataBuilder extends ParquetWriter.Builder<RowData, ParquetRowDataBuilder> {

    private final RowType rowType;
    private final boolean utcTimestamp;

    public ParquetRowDataBuilder(OutputFile path, RowType rowType, boolean utcTimestamp) {
        super(path);
        this.rowType = rowType;
        this.utcTimestamp = utcTimestamp;
    }

    @Override
    protected ParquetRowDataBuilder self() {
        return this;
    }

    @Override
    protected WriteSupport<RowData> getWriteSupport(Configuration conf) {
        return new ParquetWriteSupport();
    }

    private class ParquetWriteSupport extends WriteSupport<RowData> {

        private MessageType schema = convertToParquetMessageType("flink_schema", rowType);
        private ParquetRowDataWriter writer;

        @Override
        public WriteContext init(Configuration configuration) {
            return new WriteContext(schema, new HashMap<>());
        }

        @Override
        public void prepareForWrite(RecordConsumer recordConsumer) {
            this.writer = new ParquetRowDataWriter(recordConsumer, rowType, schema, utcTimestamp);
        }

        @Override
        public void write(RowData record) {
            try {
                this.writer.write(record);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Create a parquet {@link BulkWriter.Factory}.
     *
     * @param rowType row type of parquet table.
     * @param conf hadoop configuration.
     * @param utcTimestamp Use UTC timezone or local timezone to the conversion between epoch time
     *     and LocalDateTime. Hive 0.x/1.x/2.x use local timezone. But Hive 3.x use UTC timezone.
     */
    public static ParquetWriterFactory<RowData> createWriterFactory(
            RowType rowType, Configuration conf, boolean utcTimestamp) {
        return new ParquetWriterFactory<>(new FlinkParquetBuilder(rowType, conf, utcTimestamp));
    }

    /** Flink Row {@link ParquetBuilder}. */
    public static class FlinkParquetBuilder implements ParquetBuilder<RowData> {

        private final RowType rowType;
        private final SerializableConfiguration configuration;
        private final boolean utcTimestamp;

        public FlinkParquetBuilder(RowType rowType, Configuration conf, boolean utcTimestamp) {
            this.rowType = rowType;
            this.configuration = new SerializableConfiguration(conf);
            this.utcTimestamp = utcTimestamp;
        }

        @Override
        public ParquetWriter<RowData> createWriter(OutputFile out) throws IOException {
            Configuration conf = configuration.conf();
            return new ParquetRowDataBuilder(out, rowType, utcTimestamp)
                    .withCompressionCodec(getParquetCompressionCodec(conf))
                    .withRowGroupSize(getBlockSize(conf))
                    .withPageSize(getPageSize(conf))
                    .withDictionaryPageSize(getDictionaryPageSize(conf))
                    .withMaxPaddingSize(
                            conf.getInt(MAX_PADDING_BYTES, ParquetWriter.MAX_PADDING_SIZE_DEFAULT))
                    .withDictionaryEncoding(getEnableDictionary(conf))
                    .withValidation(getValidation(conf))
                    .withWriterVersion(getWriterVersion(conf))
                    .withConf(conf)
                    .build();
        }
    }
}
