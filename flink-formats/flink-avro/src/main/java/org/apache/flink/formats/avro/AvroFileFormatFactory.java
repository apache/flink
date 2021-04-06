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

package org.apache.flink.formats.avro;

import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.BulkWriterFormatFactory;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/** Avro format factory for file system. */
public class AvroFileFormatFactory implements BulkWriterFormatFactory {

    public static final String IDENTIFIER = "avro";

    public static final ConfigOption<String> AVRO_OUTPUT_CODEC =
            ConfigOptions.key("codec")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The compression codec for avro");

    @Override
    public EncodingFormat<BulkWriter.Factory<RowData>> createEncodingFormat(
            DynamicTableFactory.Context context, ReadableConfig formatOptions) {
        return new EncodingFormat<BulkWriter.Factory<RowData>>() {

            @Override
            public ChangelogMode getChangelogMode() {
                return ChangelogMode.insertOnly();
            }

            @Override
            public BulkWriter.Factory<RowData> createRuntimeEncoder(
                    DynamicTableSink.Context context, DataType consumedDataType) {
                return new RowDataAvroWriterFactory(
                        (RowType) consumedDataType.getLogicalType(),
                        formatOptions.get(AVRO_OUTPUT_CODEC));
            }
        };
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return new HashSet<>();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(AVRO_OUTPUT_CODEC);
        return options;
    }

    /**
     * A {@link BulkWriter.Factory} to convert {@link RowData} to {@link GenericRecord} and wrap
     * {@link AvroWriterFactory}.
     */
    private static class RowDataAvroWriterFactory implements BulkWriter.Factory<RowData> {

        private static final long serialVersionUID = 1L;

        private final AvroWriterFactory<GenericRecord> factory;
        private final RowType rowType;

        private RowDataAvroWriterFactory(RowType rowType, String codec) {
            this.rowType = rowType;
            this.factory =
                    new AvroWriterFactory<>(
                            (AvroBuilder<GenericRecord>)
                                    out -> {
                                        Schema schema =
                                                AvroSchemaConverter.convertToSchema(rowType);
                                        DatumWriter<GenericRecord> datumWriter =
                                                new GenericDatumWriter<>(schema);
                                        DataFileWriter<GenericRecord> dataFileWriter =
                                                new DataFileWriter<>(datumWriter);

                                        if (codec != null) {
                                            dataFileWriter.setCodec(CodecFactory.fromString(codec));
                                        }
                                        dataFileWriter.create(schema, out);
                                        return dataFileWriter;
                                    });
        }

        @Override
        public BulkWriter<RowData> create(FSDataOutputStream out) throws IOException {
            BulkWriter<GenericRecord> writer = factory.create(out);
            RowDataToAvroConverters.RowDataToAvroConverter converter =
                    RowDataToAvroConverters.createConverter(rowType);
            Schema schema = AvroSchemaConverter.convertToSchema(rowType);
            return new BulkWriter<RowData>() {

                @Override
                public void addElement(RowData element) throws IOException {
                    GenericRecord record = (GenericRecord) converter.convert(schema, element);
                    writer.addElement(record);
                }

                @Override
                public void flush() throws IOException {
                    writer.flush();
                }

                @Override
                public void finish() throws IOException {
                    writer.finish();
                }
            };
        }
    }
}
