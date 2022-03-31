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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.testframe.environment.MiniClusterTestEnvironment;
import org.apache.flink.connector.testframe.external.ExternalContext;
import org.apache.flink.connector.testframe.external.ExternalContextFactory;
import org.apache.flink.connector.testframe.external.ExternalSystemSplitDataWriter;
import org.apache.flink.connector.testframe.external.source.DataStreamSourceExternalContext;
import org.apache.flink.connector.testframe.external.source.TestingSourceSettings;
import org.apache.flink.connector.testframe.junit.annotations.TestContext;
import org.apache.flink.connector.testframe.junit.annotations.TestEnv;
import org.apache.flink.connector.testframe.junit.annotations.TestSemantics;
import org.apache.flink.connector.testframe.testsuites.SourceTestSuiteBase;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.util.FileUtils;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import static org.apache.flink.formats.avro.AvroBulkFormatTestUtils.ROW_TYPE;

/** IT cases for {@link AbstractAvroBulkFormat}. */
class AvroBulkFormatITCase extends SourceTestSuiteBase<RowData> {

    private static final RowDataSerializer SERIALIZER = new RowDataSerializer(ROW_TYPE);

    @SuppressWarnings("unused")
    @TestSemantics
    CheckpointingMode[] semantics = new CheckpointingMode[] {CheckpointingMode.EXACTLY_ONCE};

    @SuppressWarnings("unused")
    @TestEnv
    MiniClusterTestEnvironment flink = new MiniClusterTestEnvironment();

    @SuppressWarnings("unused")
    @TestContext
    AvroBulkFormatExternalContext.Factory oneBlockPerFile =
            new AvroBulkFormatExternalContext.Factory(1);

    @SuppressWarnings("unused")
    @TestContext
    AvroBulkFormatExternalContext.Factory twoBlocksPerFile =
            new AvroBulkFormatExternalContext.Factory(2);

    /**
     * {@link ExternalContext} for the IT case. It prepares avro test files and creates avro
     * sources.
     */
    public static class AvroBulkFormatExternalContext
            implements DataStreamSourceExternalContext<RowData> {

        private final Path tmpDir;
        private final int blocksPerFile;
        private int index;

        private AvroBulkFormatExternalContext(int blocksPerFile) {
            try {
                this.tmpDir = Files.createTempDirectory("avro-bulk-format-it-case");
            } catch (IOException e) {
                throw new RuntimeException(
                        "Encountered exception when creating temp directory for tests", e);
            }
            this.blocksPerFile = blocksPerFile;
            this.index = 0;
        }

        @Override
        public Source<RowData, ?, ?> createSource(TestingSourceSettings sourceSettings) {
            if (sourceSettings.getBoundedness() == Boundedness.CONTINUOUS_UNBOUNDED) {
                throw new UnsupportedOperationException(
                        "Currently Avro format only supports running in bounded mode");
            }
            AvroBulkFormatTestUtils.TestingAvroBulkFormat format =
                    new AvroBulkFormatTestUtils.TestingAvroBulkFormat();
            return FileSource.forBulkFileFormat(
                            format, org.apache.flink.core.fs.Path.fromLocalFile(tmpDir.toFile()))
                    .build();
        }

        @Override
        public ExternalSystemSplitDataWriter<RowData> createSourceSplitDataWriter(
                TestingSourceSettings sourceSettings) {
            File file = Paths.get(tmpDir.toString(), String.valueOf(index)).toFile();
            AvroBulkFormatExternalSystemSplitDataWriter writer;
            try {
                file.createNewFile();
                writer =
                        new AvroBulkFormatExternalSystemSplitDataWriter(new FileOutputStream(file));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            index++;
            return writer;
        }

        @Override
        public List<RowData> generateTestData(
                TestingSourceSettings sourceSettings, int splitIndex, long seed) {
            Random random = new Random(seed);
            List<RowData> data = new ArrayList<>();
            for (int i = 0; i < blocksPerFile; i++) {
                data.add(getBinaryRow(randomString(4, random), randomString(8, random)));
                data.add(getBinaryRow(randomString(16, random), null));
                data.add(getBinaryRow(randomString(32, random), randomString(1024, random)));
            }
            return data;
        }

        @Override
        public TypeInformation<RowData> getProducedType() {
            return TypeInformation.of(RowData.class);
        }

        @Override
        public void close() throws Exception {
            FileUtils.deleteDirectory(tmpDir.toFile());
        }

        private StringData randomString(int len, Random random) {
            StringBuilder builder = new StringBuilder();
            for (int i = 0; i < len; i++) {
                builder.append((char) (random.nextInt(26) + 'a'));
            }
            return StringData.fromString(builder.toString());
        }

        private RowData getBinaryRow(Object... values) {
            GenericRowData genericRowData = new GenericRowData(values.length);
            for (int i = 0; i < values.length; i++) {
                genericRowData.setField(i, values[i]);
            }
            BinaryRowData binaryRowData = SERIALIZER.toBinaryRow(genericRowData);
            return SERIALIZER.copy(binaryRowData);
        }

        @Override
        public List<URL> getConnectorJarPaths() {
            return Collections.emptyList();
        }

        /** Factory to create {@link AvroBulkFormatExternalContext}. */
        public static class Factory
                implements ExternalContextFactory<AvroBulkFormatExternalContext> {

            private final int blocksPerFile;

            public Factory(int blocksPerFile) {
                this.blocksPerFile = blocksPerFile;
            }

            @Override
            public AvroBulkFormatExternalContext createExternalContext(String testName) {
                return new AvroBulkFormatExternalContext(blocksPerFile);
            }
        }

        @Override
        public String toString() {
            return "AvroBulkFormatExternalContext{" + "blocksPerFile=" + blocksPerFile + '}';
        }
    }

    private static class AvroBulkFormatExternalSystemSplitDataWriter
            implements ExternalSystemSplitDataWriter<RowData> {

        private final Schema schema;
        private final RowDataToAvroConverters.RowDataToAvroConverter converter;
        private final DataFileWriter<GenericRecord> dataFileWriter;

        private AvroBulkFormatExternalSystemSplitDataWriter(FileOutputStream out)
                throws IOException {
            schema = AvroSchemaConverter.convertToSchema(ROW_TYPE);
            converter = RowDataToAvroConverters.createConverter(ROW_TYPE);

            DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
            dataFileWriter = new DataFileWriter<>(datumWriter);
            dataFileWriter.create(schema, out);
            dataFileWriter.setSyncInterval(128);
        }

        @Override
        public void writeRecords(List<RowData> records) {
            for (RowData rowData : records) {
                try {
                    dataFileWriter.append((GenericRecord) converter.convert(schema, rowData));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        @Override
        public void close() throws Exception {
            dataFileWriter.close();
        }
    }
}
