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

package org.apache.flink.formats.parquet.avro;

import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.src.reader.StreamFormat;
import org.apache.flink.connector.file.src.util.CheckpointedPosition;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.ParquetWriterFactory;
import org.apache.flink.formats.parquet.generated.Address;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.specific.SpecificData;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Unit test for {@link AvroParquetRecordFormat} and {@link
 * org.apache.flink.connector.file.src.reader.StreamFormat}.
 */
class AvroParquetRecordFormatTest {

    private static final String USER_PARQUET_FILE = "user.parquet";
    private static final String ADDRESS_PARQUET_FILE = "address.parquet";
    private static final String DATUM_PARQUET_FILE = "datum.parquet";

    private static Path userPath;
    private static Path addressPath;
    private static Path datumPath;

    private static Schema schema;

    private static final List<GenericRecord> userRecords = new ArrayList<>(3);
    private static final List<Address> addressRecords = new ArrayList<>(3);
    private static final List<Datum> datumRecords = new ArrayList<>(3);

    @TempDir static java.nio.file.Path temporaryFolder;

    /**
     * Create a parquet file in the {@code TEMPORARY_FOLDER} directory.
     *
     * @throws IOException if new file can not be created.
     */
    @BeforeAll
    static void setup() throws IOException {
        // Generic records
        schema =
                new Schema.Parser()
                        .parse(
                                "{\"type\": \"record\", "
                                        + "\"name\": \"User\", "
                                        + "\"fields\": [\n"
                                        + "        {\"name\": \"name\", \"type\": \"string\" },\n"
                                        + "        {\"name\": \"favoriteNumber\",  \"type\": [\"int\", \"null\"] },\n"
                                        + "        {\"name\": \"favoriteColor\", \"type\": [\"string\", \"null\"] }\n"
                                        + "    ]\n"
                                        + "    }");

        userRecords.add(createUser("Peter", 1, "red"));
        userRecords.add(createUser("Tom", 2, "yellow"));
        userRecords.add(createUser("Jack", 3, "green"));

        userPath = new Path(temporaryFolder.resolve(USER_PARQUET_FILE).toUri());
        createParquetFile(AvroParquetWriters.forGenericRecord(schema), userPath, userRecords);

        // Specific records
        addressRecords.addAll(createAddressList());
        addressPath = new Path(temporaryFolder.resolve(ADDRESS_PARQUET_FILE).toUri());
        createParquetFile(
                AvroParquetWriters.forSpecificRecord(Address.class), addressPath, addressRecords);

        // Reflect records
        datumRecords.addAll(createDatumList());
        datumPath = new Path(temporaryFolder.resolve(DATUM_PARQUET_FILE).toUri());
        createParquetFile(
                AvroParquetWriters.forReflectRecord(Datum.class), datumPath, datumRecords);
    }

    @Test
    void testCreateSpecificReader() throws IOException {
        StreamFormat.Reader<Address> reader =
                createReader(
                        AvroParquetReaders.forSpecificRecord(Address.class),
                        new Configuration(),
                        addressPath,
                        0,
                        addressPath.getFileSystem().getFileStatus(addressPath).getLen());
        for (Address address : addressRecords) {
            Address address1 = Objects.requireNonNull(reader.read());
            assertEquals(address1, address);
        }
    }

    @Test
    void testCreateReflectReader() throws IOException {
        StreamFormat.Reader<Datum> reader =
                createReader(
                        AvroParquetReaders.forReflectRecord(Datum.class),
                        new Configuration(),
                        datumPath,
                        0,
                        datumPath.getFileSystem().getFileStatus(datumPath).getLen());
        for (Datum datum : datumRecords) {
            assertEquals(Objects.requireNonNull(reader.read()), datum);
        }
    }

    @Test
    void testCreateGenericReader() throws IOException {
        StreamFormat.Reader<GenericRecord> reader =
                createReader(
                        AvroParquetReaders.forGenericRecord(schema),
                        new Configuration(),
                        userPath,
                        0,
                        userPath.getFileSystem().getFileStatus(userPath).getLen());
        for (GenericRecord record : userRecords) {
            assertUserEquals(Objects.requireNonNull(reader.read()), record);
        }
    }

    /** Expect exception since splitting is not supported now. */
    @Test
    void testCreateGenericReaderWithSplitting() {
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        createReader(
                                AvroParquetReaders.forGenericRecord(schema),
                                new Configuration(),
                                userPath,
                                5,
                                5));
    }

    @Test
    void testRestoreGenericReaderWithWrongOffset() {
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        restoreReader(
                                AvroParquetReaders.forGenericRecord(schema),
                                new Configuration(),
                                userPath,
                                10,
                                0,
                                userPath.getFileSystem().getFileStatus(userPath).getLen()));
    }

    @Test
    void testReadWithRestoreGenericReader() throws IOException {
        StreamFormat.Reader<GenericRecord> reader =
                restoreReader(
                        AvroParquetReaders.forGenericRecord(schema),
                        new Configuration(),
                        userPath,
                        CheckpointedPosition.NO_OFFSET,
                        0,
                        userPath.getFileSystem().getFileStatus(userPath).getLen());
        for (GenericRecord record : userRecords) {
            assertUserEquals(Objects.requireNonNull(reader.read()), record);
        }
    }

    @Test
    void testSplittable() {
        assertFalse(AvroParquetReaders.forGenericRecord(schema).isSplittable());
    }

    @Test
    void getProducedType() {
        assertEquals(
                AvroParquetReaders.forGenericRecord(schema).getProducedType().getTypeClass(),
                GenericRecord.class);
    }

    @Test
    void getDataModel() {
        assertEquals(
                AvroParquetReaders.forGenericRecord(schema).getDataModel().getClass(),
                GenericData.class);
        assertEquals(
                AvroParquetReaders.forSpecificRecord(Address.class).getDataModel().getClass(),
                SpecificData.class);
        assertEquals(
                AvroParquetReaders.forReflectRecord(Datum.class).getDataModel().getClass(),
                ReflectData.class);
    }

    // ------------------------------------------------------------------------
    //  helper methods
    // ------------------------------------------------------------------------

    private <T> StreamFormat.Reader<T> createReader(
            AvroParquetRecordFormat<T> format,
            Configuration config,
            Path filePath,
            long splitOffset,
            long splitLength)
            throws IOException {

        final FileSystem fileSystem = filePath.getFileSystem();
        final FileStatus fileStatus = fileSystem.getFileStatus(filePath);
        final FSDataInputStream inputStream = fileSystem.open(filePath);

        if (format.isSplittable()) {
            inputStream.seek(splitOffset);
        } else {
            inputStream.seek(0);
            checkArgument(splitLength == fileStatus.getLen());
        }

        return format.createReader(
                config, inputStream, fileStatus.getLen(), splitOffset + splitLength);
    }

    private <T> StreamFormat.Reader<T> restoreReader(
            AvroParquetRecordFormat<T> format,
            Configuration config,
            Path filePath,
            long restoredOffset,
            long splitOffset,
            long splitLength)
            throws IOException {

        final FileSystem fileSystem = filePath.getFileSystem();
        final FileStatus fileStatus = fileSystem.getFileStatus(filePath);
        final FSDataInputStream inputStream = fileSystem.open(filePath);

        if (format.isSplittable()) {
            inputStream.seek(splitOffset);
        } else {
            inputStream.seek(0);
            checkArgument(splitLength == fileStatus.getLen());
        }

        return format.restoreReader(
                config,
                inputStream,
                restoredOffset,
                fileStatus.getLen(),
                splitOffset + splitLength);
    }

    private static <T> void createParquetFile(
            ParquetWriterFactory<T> writerFactory, Path parquetFilePath, List<T> records)
            throws IOException {
        BulkWriter<T> writer =
                writerFactory.create(
                        parquetFilePath
                                .getFileSystem()
                                .create(parquetFilePath, FileSystem.WriteMode.OVERWRITE));

        for (T record : records) {
            writer.addElement(record);
        }

        writer.flush();
        writer.finish();
    }

    private static GenericRecord createUser(String name, int favoriteNumber, String favoriteColor) {
        GenericRecord record = new GenericData.Record(schema);
        record.put("name", name);
        record.put("favoriteNumber", favoriteNumber);
        record.put("favoriteColor", favoriteColor);
        return record;
    }

    private void assertUserEquals(GenericRecord user, GenericRecord expected) {
        assertEquals(user.get("name").toString(), expected.get("name"));
        assertEquals(user.get("favoriteNumber"), expected.get("favoriteNumber"));
        assertEquals(user.get("favoriteColor").toString(), expected.get("favoriteColor"));
    }

    private static List<Address> createAddressList() {
        return Arrays.asList(
                new Address(1, "a", "b", "c", "12345"),
                new Address(2, "p", "q", "r", "12345"),
                new Address(3, "x", "y", "z", "12345"));
    }

    private static List<Datum> createDatumList() {
        return Arrays.asList(new Datum("a", 1), new Datum("b", 2), new Datum("c", 3));
    }
}
