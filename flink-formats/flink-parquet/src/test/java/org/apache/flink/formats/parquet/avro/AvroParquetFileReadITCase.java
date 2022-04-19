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

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.ParquetWriterFactory;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.util.CloseableIterator;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** ITCase for {@link AvroParquetRecordFormat}. */
public class AvroParquetFileReadITCase extends AbstractTestBase {

    private static final int PARALLELISM = 4;
    private static final String USER_PARQUET_FILE_1 = "user1.parquet";
    private static final String USER_PARQUET_FILE_2 = "user2.parquet";
    private static final String USER_PARQUET_FILE_3 = "user3.parquet";

    private static Schema schema;
    private static final List<GenericRecord> userRecords = new ArrayList<>(3);

    @BeforeClass
    public static void setup() throws IOException {
        // Generic records
        schema =
                new Schema.Parser()
                        .parse(
                                "{\"type\": \"record\", "
                                        + "\"name\": \"User\", "
                                        + "\"namespace\": \"org.apache.flink.formats.parquet.avro.AvroParquetRecordFormatTest\", "
                                        + "\"fields\": [\n"
                                        + "        {\"name\": \"name\", \"type\": \"string\" },\n"
                                        + "        {\"name\": \"favoriteNumber\",  \"type\": [\"int\", \"null\"] },\n"
                                        + "        {\"name\": \"favoriteColor\", \"type\": [\"string\", \"null\"] }\n"
                                        + "    ]\n"
                                        + "    }");

        userRecords.add(createUser("Peter", 1, "red"));
        userRecords.add(createUser("Tom", 2, "yellow"));
        userRecords.add(createUser("Jack", 3, "green"));

        createParquetFile(
                AvroParquetWriters.forGenericRecord(schema),
                Path.fromLocalFile(TEMPORARY_FOLDER.newFile(USER_PARQUET_FILE_1)),
                userRecords.toArray(new GenericRecord[0]));

        GenericRecord user = createUser("Max", 4, "blue");
        userRecords.add(user);
        createParquetFile(
                AvroParquetWriters.forGenericRecord(schema),
                Path.fromLocalFile(TEMPORARY_FOLDER.newFile(USER_PARQUET_FILE_2)),
                user);

        user = createUser("Alex", 5, "White");
        GenericRecord user1 = createUser("Anna", 6, "Pink");

        userRecords.add(user);
        userRecords.add(user1);
        createParquetFile(
                AvroParquetWriters.forGenericRecord(schema),
                Path.fromLocalFile(TEMPORARY_FOLDER.newFile(USER_PARQUET_FILE_3)),
                user,
                user1);
    }

    @Test
    public void testReadAvroRecord() throws Exception {
        final FileSource<GenericRecord> source =
                FileSource.forRecordStreamFormat(
                                AvroParquetReaders.forGenericRecord(schema),
                                Path.fromLocalFile(TEMPORARY_FOLDER.getRoot()))
                        .monitorContinuously(Duration.ofMillis(5))
                        .build();
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(PARALLELISM);
        env.enableCheckpointing(10L);

        DataStream<GenericRecord> stream =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "file-source");

        try (CloseableIterator<GenericRecord> iterator =
                stream.executeAndCollect("Reading Avro GenericRecords")) {
            List<GenericRecord> list = collectRecords(iterator, 6);
            assertEquals(list.size(), 6);

            for (int i = 0; i < 6; i++) {
                assertTrue(list.contains(userRecords.get(i)));
            }
        }
    }

    @Test
    public void testReadAvroReflectRecord() throws Exception {
        final FileSource<AvroParquetRecordFormatTest.User> source =
                FileSource.forRecordStreamFormat(
                                AvroParquetReaders.forReflectRecord(
                                        AvroParquetRecordFormatTest.User.class),
                                Path.fromLocalFile(TEMPORARY_FOLDER.getRoot()))
                        .monitorContinuously(Duration.ofMillis(5))
                        .build();
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(PARALLELISM);
        env.enableCheckpointing(10L);

        DataStream<AvroParquetRecordFormatTest.User> stream =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "file-source");

        try (CloseableIterator<AvroParquetRecordFormatTest.User> iterator =
                stream.executeAndCollect("Reading Avro Reflect Records")) {
            List<AvroParquetRecordFormatTest.User> list = collectRecords(iterator, 6);
            Collections.sort(
                    list,
                    Comparator.comparing(AvroParquetRecordFormatTest.User::getFavoriteNumber));
            assertEquals(list.size(), 6);

            for (int i = 0; i < 6; i++) {
                assertUserEquals(list.get(i), userRecords.get(i));
            }
        }
    }

    private static <E> List<E> collectRecords(
            final CloseableIterator<E> iterator, final int numElements) {

        checkNotNull(iterator, "iterator");
        checkArgument(numElements > 0, "numElement must be > 0");

        final ArrayList<E> result = new ArrayList<>(numElements);

        while (iterator.hasNext()) {
            result.add(iterator.next());
            if (result.size() == numElements) {
                break;
            }
        }

        return result;
    }

    @SafeVarargs
    private static <T> void createParquetFile(
            ParquetWriterFactory<T> writerFactory, Path parquetFilePath, T... records)
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

    private void assertUserEquals(AvroParquetRecordFormatTest.User user, GenericRecord expected) {
        assertThat(user).isNotNull();
        assertThat(String.valueOf(user.getName())).isNotNull().isEqualTo(expected.get("name"));
        assertThat(user.getFavoriteNumber()).isEqualTo(expected.get("favoriteNumber"));
        assertThat(String.valueOf(user.getFavoriteColor()))
                .isEqualTo(String.valueOf(expected.get("favoriteColor")));
    }

    private static GenericRecord createUser(String name, int favoriteNumber, String favoriteColor) {
        GenericRecord record = new GenericData.Record(schema);
        record.put("name", name);
        record.put("favoriteNumber", favoriteNumber);
        record.put("favoriteColor", favoriteColor);
        return record;
    }
}
