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
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.avro.generated.Address;
import org.apache.flink.formats.avro.typeutils.GenericRecordAvroTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.UniqueBucketAssigner;
import org.apache.flink.streaming.util.FiniteTestSource;
import org.apache.flink.test.util.AbstractTestBase;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.AbstractCollection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Simple integration test case for writing bulk encoded files with the {@link StreamingFileSink}
 * with Avro.
 */
public class AvroStreamingFileSinkITCase extends AbstractTestBase {

    @Rule public final Timeout timeoutPerTest = Timeout.seconds(20);

    @Test
    public void testWriteAvroSpecific() throws Exception {
        File folder = TEMPORARY_FOLDER.newFolder();

        List<Address> data =
                Arrays.asList(
                        new Address(1, "a", "b", "c", "12345"),
                        new Address(2, "p", "q", "r", "12345"),
                        new Address(3, "x", "y", "z", "12345"));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(100);

        AvroWriterFactory<Address> avroWriterFactory = AvroWriters.forSpecificRecord(Address.class);
        DataStream<Address> stream =
                env.addSource(new FiniteTestSource<>(data), TypeInformation.of(Address.class));
        stream.addSink(
                StreamingFileSink.forBulkFormat(Path.fromLocalFile(folder), avroWriterFactory)
                        .withBucketAssigner(new UniqueBucketAssigner<>("test"))
                        .build());
        env.execute();

        validateResults(folder, new SpecificDatumReader<>(Address.class), data);
    }

    @Test
    public void testWriteAvroGeneric() throws Exception {
        File folder = TEMPORARY_FOLDER.newFolder();

        Schema schema = Address.getClassSchema();
        Collection<GenericRecord> data = new GenericTestDataCollection();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(100);

        AvroWriterFactory<GenericRecord> avroWriterFactory = AvroWriters.forGenericRecord(schema);
        DataStream<GenericRecord> stream =
                env.addSource(new FiniteTestSource<>(data), new GenericRecordAvroTypeInfo(schema));
        stream.addSink(
                StreamingFileSink.forBulkFormat(Path.fromLocalFile(folder), avroWriterFactory)
                        .withBucketAssigner(new UniqueBucketAssigner<>("test"))
                        .build());
        env.execute();

        validateResults(folder, new GenericDatumReader<>(schema), new ArrayList<>(data));
    }

    @Test
    public void testWriteAvroReflect() throws Exception {
        File folder = TEMPORARY_FOLDER.newFolder();

        List<Datum> data = Arrays.asList(new Datum("a", 1), new Datum("b", 2), new Datum("c", 3));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(100);

        AvroWriterFactory<Datum> avroWriterFactory = AvroWriters.forReflectRecord(Datum.class);
        DataStream<Datum> stream =
                env.addSource(new FiniteTestSource<>(data), TypeInformation.of(Datum.class));
        stream.addSink(
                StreamingFileSink.forBulkFormat(Path.fromLocalFile(folder), avroWriterFactory)
                        .withBucketAssigner(new UniqueBucketAssigner<>("test"))
                        .build());
        env.execute();

        validateResults(folder, new ReflectDatumReader<>(Datum.class), data);
    }

    // ------------------------------------------------------------------------

    private static <T> void validateResults(
            File folder, DatumReader<T> datumReader, List<T> expected) throws Exception {
        File[] buckets = folder.listFiles();
        assertThat(buckets).hasSize(1);

        File[] partFiles = buckets[0].listFiles();
        assertThat(partFiles).hasSize(2);

        for (File partFile : partFiles) {
            assertThat(partFile).isNotEmpty();

            final List<T> fileContent = readAvroFile(partFile, datumReader);
            assertThat(fileContent).isEqualTo(expected);
        }
    }

    private static <T> List<T> readAvroFile(File file, DatumReader<T> datumReader)
            throws IOException {
        ArrayList<T> results = new ArrayList<>();

        try (DataFileReader<T> dataFileReader = new DataFileReader<>(file, datumReader)) {
            while (dataFileReader.hasNext()) {
                results.add(dataFileReader.next());
            }
        }

        return results;
    }

    private static class GenericTestDataCollection extends AbstractCollection<GenericRecord>
            implements Serializable {

        @Override
        public Iterator<GenericRecord> iterator() {
            final GenericRecord rec1 = new GenericData.Record(Address.getClassSchema());
            rec1.put(0, 1);
            rec1.put(1, "a");
            rec1.put(2, "b");
            rec1.put(3, "c");
            rec1.put(4, "12345");

            final GenericRecord rec2 = new GenericData.Record(Address.getClassSchema());
            rec2.put(0, 2);
            rec2.put(1, "x");
            rec2.put(2, "y");
            rec2.put(3, "z");
            rec2.put(4, "98765");

            return Arrays.asList(rec1, rec2).iterator();
        }

        @Override
        public int size() {
            return 2;
        }
    }

    // ------------------------------------------------------------------------

    /** Test datum. */
    public static class Datum implements Serializable {

        public String a;
        public int b;

        public Datum() {}

        public Datum(String a, int b) {
            this.a = a;
            this.b = b;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            Datum datum = (Datum) o;
            return b == datum.b && (a != null ? a.equals(datum.a) : datum.a == null);
        }

        @Override
        public int hashCode() {
            int result = a != null ? a.hashCode() : 0;
            result = 31 * result + b;
            return result;
        }
    }
}
