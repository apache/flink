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

package org.apache.flink.formats.sequencefile;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.UniqueBucketAssigner;
import org.apache.flink.streaming.util.FiniteTestSource;
import org.apache.flink.test.junit5.MiniClusterExtension;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test case for writing bulk encoded files with the {@link StreamingFileSink} with
 * SequenceFile.
 */
@ExtendWith(MiniClusterExtension.class)
class SequenceStreamingFileSinkITCase {

    private final Configuration configuration = new Configuration();

    private final List<Tuple2<Long, String>> testData =
            Arrays.asList(new Tuple2<>(1L, "a"), new Tuple2<>(2L, "b"), new Tuple2<>(3L, "c"));

    @Test
    void testWriteSequenceFile(@TempDir final File folder) throws Exception {
        final Path testPath = Path.fromLocalFile(folder);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(100);

        DataStream<Tuple2<Long, String>> stream =
                env.addSource(
                        new FiniteTestSource<>(testData),
                        TypeInformation.of(new TypeHint<Tuple2<Long, String>>() {}));

        stream.map(
                        new MapFunction<Tuple2<Long, String>, Tuple2<LongWritable, Text>>() {
                            @Override
                            public Tuple2<LongWritable, Text> map(Tuple2<Long, String> value)
                                    throws Exception {
                                return new Tuple2<>(new LongWritable(value.f0), new Text(value.f1));
                            }
                        })
                .addSink(
                        StreamingFileSink.forBulkFormat(
                                        testPath,
                                        new SequenceFileWriterFactory<>(
                                                configuration,
                                                LongWritable.class,
                                                Text.class,
                                                "BZip2"))
                                .withBucketAssigner(new UniqueBucketAssigner<>("test"))
                                .build());

        env.execute();

        validateResults(folder, testData);
    }

    private List<Tuple2<Long, String>> readSequenceFile(File file) throws IOException {
        SequenceFile.Reader reader =
                new SequenceFile.Reader(
                        configuration,
                        SequenceFile.Reader.file(new org.apache.hadoop.fs.Path(file.toURI())));
        LongWritable key = new LongWritable();
        Text val = new Text();
        ArrayList<Tuple2<Long, String>> results = new ArrayList<>();
        while (reader.next(key, val)) {
            results.add(new Tuple2<>(key.get(), val.toString()));
        }
        reader.close();
        return results;
    }

    private void validateResults(File folder, List<Tuple2<Long, String>> expected)
            throws Exception {
        File[] buckets = folder.listFiles();
        assertThat(buckets).isNotNull();
        assertThat(buckets).hasSize(1);

        final File[] partFiles = buckets[0].listFiles();
        assertThat(partFiles).isNotNull();
        assertThat(partFiles).hasSize(2);

        for (File partFile : partFiles) {
            assertThat(partFile.length()).isGreaterThan(0);

            final List<Tuple2<Long, String>> fileContent = readSequenceFile(partFile);
            assertThat(fileContent).isEqualTo(expected);
        }
    }
}
