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

package org.apache.flink.formats.parquet.protobuf;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.UniqueBucketAssigner;
import org.apache.flink.streaming.util.FiniteTestSource;
import org.apache.flink.test.junit5.MiniClusterExtension;

import com.google.protobuf.Message;
import com.google.protobuf.MessageOrBuilder;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.proto.ProtoParquetReader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.google.protobuf.Message.Builder;
import static org.apache.flink.formats.parquet.protobuf.SimpleRecord.SimpleProtoRecord;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Simple integration test case for writing bulk encoded files with the {@link StreamingFileSink}
 * with Parquet.
 */
@ExtendWith(MiniClusterExtension.class)
class ParquetProtoStreamingFileSinkITCase {

    @Test
    void testParquetProtoWriters(@TempDir File folder) throws Exception {

        List<SimpleProtoRecord> data =
                Arrays.asList(
                        SimpleProtoRecord.newBuilder().setFoo("a").setBar("x").setNum(1).build(),
                        SimpleProtoRecord.newBuilder().setFoo("b").setBar("y").setNum(2).build(),
                        SimpleProtoRecord.newBuilder().setFoo("c").setBar("z").setNum(3).build());

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(100);

        DataStream<SimpleProtoRecord> stream =
                env.addSource(
                        new FiniteTestSource<>(data), TypeInformation.of(SimpleProtoRecord.class));

        stream.addSink(
                StreamingFileSink.forBulkFormat(
                                Path.fromLocalFile(folder),
                                ParquetProtoWriters.forType(SimpleProtoRecord.class))
                        .withBucketAssigner(new UniqueBucketAssigner<>("test"))
                        .build());

        env.execute();

        validateResults(folder, data);
    }

    // ------------------------------------------------------------------------

    private static <T extends MessageOrBuilder> void validateResults(File folder, List<T> expected)
            throws Exception {
        File[] buckets = folder.listFiles();
        assertThat(buckets).hasSize(1);

        File[] partFiles = buckets[0].listFiles();
        assertThat(partFiles).hasSize(2);

        for (File partFile : partFiles) {
            assertThat(partFile.length()).isGreaterThan(0);

            final List<Message> fileContent = readParquetFile(partFile);
            assertThat(fileContent).isEqualTo(expected);
        }
    }

    private static List<Message> readParquetFile(File file) throws IOException {
        org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(file.getAbsolutePath());

        ArrayList<Message> results = new ArrayList<>();
        try (ParquetReader<MessageOrBuilder> reader =
                ProtoParquetReader.<MessageOrBuilder>builder(path).build()) {
            MessageOrBuilder next;
            while ((next = reader.read()) != null) {
                if (next instanceof Builder) {
                    // Builder is mutable and we need to ensure the saved reference does not change
                    // after reading more records.
                    results.add(((Builder) next).build());
                } else {
                    results.add((Message) next);
                }
            }
        }

        return results;
    }
}
