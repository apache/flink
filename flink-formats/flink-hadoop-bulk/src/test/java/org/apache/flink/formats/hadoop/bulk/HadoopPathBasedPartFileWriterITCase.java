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

package org.apache.flink.formats.hadoop.bulk;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.HadoopPathBasedBulkFormatBuilder;
import org.apache.flink.streaming.api.functions.sink.filesystem.TestStreamingFileSinkFactory;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.BasePathBucketAssigner;
import org.apache.flink.streaming.util.FiniteTestSource;
import org.apache.flink.test.util.AbstractTestBase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.flink.formats.hadoop.bulk.HadoopPathBasedPartFileWriter.HadoopPathBasedPendingFileRecoverable;
import static org.apache.flink.formats.hadoop.bulk.HadoopPathBasedPartFileWriter.HadoopPathBasedPendingFileRecoverableSerializer;
import static org.assertj.core.api.Assertions.assertThat;

/** Base class for testing writing data to the hadoop file system with different configurations. */
public class HadoopPathBasedPartFileWriterITCase extends AbstractTestBase {
    @Rule public final Timeout timeoutPerTest = Timeout.seconds(2000);

    @Test
    public void testPendingFileRecoverableSerializer() throws IOException {
        HadoopPathBasedPendingFileRecoverable recoverable =
                new HadoopPathBasedPendingFileRecoverable(
                        new Path("hdfs://fake/path"), new Path("hdfs://fake/path.inprogress.uuid"));
        HadoopPathBasedPendingFileRecoverableSerializer serializer =
                new HadoopPathBasedPendingFileRecoverableSerializer();

        byte[] serializedBytes = serializer.serialize(recoverable);
        HadoopPathBasedPendingFileRecoverable deSerialized =
                serializer.deserialize(serializer.getVersion(), serializedBytes);

        assertThat(deSerialized.getTargetFilePath()).isEqualTo(recoverable.getTargetFilePath());
        assertThat(deSerialized.getTempFilePath()).isEqualTo(recoverable.getTempFilePath());
    }

    @Test
    public void testWriteFile() throws Exception {
        File file = TEMPORARY_FOLDER.newFolder();
        Path basePath = new Path(file.toURI());

        List<String> data = Arrays.asList("first line", "second line", "third line");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(100);

        // FiniteTestSource will generate two elements with a checkpoint trigger in between the two
        // elements
        DataStream<String> stream =
                env.addSource(new FiniteTestSource<>(data), TypeInformation.of(String.class));
        Configuration configuration = new Configuration();
        // Elements from source are going to be assigned to one bucket
        HadoopPathBasedBulkFormatBuilder<String, String, ?> builder =
                new HadoopPathBasedBulkFormatBuilder<>(
                        basePath,
                        new TestHadoopPathBasedBulkWriterFactory(),
                        configuration,
                        new BasePathBucketAssigner<>());
        TestStreamingFileSinkFactory<String> streamingFileSinkFactory =
                new TestStreamingFileSinkFactory<>();
        stream.addSink(streamingFileSinkFactory.createSink(builder, 1000));

        env.execute();
        validateResult(data, configuration, basePath);
    }

    // ------------------------------------------------------------------------

    private void validateResult(List<String> expected, Configuration config, Path basePath)
            throws IOException {
        FileSystem fileSystem = FileSystem.get(basePath.toUri(), config);
        FileStatus[] partFiles = fileSystem.listStatus(basePath);
        assertThat(partFiles).isNotNull();
        assertThat(partFiles).hasSize(2);
        for (FileStatus partFile : partFiles) {
            assertThat(partFile.getLen()).isGreaterThan(0);

            List<String> fileContent = readHadoopPath(fileSystem, partFile.getPath());
            assertThat(fileContent).isEqualTo(expected);
        }
    }

    private List<String> readHadoopPath(FileSystem fileSystem, Path partFile) throws IOException {
        try (FSDataInputStream dataInputStream = fileSystem.open(partFile)) {
            List<String> lines = new ArrayList<>();
            BufferedReader reader = new BufferedReader(new InputStreamReader(dataInputStream));
            String line = null;
            while ((line = reader.readLine()) != null) {
                lines.add(line);
            }

            return lines;
        }
    }
}
