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

package org.apache.flink.formats.csv;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.TestDataGenerators;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

public class CsvBulkWriterIT {

    @TempDir File outDir;

    /**
     * FLINK-35240 : Verifies that Jackson CSV writer does not flush per record but waits for a
     * flush signal from Flink.
     */
    @Test
    public void testNoDataIsWrittenBeforeFlinkFlush() throws Exception {

        Configuration config = new Configuration();
        config.set(
                RestartStrategyOptions.RESTART_STRATEGY,
                RestartStrategyOptions.RestartStrategyType.NO_RESTART_STRATEGY.getMainValue());
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
        env.setParallelism(1);
        env.enableCheckpointing(100);

        // Workaround serialization limitations
        File outDirRef = new File(outDir.getAbsolutePath());

        FileSink<Pojo> sink =
                FileSink.forBulkFormat(
                                new org.apache.flink.core.fs.Path(outDir.getAbsolutePath()),
                                out -> {
                                    FSDataOutputStreamWrapper outputStreamWrapper =
                                            new FSDataOutputStreamWrapper(out);
                                    return new CsvBulkWriterWrapper<>(
                                            CsvBulkWriter.forPojo(Pojo.class, outputStreamWrapper),
                                            outputStreamWrapper,
                                            outDirRef);
                                })
                        .build();

        List<Pojo> integers = Arrays.asList(new Pojo(1), new Pojo(2));
        DataGeneratorSource<Pojo> generatorSource =
                TestDataGenerators.fromDataWithSnapshotsLatch(
                        integers, TypeInformation.of(Pojo.class));
        env.fromSource(generatorSource, WatermarkStrategy.noWatermarks(), "").sinkTo(sink);
        env.execute();
        assertThat(getResultsFromSinkFiles(outDir)).containsSequence("1", "2", "1", "2");
    }

    private static class CsvBulkWriterWrapper<T> implements BulkWriter<T> {

        private static int addedElements = 0;

        private static int expectedFlushedElements = 0;

        private final CsvBulkWriter<T, ?, ?> csvBulkWriter;

        private final File outDir;

        private final FSDataOutputStreamWrapper stream;

        CsvBulkWriterWrapper(
                CsvBulkWriter<T, ?, ?> csvBulkWriter,
                FSDataOutputStreamWrapper stream,
                File outDir) {
            this.csvBulkWriter = csvBulkWriter;
            this.stream = stream;
            this.outDir = outDir;
        }

        @Override
        public void addElement(T element) throws IOException {
            addedElements++;
            csvBulkWriter.addElement(element);
            assertThat(getResultsFromSinkFiles(outDir)).hasSize(expectedFlushedElements);
        }

        @Override
        public void flush() throws IOException {
            csvBulkWriter.flush();
            expectedFlushedElements = addedElements;
            assertThat(getResultsFromSinkFiles(outDir)).hasSize(expectedFlushedElements);
        }

        @Override
        public void finish() throws IOException {
            csvBulkWriter.finish();
            // The stream should not be closed by the CsvBulkWriter.finish() method
            assertThat(stream.closed).isFalse();
        }
    }

    private static class FSDataOutputStreamWrapper extends FSDataOutputStream {

        private boolean closed = false;

        private final FSDataOutputStream stream;

        FSDataOutputStreamWrapper(FSDataOutputStream stream) {
            this.stream = stream;
        }

        @Override
        public long getPos() throws IOException {
            return stream.getPos();
        }

        @Override
        public void write(int b) throws IOException {
            stream.write(b);
        }

        @Override
        public void write(byte[] b) throws IOException {
            stream.write(b);
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            stream.write(b, off, len);
        }

        @Override
        public void flush() throws IOException {
            stream.flush();
        }

        @Override
        public void sync() throws IOException {
            stream.sync();
        }

        @Override
        public void close() throws IOException {
            stream.close();
            closed = true;
        }
    }

    public static class Pojo {
        public long x;

        public Pojo(long x) {
            this.x = x;
        }

        public Pojo() {}
    }

    private static List<String> getResultsFromSinkFiles(File outDir) throws IOException {
        final Map<File, String> contents = getFileContentByPath(outDir);
        return contents.entrySet().stream()
                .flatMap(e -> Arrays.stream(e.getValue().split("\n")))
                .filter(s -> !s.isEmpty())
                .collect(Collectors.toList());
    }

    private static Map<File, String> getFileContentByPath(File directory) throws IOException {
        Map<File, String> contents = new HashMap<>();

        final Collection<File> filesInBucket = FileUtils.listFiles(directory, null, true);
        for (File file : filesInBucket) {
            contents.put(file, FileUtils.readFileToString(file));
        }
        return contents;
    }
}
