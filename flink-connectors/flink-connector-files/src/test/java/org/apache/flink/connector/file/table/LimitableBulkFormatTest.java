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

package org.apache.flink.connector.file.table;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.impl.StreamFormatAdapter;
import org.apache.flink.connector.file.src.reader.BulkFormat;
import org.apache.flink.connector.file.src.reader.SimpleStreamFormat;
import org.apache.flink.connector.file.src.reader.StreamFormat;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.connector.file.src.util.Utils;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.Path;
import org.apache.flink.util.FileUtils;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link LimitableBulkFormat}. */
class LimitableBulkFormatTest {

    @TempDir private java.nio.file.Path path;
    private File file;

    @BeforeEach
    void prepare() throws IOException {
        file = Files.createTempFile(path, "prefix", "suffix").toFile();
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < 10000; i++) {
            builder.append(i).append("\n");
        }
        FileUtils.writeFileUtf8(file, builder.toString());
    }

    @Test
    void test() throws IOException {
        // read
        BulkFormat<String, FileSourceSplit> format =
                LimitableBulkFormat.create(
                        new StreamFormatAdapter<>(new TextLineInputFormat()), 22L);

        BulkFormat.Reader<String> reader =
                format.createReader(
                        new Configuration(),
                        new FileSourceSplit(
                                "id",
                                new Path(file.toURI()),
                                0,
                                file.length(),
                                file.lastModified(),
                                file.length()));

        AtomicInteger i = new AtomicInteger(0);
        Utils.forEachRemaining(reader, s -> i.incrementAndGet());
        assertThat(i.get()).isEqualTo(22);
    }

    @Test
    void testLimitOverBatches() throws IOException {
        // set limit
        Long limit = 2048L;

        // configuration for small batches
        Configuration conf = new Configuration();
        conf.set(StreamFormat.FETCH_IO_SIZE, MemorySize.parse("4k"));

        // read
        BulkFormat<String, FileSourceSplit> format =
                LimitableBulkFormat.create(
                        new StreamFormatAdapter<>(new TextLineInputFormat()), limit);

        BulkFormat.Reader<String> reader =
                format.createReader(
                        conf,
                        new FileSourceSplit(
                                "id",
                                new Path(file.toURI()),
                                0,
                                file.length(),
                                file.lastModified(),
                                file.length()));

        // check
        AtomicInteger i = new AtomicInteger(0);
        Utils.forEachRemaining(reader, s -> i.incrementAndGet());
        assertThat(i.get()).isEqualTo(limit.intValue());
    }

    @Test
    void testSwallowExceptionWhenLimited() throws IOException {
        long limit = 1000L;
        LimitableBulkFormat<String, FileSourceSplit> format =
                (LimitableBulkFormat<String, FileSourceSplit>)
                        LimitableBulkFormat.create(
                                new StreamFormatAdapter<>(new FailedFormat()), limit);

        BulkFormat.Reader<String> reader =
                format.createReader(
                        new Configuration(),
                        new FileSourceSplit("id", new Path(file.toURI()), 0, file.length()));

        format.globalNumberRead().set(limit + 1);

        // should swallow exception
        reader.readBatch();
    }

    private static class FailedFormat extends SimpleStreamFormat<String> {

        @Override
        public FailedReader createReader(Configuration config, FSDataInputStream stream)
                throws IOException {
            return new FailedReader();
        }

        @Override
        public TypeInformation<String> getProducedType() {
            return Types.STRING;
        }
    }

    private static final class FailedReader implements StreamFormat.Reader<String> {

        @Nullable
        @Override
        public String read() throws IOException {
            throw new RuntimeException();
        }

        @Override
        public void close() throws IOException {}
    }
}
