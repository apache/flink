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

import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

/** Test for {@link LimitableBulkFormat}. */
public class LimitableBulkFormatTest {

    @ClassRule public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

    private File file;

    @Before
    public void prepare() throws IOException {
        file = TEMP_FOLDER.newFile();
        file.createNewFile();
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < 10000; i++) {
            builder.append(i).append("\n");
        }
        FileUtils.writeFileUtf8(file, builder.toString());
    }

    @Test
    public void test() throws IOException {
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
        Assert.assertEquals(22, i.get());
    }

    @Test
    public void testLimitOverBatches() throws IOException {
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
        Assert.assertEquals(limit.intValue(), i.get());
    }

    @Test
    public void testSwallowExceptionWhenLimited() throws IOException {
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
