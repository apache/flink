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

package org.apache.flink.connector.file.src.impl;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.connector.testutils.source.reader.TestingReaderContext;
import org.apache.flink.core.fs.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for the {@link FileSourceReader}. */
class FileSourceReaderTest {

    @TempDir private static java.nio.file.Path tmpDir;

    @Test
    void testRequestSplitWhenNoSplitRestored() throws Exception {
        final TestingReaderContext context = new TestingReaderContext();
        final FileSourceReader<String, FileSourceSplit> reader = createReader(context);

        reader.start();
        reader.close();

        assertThat(context.getNumSplitRequests()).isEqualTo(1);
    }

    @Test
    void testNoSplitRequestWhenSplitRestored() throws Exception {
        final TestingReaderContext context = new TestingReaderContext();
        final FileSourceReader<String, FileSourceSplit> reader = createReader(context);

        reader.addSplits(Collections.singletonList(createTestFileSplit()));
        reader.start();
        reader.close();

        assertThat(context.getNumSplitRequests()).isEqualTo(0);
    }

    private static FileSourceReader<String, FileSourceSplit> createReader(
            TestingReaderContext context) {
        return new FileSourceReader<>(
                context, new StreamFormatAdapter<>(new TextLineInputFormat()), new Configuration());
    }

    private static FileSourceSplit createTestFileSplit() throws IOException {
        return new FileSourceSplit("test-id", Path.fromLocalFile(tmpDir.toFile()), 0L, 0L, 0L, 0L);
    }
}
