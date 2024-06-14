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

package org.apache.flink.connector.file.sink.compactor.operator;

import org.apache.flink.connector.file.sink.compactor.AbstractCompactTestBase;
import org.apache.flink.connector.file.sink.compactor.ConcatFileCompactor;
import org.apache.flink.connector.file.sink.compactor.FileCompactor;
import org.apache.flink.connector.file.sink.compactor.RecordWiseFileCompactor;
import org.apache.flink.connector.file.sink.utils.TestBucketWriter;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.functions.sink.filesystem.CompactingFileWriter.Type;

import org.junit.jupiter.api.Test;

import java.io.OutputStream;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link CompactService}. */
class CompactServiceTest extends AbstractCompactTestBase {

    @Test
    void testOutputStreamWriterType() {
        final FileCompactor compactor = new ConcatFileCompactor();
        final Type writerType = CompactService.getWriterType(compactor);

        assertThat(writerType).isEqualTo(Type.OUTPUT_STREAM);
    }

    @Test
    void testRecordWiseWriterType() {
        final FileCompactor compactor = new RecordWiseFileCompactor<>(null);
        final Type writerType = CompactService.getWriterType(compactor);

        assertThat(writerType).isEqualTo(Type.RECORD_WISE);
    }

    @Test
    void testCompressedDoCompactCall() throws Exception {
        final DummyCompressedFileCompactor dummyCompactor = new DummyCompressedFileCompactor();
        final CompactService compactService =
                new CompactService(0, dummyCompactor, new TestBucketWriter());

        compactService.compact(
                rawRequest(
                        "0",
                        Arrays.asList(committable("0", ".0", 5), committable("0", ".1", 5)),
                        null));

        assertThat(dummyCompactor.compactCalled).isTrue();
    }

    private static class DummyCompressedFileCompactor extends ConcatFileCompactor {

        boolean compactCalled;

        @Override
        protected void doCompact(List<Path> inputFiles, OutputStream outputStream) {
            compactCalled = true;
        }
    }
}
