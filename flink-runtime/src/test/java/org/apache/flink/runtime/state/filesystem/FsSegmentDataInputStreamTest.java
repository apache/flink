/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state.filesystem;

import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.fs.EntropyInjector;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.OutputStreamAndPath;
import org.apache.flink.core.fs.Path;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.Random;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link FsSegmentDataInputStream}. */
public class FsSegmentDataInputStreamTest {

    @Rule public final TemporaryFolder tmp = new TemporaryFolder();

    private static final CloseableRegistry closeableRegistry = new CloseableRegistry();

    private static final Random random = new Random();

    @Test
    public void testReadFromFileSegments() throws IOException {
        Path dirPath = new Path(tmp.getRoot().getPath());
        Path filePath = new Path(dirPath, UUID.randomUUID().toString());
        byte[] fileContent = prepareFileToRead(filePath, 512);
        int startPos, segmentSize;

        // 1. whole file as one segment
        startPos = 0;
        segmentSize = 512;
        FsSegmentDataInputStream inputStream = openSegment(filePath, startPos, segmentSize);
        closeableRegistry.registerCloseable(inputStream);
        byte[] readResult = new byte[segmentSize];
        int readLen = inputStream.read(readResult);
        assertThat(readLen).isEqualTo(segmentSize);
        assertBytesContentEqual(fileContent, startPos, segmentSize, readResult);
        assertThat(inputStream.read()).isEqualTo(-1);

        // 2. read with a file segment
        startPos = 26;
        segmentSize = 483;
        inputStream = openSegment(filePath, startPos, segmentSize);
        readResult = new byte[segmentSize];
        readLen = inputStream.read(readResult);
        assertThat(readLen).isEqualTo(segmentSize);
        assertBytesContentEqual(fileContent, startPos, segmentSize, readResult);
        assertThat(inputStream.read()).isEqualTo(-1);

        // 3. seek to a relative position
        startPos = 56;
        segmentSize = 123;
        inputStream = openSegment(filePath, startPos, segmentSize);
        int readBufferSize = 32;
        readResult = new byte[readBufferSize];
        int seekPos = 74;
        inputStream.seek(seekPos);
        assertThat(inputStream.getPos()).isEqualTo(seekPos);
        readLen = inputStream.read(readResult);
        assertThat(readLen).isEqualTo(readBufferSize);
        assertBytesContentEqual(fileContent, startPos + seekPos, readLen, readResult);
        assertThat(inputStream.getPos()).isEqualTo(seekPos + readBufferSize);
        // current relative position is (74 + 32 = 106)
        // reading another 32 bytes will cross the segment boundary
        assertThat(inputStream.read(readResult)).isEqualTo(segmentSize - seekPos - readBufferSize);
        assertThat(inputStream.read()).isEqualTo(-1);
        assertThat(inputStream.read(new byte[10])).isEqualTo(-1);
        assertThat(inputStream.read(new byte[10], 0, 1)).isEqualTo(-1);
    }

    private byte[] prepareFileToRead(Path filePath, int fileSize) throws IOException {
        OutputStreamAndPath streamAndPath =
                EntropyInjector.createEntropyAware(
                        filePath.getFileSystem(), filePath, FileSystem.WriteMode.NO_OVERWRITE);
        FSDataOutputStream outputStream = streamAndPath.stream();
        byte[] fileContent = randomBytes(fileSize);
        outputStream.write(fileContent);
        outputStream.close();
        return fileContent;
    }

    private FsSegmentDataInputStream openSegment(
            Path filePath, long startPosition, long segmentSize) throws IOException {

        FSDataInputStream inputStream = filePath.getFileSystem().open(filePath);
        return new FsSegmentDataInputStream(inputStream, startPosition, segmentSize);
    }

    private byte[] randomBytes(int len) {
        byte[] bytes = new byte[len];
        random.nextBytes(bytes);
        return bytes;
    }

    private void assertBytesContentEqual(
            byte[] expected, int startPosInExpected, int sizeInExpected, byte[] actual) {
        assertThat(actual.length).isEqualTo(sizeInExpected);
        byte[] expectedSegment = new byte[sizeInExpected];
        System.arraycopy(expected, startPosInExpected, expectedSegment, 0, sizeInExpected);
        assertThat(actual).isEqualTo(expectedSegment);
    }
}
