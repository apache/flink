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

package org.apache.flink.fs.osshadoop;

import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.RefCountedBufferingFileStream;
import org.apache.flink.core.fs.RefCountedFileWithStream;
import org.apache.flink.fs.osshadoop.writer.OSSRecoverableMultipartUpload;

import org.junit.rules.TemporaryFolder;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.SplittableRandom;
import java.util.UUID;

import static junit.framework.TestCase.assertEquals;

/** OSS test utility class. */
public class OSSTestUtils {
    private static final int BUFFER_SIZE = 10;

    public static void objectContentEquals(
            FileSystem fs, Path objectPath, List<byte[]> expectContents) throws IOException {
        String actualContent;
        try (FSDataInputStream in = fs.open(objectPath);
                ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            byte[] buffer = new byte[4096];
            int bytes = in.read(buffer);
            while (bytes != -1) {
                out.write(buffer, 0, bytes);
                bytes = in.read(buffer);
            }
            actualContent = out.toString(StandardCharsets.UTF_8.name());
        }

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        for (byte[] bytes : expectContents) {
            out.write(bytes);
        }
        assertEquals(out.toString(), actualContent);
    }

    public static void objectContentEquals(FileSystem fs, Path objectPath, byte[]... expectContents)
            throws IOException {
        objectContentEquals(fs, objectPath, Arrays.asList(expectContents));
    }

    public static byte[] bytesOf(String str, long requiredSize) {
        StringBuilder sb = new StringBuilder();
        while (sb.length() < requiredSize) {
            sb.append(str);
        }
        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }

    public static void uploadPart(
            OSSRecoverableMultipartUpload uploader,
            final TemporaryFolder temporaryFolder,
            final byte[] content)
            throws IOException {
        RefCountedBufferingFileStream partFile = writeData(temporaryFolder, content);

        partFile.close();

        uploader.uploadPart(partFile);
    }

    public static RefCountedBufferingFileStream writeData(
            TemporaryFolder temporaryFolder, byte[] content) throws IOException {
        final File newFile = new File(temporaryFolder.getRoot(), ".tmp_" + UUID.randomUUID());
        final OutputStream out =
                Files.newOutputStream(newFile.toPath(), StandardOpenOption.CREATE_NEW);

        final RefCountedBufferingFileStream testStream =
                new RefCountedBufferingFileStream(
                        RefCountedFileWithStream.newFile(newFile, out), BUFFER_SIZE);

        testStream.write(content, 0, content.length);
        return testStream;
    }

    public static List<byte[]> generateRandomBuffer(long size, int partSize) {
        List<byte[]> buffers = new ArrayList<>();

        final SplittableRandom random = new SplittableRandom();

        long totalSize = 0L;

        while (totalSize < size) {
            int bufferSize = random.nextInt(0, 2 * partSize);
            byte[] buffer = new byte[bufferSize];
            for (int i = 0; i < bufferSize; ++i) {
                buffer[i] = (byte) (random.nextInt() & 0xFF);
            }

            buffers.add(buffer);
            totalSize += bufferSize;
        }

        return buffers;
    }
}
