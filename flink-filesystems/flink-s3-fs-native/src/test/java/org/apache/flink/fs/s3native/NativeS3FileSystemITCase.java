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

package org.apache.flink.fs.s3native;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.RecoverableFsDataOutputStream;
import org.apache.flink.core.fs.RecoverableWriter;
import org.apache.flink.core.testutils.AllCallbackWrapper;
import org.apache.flink.core.testutils.TestContainerExtension;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/** Exercises native S3 filesystem operations directly. */
class NativeS3FileSystemITCase {

    @RegisterExtension
    private static final AllCallbackWrapper<TestContainerExtension<SeaweedFsNativeS3TestContainer>>
            SEAWEEDFS_EXTENSION =
                    new AllCallbackWrapper<>(
                            new TestContainerExtension<>(SeaweedFsNativeS3TestContainer::new));

    private static FileSystem fs;
    private static String bucketUri;

    private static SeaweedFsNativeS3TestContainer container() {
        return SEAWEEDFS_EXTENSION.getCustomExtension().getTestContainer();
    }

    @BeforeAll
    static void setUp() throws Exception {
        final Configuration config = new Configuration();
        container().setS3ConfigOptions(config);

        final NativeS3FileSystemFactory factory = new NativeS3FileSystemFactory();
        factory.configure(config);

        bucketUri = container().getS3UriForDefaultBucket();
        fs = factory.create(URI.create(bucketUri + "/"));
    }

    @Test
    void testWriteReadAndStat() throws Exception {
        final Path file = path("dir/" + UUID.randomUUID() + ".txt");
        final byte[] data = "hello seaweedfs".getBytes(StandardCharsets.UTF_8);
        write(file, data);

        assertThat(fs.exists(file)).isTrue();
        assertThat(fs.getFileStatus(file).getLen()).isEqualTo(data.length);
        assertThat(read(file, data.length)).isEqualTo(data);
    }

    @Test
    void testListRenameDelete() throws Exception {
        final String dir = "listdir-" + UUID.randomUUID();
        final Path a = path(dir + "/a.txt");
        final Path b = path(dir + "/b.txt");
        write(a, "a".getBytes(StandardCharsets.UTF_8));
        write(b, "b".getBytes(StandardCharsets.UTF_8));

        final FileStatus[] listed = fs.listStatus(path(dir));
        assertThat(listed)
                .extracting(status -> status.getPath().getName())
                .containsExactlyInAnyOrder("a.txt", "b.txt");

        final Path renamed = path(dir + "/c.txt");
        assertThat(fs.rename(a, renamed)).isTrue();
        assertThat(fs.exists(a)).isFalse();
        assertThat(fs.exists(renamed)).isTrue();

        assertThat(fs.delete(path(dir), true)).isTrue();
        assertThat(fs.exists(renamed)).isFalse();
    }

    @Test
    void testMkdirsDoesNotThrowOnObjectStore() {
        // S3 has no real directories, so mkdirs() on an object store is a no-op that must not
        // throw, even though nothing is actually created.
        assertThatCode(() -> fs.mkdirs(path("mkdir-" + UUID.randomUUID())))
                .doesNotThrowAnyException();
    }

    @Test
    void testRecoverableWriterMultipartCommit() throws Exception {
        final Path file = path("recoverable-" + UUID.randomUUID() + ".bin");
        // Bigger than the S3 multipart minimum part size so the commit exercises a real
        // multipart upload rather than a single-shot put.
        final byte[] data =
                payload((int) NativeS3FileSystemFactory.S3_MULTIPART_MIN_PART_SIZE + (1024 * 1024));

        final RecoverableWriter writer = fs.createRecoverableWriter();
        final RecoverableFsDataOutputStream out = writer.open(file);
        out.write(data);
        out.persist();
        out.closeForCommit().commit();

        assertThat(fs.getFileStatus(file).getLen()).isEqualTo(data.length);
        assertThat(read(file, data.length)).isEqualTo(data);
    }

    private static Path path(String name) {
        return new Path(bucketUri + "/" + name);
    }

    private static void write(Path path, byte[] data) throws Exception {
        try (FSDataOutputStream out = fs.create(path, FileSystem.WriteMode.OVERWRITE)) {
            out.write(data);
        }
    }

    private static byte[] read(Path path, int length) throws Exception {
        final byte[] target = new byte[length];
        try (FSDataInputStream in = fs.open(path)) {
            int offset = 0;
            while (offset < length) {
                final int read = in.read(target, offset, length - offset);
                if (read <= 0) {
                    // read == 0 is treated as EOF.
                    // To avoid spinning without progress just breakout.
                    break;
                }
                offset += read;
            }
            assertThat(offset).isEqualTo(length);
        }
        return target;
    }

    private static byte[] payload(int size) {
        final byte[] data = new byte[size];
        for (int i = 0; i < size; i++) {
            data[i] = (byte) (i % 127);
        }
        return data;
    }
}
