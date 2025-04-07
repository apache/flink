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

package org.apache.flink.fs.gs.writer;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.fs.gs.GSFileSystemOptions;
import org.apache.flink.fs.gs.TestUtils;
import org.apache.flink.fs.gs.storage.GSBlobIdentifier;
import org.apache.flink.fs.gs.storage.MockBlobStorage;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameter;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;
import org.apache.flink.util.function.ThrowingRunnable;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Random;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test {@link GSResumeRecoverable}. */
@ExtendWith(ParameterizedTestExtension.class)
class GSRecoverableFsDataOutputStreamTest {

    @Parameter private boolean empty;

    @Parameter(value = 1)
    @Nullable
    private String temporaryBucketName;

    @Parameter(value = 2)
    private int componentObjectCount;

    @Parameter(value = 3)
    private long position;

    @Parameter(value = 4)
    private boolean closed;

    @Parameters(
            name =
                    "empty={0}, temporaryBucketName={1}, componentObjectCount={2}, position={3}, closed={4}")
    private static Collection<Object[]> data() {
        return Arrays.asList(
                new Object[][] {
                    // not empty, no explicit temporary bucket, 0 components, position=0, not closed
                    {false, null, 0, 0, false},
                    // not empty, no explicit temporary bucket, 4 components, position=32, not
                    // closed
                    {false, null, 4, 32, true},
                    // not empty, explicit temporary bucket, 4 components, position=32, not closed
                    {false, "temporary-bucket", 4, 32, false},
                    // not empty, explicit temporary bucket, 4 components, position=64, closed
                    {false, "temporary-bucket", 4, 64, true},
                    // empty, no explicit temporary bucket, 0 components, position=0, not closed
                    {true, null, 0, 0, false},
                    // empty, explicit temporary bucket, 0 components, position=0, not closed
                    {true, "temporary-bucket", 0, 0, false},
                });
    }

    private Random random;

    private GSFileSystemOptions options;

    private MockBlobStorage blobStorage;

    private ArrayList<UUID> componentObjectIds;

    private GSRecoverableFsDataOutputStream fsDataOutputStream;

    private GSBlobIdentifier blobIdentifier;

    private byte byteValue;

    @BeforeEach
    void before() {

        random = new Random(TestUtils.RANDOM_SEED);
        blobIdentifier = new GSBlobIdentifier("foo", "bar");
        byteValue = (byte) 167;

        Configuration flinkConfig = new Configuration();
        if (temporaryBucketName != null) {
            flinkConfig.set(GSFileSystemOptions.WRITER_TEMPORARY_BUCKET_NAME, temporaryBucketName);
        }

        componentObjectIds = new ArrayList<>();
        for (int i = 0; i < componentObjectCount; i++) {
            componentObjectIds.add(UUID.randomUUID());
        }

        options = new GSFileSystemOptions(flinkConfig);

        blobStorage = new MockBlobStorage();

        if (empty) {
            fsDataOutputStream =
                    new GSRecoverableFsDataOutputStream(blobStorage, options, blobIdentifier);
        } else {
            GSResumeRecoverable resumeRecoverable =
                    new GSResumeRecoverable(blobIdentifier, componentObjectIds, position, closed);
            fsDataOutputStream =
                    new GSRecoverableFsDataOutputStream(blobStorage, options, resumeRecoverable);
        }
    }

    @TestTemplate
    void emptyStreamShouldHaveProperPositionAndComponentObjectCount() {
        if (empty) {
            assertThat(position).isZero();
            assertThat(componentObjectCount).isZero();
        }
    }

    @TestTemplate
    void shouldConstructStream() throws IOException {
        if (empty) {
            assertThat(fsDataOutputStream.getPos()).isEqualTo(0);

        } else {
            assertThat(fsDataOutputStream.getPos()).isEqualTo(position);
        }
    }

    @TestTemplate
    void shouldReturnPosition() throws IOException {
        assertThat(fsDataOutputStream.getPos()).isEqualTo(position);
    }

    private void writeContent(ThrowingRunnable<IOException> write, byte[] expectedContent)
            throws IOException {

        // write the byte, confirm position change and existence of write channel
        assertThat(fsDataOutputStream.getPos()).isEqualTo(position);
        write.run();
        assertThat(fsDataOutputStream.getPos()).isEqualTo(position + expectedContent.length);

        // close and persist. there should be exactly zero blobs before and one after, with this
        // byte value in it
        assertThat(blobStorage.blobs).isEmpty();
        fsDataOutputStream.closeForCommit();
        assertThat(blobStorage.blobs).hasSize(1);
        GSBlobIdentifier blobIdentifier =
                blobStorage.blobs.keySet().toArray(new GSBlobIdentifier[0])[0];
        MockBlobStorage.BlobValue blobValue = blobStorage.blobs.get(blobIdentifier);
        assertThat(blobValue).isNotNull();
        assertThat(blobValue.content).isEqualTo(expectedContent);
    }

    private void writeByte() throws IOException {
        writeContent(() -> fsDataOutputStream.write(byteValue), new byte[] {byteValue});
    }

    @TestTemplate
    void shouldWriteByte() throws IOException {
        if (closed) {
            assertThatThrownBy(this::writeByte).isInstanceOf(IOException.class);
        } else {
            writeByte();
        }
    }

    private void writeArray() throws IOException {
        byte[] bytes = new byte[64];
        random.nextBytes(bytes);
        writeContent(() -> fsDataOutputStream.write(bytes), bytes);
    }

    @TestTemplate
    void shouldWriteArray() throws IOException {
        if (closed) {
            assertThatThrownBy(this::writeArray).isInstanceOf(IOException.class);
        } else {
            writeArray();
        }
    }

    private void writeArraySlice() throws IOException {
        final int start = 4;
        final int length = 10;
        byte[] bytes = new byte[64];
        random.nextBytes(bytes);
        writeContent(
                () -> fsDataOutputStream.write(bytes, start, length),
                Arrays.copyOfRange(bytes, start, start + length));
    }

    @TestTemplate
    void shouldWriteArraySlice() throws IOException {
        if (closed) {
            assertThatThrownBy(this::writeArraySlice).isInstanceOf(IOException.class);
        } else {
            writeArraySlice();
        }
    }

    @TestTemplate
    void shouldFlush() throws IOException {
        if (!closed) {
            fsDataOutputStream.write(byteValue);
            fsDataOutputStream.flush();
        }
    }

    @TestTemplate
    void shouldSync() throws IOException {
        if (!closed) {
            fsDataOutputStream.write(byteValue);
            fsDataOutputStream.sync();
        }
    }

    @TestTemplate
    void shouldPersist() throws IOException {
        if (!closed) {
            GSResumeRecoverable recoverable = (GSResumeRecoverable) fsDataOutputStream.persist();
            assertThat(recoverable.finalBlobIdentifier).isEqualTo(blobIdentifier);
            if (empty) {
                assertThat(recoverable.componentObjectIds).isEmpty();
            } else {
                assertThat(recoverable.componentObjectIds.toArray())
                        .isEqualTo(componentObjectIds.toArray());
            }
            assertThat(recoverable.position).isEqualTo(position);
            assertThat(recoverable.closed).isFalse();
        }
    }

    @TestTemplate
    void shouldFailOnPartialWrite() {
        if (!closed) {
            blobStorage.maxWriteCount = 1;
            byte[] bytes = new byte[2];
            random.nextBytes(bytes);

            assertThatThrownBy(() -> fsDataOutputStream.write(bytes))
                    .isInstanceOf(IOException.class);
        }
    }
}
