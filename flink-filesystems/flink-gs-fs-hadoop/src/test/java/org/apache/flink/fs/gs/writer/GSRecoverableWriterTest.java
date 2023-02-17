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
import org.apache.flink.core.fs.Path;
import org.apache.flink.fs.gs.GSFileSystemOptions;
import org.apache.flink.fs.gs.storage.GSBlobIdentifier;
import org.apache.flink.fs.gs.storage.MockBlobStorage;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameter;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Test {@link GSRecoverableWriter}. */
@ExtendWith(ParameterizedTestExtension.class)
public class GSRecoverableWriterTest {

    @Parameter public long position = 16;

    @Parameter(value = 1)
    public boolean closed = false;

    @Parameter(value = 2)
    public int componentCount;

    @Parameters(name = "position={0}, closed={1}, componentCount={2}")
    public static Collection<Object[]> data() {
        return Arrays.asList(
                new Object[][] {
                    // position 0, not closed, component count = 0
                    {0, false, 0},
                    // position 16, not closed, component count = 2
                    {16, false, 2},
                    // position 0, closed, component count = 0
                    {0, true, 0},
                    // position 16, closed, component count = 2
                    {16, true, 2},
                });
    }

    private GSFileSystemOptions options;

    private GSRecoverableWriter writer;

    private List<UUID> componentObjectIds;

    private GSResumeRecoverable resumeRecoverable;

    private GSCommitRecoverable commitRecoverable;

    private GSBlobIdentifier blobIdentifier;

    @BeforeEach
    public void before() {
        MockBlobStorage storage = new MockBlobStorage();
        blobIdentifier = new GSBlobIdentifier("foo", "bar");

        Configuration flinkConfig = new Configuration();
        options = new GSFileSystemOptions(flinkConfig);
        writer = new GSRecoverableWriter(storage, options);

        componentObjectIds = new ArrayList<UUID>();
        for (int i = 0; i < componentCount; i++) {
            componentObjectIds.add(UUID.randomUUID());
        }

        resumeRecoverable =
                new GSResumeRecoverable(blobIdentifier, componentObjectIds, position, closed);
        commitRecoverable = new GSCommitRecoverable(blobIdentifier, componentObjectIds);
    }

    @TestTemplate
    public void testRequiresCleanupOfRecoverableState() {
        assertFalse(writer.requiresCleanupOfRecoverableState());
    }

    @TestTemplate
    public void testSupportsResume() {
        assertTrue(writer.supportsResume());
    }

    @TestTemplate
    public void testOpen() throws IOException {
        Path path = new Path("gs://foo/bar");
        GSRecoverableFsDataOutputStream stream =
                (GSRecoverableFsDataOutputStream) writer.open(path);
        assertNotNull(stream);
    }

    @TestTemplate
    public void testOpenWithEmptyBucketName() throws IOException {
        Path path = new Path("gs:///bar");

        assertThatThrownBy(() -> writer.open(path)).isInstanceOf(IllegalArgumentException.class);
    }

    @TestTemplate
    public void testOpenWithEmptyObjectName() throws IOException {
        Path path = new Path("gs://foo/");

        assertThatThrownBy(() -> writer.open(path)).isInstanceOf(IllegalArgumentException.class);
    }

    @TestTemplate
    public void testOpenWithMissingObjectName() throws IOException {
        Path path = new Path("gs://foo");

        assertThatThrownBy(() -> writer.open(path)).isInstanceOf(IllegalArgumentException.class);
    }

    @TestTemplate
    public void testCleanupRecoverableState() {
        assertTrue(writer.cleanupRecoverableState(resumeRecoverable));
    }

    @TestTemplate
    public void testRecover() throws IOException {
        GSRecoverableFsDataOutputStream stream =
                (GSRecoverableFsDataOutputStream) writer.recover(resumeRecoverable);
        assertEquals(position, stream.getPos());
    }

    @TestTemplate
    public void testRecoverForCommit() {
        GSRecoverableWriterCommitter committer =
                (GSRecoverableWriterCommitter) writer.recoverForCommit(commitRecoverable);
        assertEquals(options, committer.options);
        assertEquals(commitRecoverable, committer.recoverable);
    }

    @TestTemplate
    public void testGetCommitRecoverableSerializer() {
        Object serializer = writer.getCommitRecoverableSerializer();
        assertEquals(GSCommitRecoverableSerializer.class, serializer.getClass());
    }

    @TestTemplate
    public void testGetResumeRecoverableSerializer() {
        Object serializer = writer.getResumeRecoverableSerializer();
        assertEquals(GSResumeRecoverableSerializer.class, serializer.getClass());
    }
}
