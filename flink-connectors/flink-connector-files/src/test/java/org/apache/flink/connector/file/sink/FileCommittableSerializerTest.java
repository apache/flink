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

package org.apache.flink.connector.file.sink;

import org.apache.flink.connector.file.sink.utils.FileSinkTestUtils;
import org.apache.flink.core.fs.Path;

import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

/** Tests the serialization and deserialization for {@link FileSinkCommittable}. */
public class FileCommittableSerializerTest {

    @ClassRule public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

    @Test
    public void testCommittableWithPendingFile() throws IOException {
        FileSinkCommittable committable =
                new FileSinkCommittable("0", new FileSinkTestUtils.TestPendingFileRecoverable());
        FileSinkCommittable deserialized = serializeAndDeserialize(committable);
        assertEquals(committable.getBucketId(), deserialized.getBucketId());
        assertEquals(committable.getPendingFile(), deserialized.getPendingFile());
        assertEquals(
                committable.getInProgressFileToCleanup(),
                deserialized.getInProgressFileToCleanup());
        assertEquals(
                committable.getCompactedFileToCleanup(), deserialized.getCompactedFileToCleanup());
    }

    @Test
    public void testCommittableWithInProgressFileToCleanup() throws IOException {
        FileSinkCommittable committable =
                new FileSinkCommittable("0", new FileSinkTestUtils.TestInProgressFileRecoverable());
        FileSinkCommittable deserialized = serializeAndDeserialize(committable);
        assertEquals(committable.getBucketId(), deserialized.getBucketId());
        assertEquals(committable.getPendingFile(), deserialized.getPendingFile());
        assertEquals(
                committable.getInProgressFileToCleanup(),
                deserialized.getInProgressFileToCleanup());
        assertEquals(
                committable.getCompactedFileToCleanup(), deserialized.getCompactedFileToCleanup());
    }

    @Test
    public void testCommittableWithCompactedFileToCleanup() throws IOException {
        FileSinkCommittable committable =
                new FileSinkCommittable("0", new Path("/tmp/mock_path_to_cleanup"));
        FileSinkCommittable deserialized = serializeAndDeserialize(committable);
        assertEquals(committable.getBucketId(), deserialized.getBucketId());
        assertEquals(committable.getPendingFile(), deserialized.getPendingFile());
        assertEquals(
                committable.getInProgressFileToCleanup(),
                deserialized.getInProgressFileToCleanup());
        assertEquals(
                committable.getCompactedFileToCleanup(), deserialized.getCompactedFileToCleanup());
    }

    private FileSinkCommittable serializeAndDeserialize(FileSinkCommittable committable)
            throws IOException {
        FileSinkCommittableSerializer serializer =
                new FileSinkCommittableSerializer(
                        new FileSinkTestUtils.SimpleVersionedWrapperSerializer<>(
                                FileSinkTestUtils.TestPendingFileRecoverable::new),
                        new FileSinkTestUtils.SimpleVersionedWrapperSerializer<>(
                                FileSinkTestUtils.TestInProgressFileRecoverable::new));
        byte[] data = serializer.serialize(committable);
        return serializer.deserialize(serializer.getVersion(), data);
    }
}
