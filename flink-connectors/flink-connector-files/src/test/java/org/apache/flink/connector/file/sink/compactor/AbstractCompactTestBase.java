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

package org.apache.flink.connector.file.sink.compactor;

import org.apache.flink.connector.file.sink.FileSinkCommittable;
import org.apache.flink.connector.file.sink.FileSinkCommittableSerializer;
import org.apache.flink.connector.file.sink.compactor.operator.CompactorRequest;
import org.apache.flink.connector.file.sink.utils.FileSinkTestUtils;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** Test base for compact operators. */
public abstract class AbstractCompactTestBase {

    public static final int TARGET_SIZE = 9;

    protected Path folder;

    @BeforeEach
    protected void before(@TempDir java.nio.file.Path tmpDir) {
        folder = new Path(tmpDir.toString());
    }

    protected StreamRecord<CompactorRequest> request(
            String bucketId,
            List<FileSinkCommittable> toCompact,
            List<FileSinkCommittable> toPassthrough) {
        return new StreamRecord<>(rawRequest(bucketId, toCompact, toPassthrough), 0L);
    }

    protected FileSinkCommittable committable(String bucketId, String name, int size)
            throws IOException {
        // put bucketId after name to keep the possible '.' prefix in name
        return new FileSinkCommittable(
                bucketId,
                new FileSinkTestUtils.TestPendingFileRecoverable(
                        newFile(name + "_" + bucketId, size <= 0 ? 1 : size), size));
    }

    protected FileSinkCommittable cleanupInprogress(String bucketId, String name, int size)
            throws IOException {
        Path toCleanup = newFile(name + "_" + bucketId, size);
        return new FileSinkCommittable(
                bucketId, new FileSinkTestUtils.TestInProgressFileRecoverable(toCleanup, size));
    }

    protected SimpleVersionedSerializer<FileSinkCommittable> getTestCommittableSerializer() {
        return new FileSinkCommittableSerializer(
                new FileSinkTestUtils.SimpleVersionedWrapperSerializer<>(
                        FileSinkTestUtils.TestPendingFileRecoverable::new),
                new FileSinkTestUtils.SimpleVersionedWrapperSerializer<>(
                        FileSinkTestUtils.TestInProgressFileRecoverable::new));
    }

    protected CompactorRequest rawRequest(
            String bucketId,
            List<FileSinkCommittable> toCompact,
            List<FileSinkCommittable> toPassthrough) {
        return new CompactorRequest(
                bucketId,
                toCompact == null ? new ArrayList<>() : toCompact,
                toPassthrough == null ? new ArrayList<>() : toPassthrough);
    }

    protected Path newFile(String name, int len) throws IOException {
        Path path = new Path(folder, name);
        File file = new File(path.getPath());
        file.delete();
        file.createNewFile();

        try (FileOutputStream out = new FileOutputStream(file)) {
            for (int i = 0; i < len; i++) {
                out.write(i);
            }
        }
        return path;
    }
}
