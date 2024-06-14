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

package org.apache.flink.connector.file.sink.utils;

import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketWriter;
import org.apache.flink.streaming.api.functions.sink.filesystem.CompactingFileWriter;
import org.apache.flink.streaming.api.functions.sink.filesystem.InProgressFileWriter;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputStreamBasedCompactingFileWriter;
import org.apache.flink.streaming.api.functions.sink.filesystem.WriterProperties;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;

/** A dummy {@link BucketWriter} for test purposes. */
public class TestBucketWriter implements BucketWriter<Integer, String> {

    @Override
    public InProgressFileWriter<Integer, String> openNewInProgressFile(
            String bucketId, Path path, long creationTime) throws IOException {
        return new InProgressFileWriter<Integer, String>() {
            BufferedWriter writer;
            long size = 0L;

            @Override
            public void write(Integer element, long currentTime) throws IOException {
                if (writer == null) {
                    writer = new BufferedWriter(new FileWriter(path.toString()));
                }
                writer.write(element);
                size += 1;
            }

            @Override
            public InProgressFileRecoverable persist() throws IOException {
                return new FileSinkTestUtils.TestInProgressFileRecoverable(path, size);
            }

            @Override
            public PendingFileRecoverable closeForCommit() throws IOException {
                return new FileSinkTestUtils.TestPendingFileRecoverable(path, size);
            }

            @Override
            public void dispose() {}

            @Override
            public String getBucketId() {
                return bucketId;
            }

            @Override
            public long getCreationTime() {
                return 0;
            }

            @Override
            public long getSize() throws IOException {
                return size;
            }

            @Override
            public long getLastUpdateTime() {
                return 0;
            }
        };
    }

    @Override
    public InProgressFileWriter<Integer, String> resumeInProgressFileFrom(
            String s,
            InProgressFileWriter.InProgressFileRecoverable inProgressFileSnapshot,
            long creationTime)
            throws IOException {
        return null;
    }

    @Override
    public WriterProperties getProperties() {
        return null;
    }

    @Override
    public PendingFile recoverPendingFile(
            InProgressFileWriter.PendingFileRecoverable pendingFileRecoverable) throws IOException {
        return new PendingFile() {
            @Override
            public void commit() throws IOException {
                FileSinkTestUtils.TestPendingFileRecoverable testRecoverable =
                        (FileSinkTestUtils.TestPendingFileRecoverable) pendingFileRecoverable;
                if (testRecoverable.getPath() != null) {
                    if (!testRecoverable.getPath().equals(testRecoverable.getUncommittedPath())) {
                        testRecoverable
                                .getPath()
                                .getFileSystem()
                                .rename(
                                        testRecoverable.getUncommittedPath(),
                                        testRecoverable.getPath());
                    }
                }
            }

            @Override
            public void commitAfterRecovery() throws IOException {
                commit();
            }
        };
    }

    @Override
    public boolean cleanupInProgressFileRecoverable(
            InProgressFileWriter.InProgressFileRecoverable inProgressFileRecoverable)
            throws IOException {
        return false;
    }

    @Override
    public CompactingFileWriter openNewCompactingFile(
            CompactingFileWriter.Type type, String bucketId, Path path, long creationTime)
            throws IOException {
        if (type == CompactingFileWriter.Type.RECORD_WISE) {
            return openNewInProgressFile(bucketId, path, creationTime);
        } else {
            FileOutputStream fileOutputStream = new FileOutputStream(path.toString());
            return new OutputStreamBasedCompactingFileWriter() {

                @Override
                public OutputStream asOutputStream() throws IOException {
                    return fileOutputStream;
                }

                @Override
                public InProgressFileWriter.PendingFileRecoverable closeForCommit()
                        throws IOException {
                    fileOutputStream.flush();
                    return new FileSinkTestUtils.TestPendingFileRecoverable(
                            path, fileOutputStream.getChannel().position());
                }
            };
        }
    }
}
