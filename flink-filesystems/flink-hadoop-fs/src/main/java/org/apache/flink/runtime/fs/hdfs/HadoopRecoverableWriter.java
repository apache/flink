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

package org.apache.flink.runtime.fs.hdfs;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.RecoverableFsDataOutputStream;
import org.apache.flink.core.fs.RecoverableFsDataOutputStream.Committer;
import org.apache.flink.core.fs.RecoverableWriter;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.runtime.util.HadoopUtils;

import org.apache.hadoop.util.VersionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.UUID;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** An implementation of the {@link RecoverableWriter} for Hadoop's file system abstraction. */
@Internal
public class HadoopRecoverableWriter implements RecoverableWriter {

    private static final Logger LOG = LoggerFactory.getLogger(HadoopRecoverableWriter.class);

    /** The Hadoop file system on which the writer operates. */
    private final org.apache.hadoop.fs.FileSystem fs;

    /**
     * Creates a new Recoverable writer.
     *
     * @param fs The Hadoop file system on which the writer operates.
     */
    public HadoopRecoverableWriter(org.apache.hadoop.fs.FileSystem fs) {
        this.fs = checkNotNull(fs);

        // This writer is only supported on a subset of file systems
        if (!("hdfs".equalsIgnoreCase(fs.getScheme())
                || "viewfs".equalsIgnoreCase(fs.getScheme()))) {
            throw new UnsupportedOperationException(
                    "Recoverable writers on Hadoop are only supported for HDFS");
        }

        // Part of functionality depends on specific versions. We check these schemes and versions
        // eagerly for
        // better error messages.
        if (!HadoopUtils.isMinHadoopVersion(2, 7)) {
            LOG.warn(
                    "WARNING: You are running on hadoop version "
                            + VersionInfo.getVersion()
                            + "."
                            + " If your RollingPolicy does not roll on every checkpoint/savepoint, the StreamingFileSink will throw an exception upon recovery.");
        }
    }

    @Override
    public RecoverableFsDataOutputStream open(Path filePath) throws IOException {
        final org.apache.hadoop.fs.Path targetFile = HadoopFileSystem.toHadoopPath(filePath);
        final org.apache.hadoop.fs.Path tempFile = generateStagingTempFilePath(fs, targetFile);
        return new HadoopRecoverableFsDataOutputStream(fs, targetFile, tempFile);
    }

    @Override
    public RecoverableFsDataOutputStream recover(ResumeRecoverable recoverable) throws IOException {
        if (recoverable instanceof HadoopFsRecoverable) {
            return new HadoopRecoverableFsDataOutputStream(fs, (HadoopFsRecoverable) recoverable);
        } else {
            throw new IllegalArgumentException(
                    "Hadoop File System cannot recover a recoverable for another file system: "
                            + recoverable);
        }
    }

    @Override
    public boolean requiresCleanupOfRecoverableState() {
        return false;
    }

    @Override
    public boolean cleanupRecoverableState(ResumeRecoverable resumable) throws IOException {
        return false;
    }

    @Override
    public Committer recoverForCommit(CommitRecoverable recoverable) throws IOException {
        if (recoverable instanceof HadoopFsRecoverable) {
            return new HadoopRecoverableFsDataOutputStream.HadoopFsCommitter(
                    fs, (HadoopFsRecoverable) recoverable);
        } else {
            throw new IllegalArgumentException(
                    "Hadoop File System  cannot recover a recoverable for another file system: "
                            + recoverable);
        }
    }

    @Override
    public SimpleVersionedSerializer<CommitRecoverable> getCommitRecoverableSerializer() {
        @SuppressWarnings("unchecked")
        SimpleVersionedSerializer<CommitRecoverable> typedSerializer =
                (SimpleVersionedSerializer<CommitRecoverable>)
                        (SimpleVersionedSerializer<?>) HadoopRecoverableSerializer.INSTANCE;

        return typedSerializer;
    }

    @Override
    public SimpleVersionedSerializer<ResumeRecoverable> getResumeRecoverableSerializer() {
        @SuppressWarnings("unchecked")
        SimpleVersionedSerializer<ResumeRecoverable> typedSerializer =
                (SimpleVersionedSerializer<ResumeRecoverable>)
                        (SimpleVersionedSerializer<?>) HadoopRecoverableSerializer.INSTANCE;

        return typedSerializer;
    }

    @Override
    public boolean supportsResume() {
        return true;
    }

    @VisibleForTesting
    static org.apache.hadoop.fs.Path generateStagingTempFilePath(
            org.apache.hadoop.fs.FileSystem fs, org.apache.hadoop.fs.Path targetFile)
            throws IOException {

        checkArgument(targetFile.isAbsolute(), "targetFile must be absolute");

        final org.apache.hadoop.fs.Path parent = targetFile.getParent();
        final String name = targetFile.getName();

        checkArgument(parent != null, "targetFile must not be the root directory");

        while (true) {
            org.apache.hadoop.fs.Path candidate =
                    new org.apache.hadoop.fs.Path(
                            parent, "." + name + ".inprogress." + UUID.randomUUID().toString());
            if (!fs.exists(candidate)) {
                return candidate;
            }
        }
    }
}
