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

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.functions.sink.filesystem.InProgressFileWriter;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Wrapper class for both type of committables in {@link FileSink}. One committable might be either
 * one pending files to commit, or one in-progress file to cleanup.
 */
@Internal
public class FileSinkCommittable implements Serializable {

    private final String bucketId;

    @Nullable private final InProgressFileWriter.PendingFileRecoverable pendingFile;

    @Nullable private final InProgressFileWriter.InProgressFileRecoverable inProgressFileToCleanup;

    @Nullable private final Path compactedFileToCleanup;

    public FileSinkCommittable(
            String bucketId, InProgressFileWriter.PendingFileRecoverable pendingFile) {
        this.bucketId = bucketId;
        this.pendingFile = checkNotNull(pendingFile);
        this.inProgressFileToCleanup = null;
        this.compactedFileToCleanup = null;
    }

    public FileSinkCommittable(
            String bucketId,
            InProgressFileWriter.InProgressFileRecoverable inProgressFileToCleanup) {
        this.bucketId = bucketId;
        this.pendingFile = null;
        this.inProgressFileToCleanup = checkNotNull(inProgressFileToCleanup);
        this.compactedFileToCleanup = null;
    }

    public FileSinkCommittable(String bucketId, Path compactedFileToCleanup) {
        this.bucketId = bucketId;
        this.pendingFile = null;
        this.inProgressFileToCleanup = null;
        this.compactedFileToCleanup = checkNotNull(compactedFileToCleanup);
    }

    FileSinkCommittable(
            String bucketId,
            @Nullable InProgressFileWriter.PendingFileRecoverable pendingFile,
            @Nullable InProgressFileWriter.InProgressFileRecoverable inProgressFileToCleanup,
            @Nullable Path compactedFileToCleanup) {
        this.bucketId = bucketId;
        this.pendingFile = pendingFile;
        this.inProgressFileToCleanup = inProgressFileToCleanup;
        this.compactedFileToCleanup = compactedFileToCleanup;
    }

    public String getBucketId() {
        return bucketId;
    }

    public boolean hasPendingFile() {
        return pendingFile != null;
    }

    @Nullable
    public InProgressFileWriter.PendingFileRecoverable getPendingFile() {
        return pendingFile;
    }

    public boolean hasInProgressFileToCleanup() {
        return inProgressFileToCleanup != null;
    }

    @Nullable
    public InProgressFileWriter.InProgressFileRecoverable getInProgressFileToCleanup() {
        return inProgressFileToCleanup;
    }

    public boolean hasCompactedFileToCleanup() {
        return compactedFileToCleanup != null;
    }

    @Nullable
    public Path getCompactedFileToCleanup() {
        return compactedFileToCleanup;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        FileSinkCommittable that = (FileSinkCommittable) o;
        return Objects.equals(bucketId, that.bucketId)
                && Objects.equals(pendingFile, that.pendingFile)
                && Objects.equals(inProgressFileToCleanup, that.inProgressFileToCleanup)
                && Objects.equals(compactedFileToCleanup, that.compactedFileToCleanup);
    }

    @Override
    public int hashCode() {
        return Objects.hash(bucketId, pendingFile, inProgressFileToCleanup, compactedFileToCleanup);
    }

    @Override
    public String toString() {
        return "FileSinkCommittable{"
                + "bucketId='"
                + bucketId
                + ", pendingFile="
                + pendingFile
                + ", inProgressFileToCleanup="
                + inProgressFileToCleanup
                + ", compactedFileToCleanup="
                + compactedFileToCleanup
                + '}';
    }
}
