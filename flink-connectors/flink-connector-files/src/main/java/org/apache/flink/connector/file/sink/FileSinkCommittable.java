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
import org.apache.flink.streaming.api.functions.sink.filesystem.InProgressFileWriter;

import javax.annotation.Nullable;

import java.io.Serializable;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Wrapper class for both type of committables in {@link FileSink}. One committable might be either
 * one pending files to commit, or one in-progress file to cleanup.
 */
@Internal
public class FileSinkCommittable implements Serializable {

    @Nullable private final InProgressFileWriter.PendingFileRecoverable pendingFile;

    @Nullable private final InProgressFileWriter.InProgressFileRecoverable inProgressFileToCleanup;

    public FileSinkCommittable(InProgressFileWriter.PendingFileRecoverable pendingFile) {
        this.pendingFile = checkNotNull(pendingFile);
        this.inProgressFileToCleanup = null;
    }

    public FileSinkCommittable(
            InProgressFileWriter.InProgressFileRecoverable inProgressFileToCleanup) {
        this.pendingFile = null;
        this.inProgressFileToCleanup = checkNotNull(inProgressFileToCleanup);
    }

    FileSinkCommittable(
            @Nullable InProgressFileWriter.PendingFileRecoverable pendingFile,
            @Nullable InProgressFileWriter.InProgressFileRecoverable inProgressFileToCleanup) {
        this.pendingFile = pendingFile;
        this.inProgressFileToCleanup = inProgressFileToCleanup;
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
}
