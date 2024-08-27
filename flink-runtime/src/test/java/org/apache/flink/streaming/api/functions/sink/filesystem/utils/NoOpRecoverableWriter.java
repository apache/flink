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

package org.apache.flink.streaming.api.functions.sink.filesystem.utils;

import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.RecoverableFsDataOutputStream;
import org.apache.flink.core.fs.RecoverableWriter;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.IOException;

/**
 * A default implementation of the {@link RecoverableWriter} that does nothing.
 *
 * <p>This is to avoid to have to implement all methods for every implementation used in tests.
 */
public class NoOpRecoverableWriter implements RecoverableWriter {

    @Override
    public RecoverableFsDataOutputStream open(Path path) throws IOException {
        return null;
    }

    @Override
    public RecoverableFsDataOutputStream recover(RecoverableWriter.ResumeRecoverable resumable)
            throws IOException {
        return null;
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
    public RecoverableFsDataOutputStream.Committer recoverForCommit(
            RecoverableWriter.CommitRecoverable resumable) throws IOException {
        return null;
    }

    @Override
    public SimpleVersionedSerializer<RecoverableWriter.CommitRecoverable>
            getCommitRecoverableSerializer() {
        return null;
    }

    @Override
    public SimpleVersionedSerializer<RecoverableWriter.ResumeRecoverable>
            getResumeRecoverableSerializer() {
        return null;
    }

    @Override
    public boolean supportsResume() {
        return false;
    }
}
