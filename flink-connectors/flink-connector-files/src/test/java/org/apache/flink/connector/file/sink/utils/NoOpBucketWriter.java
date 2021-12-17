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
import org.apache.flink.streaming.api.functions.sink.filesystem.InProgressFileWriter;
import org.apache.flink.streaming.api.functions.sink.filesystem.WriterProperties;

import java.io.IOException;

/** An empty bucket writer that does nothing. */
public class NoOpBucketWriter implements BucketWriter<String, String> {
    @Override
    public InProgressFileWriter<String, String> openNewInProgressFile(
            String s, Path path, long creationTime) throws IOException {
        return null;
    }

    @Override
    public InProgressFileWriter<String, String> resumeInProgressFileFrom(
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
        return null;
    }

    @Override
    public boolean cleanupInProgressFileRecoverable(
            InProgressFileWriter.InProgressFileRecoverable inProgressFileRecoverable)
            throws IOException {
        return false;
    }
}
