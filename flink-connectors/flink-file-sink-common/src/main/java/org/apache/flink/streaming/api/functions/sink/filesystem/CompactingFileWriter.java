/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.functions.sink.filesystem;

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.api.functions.sink.filesystem.InProgressFileWriter.PendingFileRecoverable;

import java.io.IOException;

/**
 * The file sink compactors use the {@link CompactingFileWriter} to write a compacting file.
 *
 * <p>A class should not directly implement the {@link CompactingFileWriter}, but to implement the
 * {@link RecordWiseCompactingFileWriter}, or the {@link OutputStreamBasedCompactingFileWriter}, or
 * both. If an class implements both interfaces, once the write method of either interface is
 * called, the write method in the other one should be disabled.
 */
@Internal
public interface CompactingFileWriter {

    /**
     * Closes the writer and gets the {@link PendingFileRecoverable} of the written compacting file.
     *
     * @return The state of the pending part file. {@link Bucket} uses this to commit the pending
     *     file.
     * @throws IOException Thrown if an I/O error occurs.
     */
    PendingFileRecoverable closeForCommit() throws IOException;

    /** Enum defining the types of {@link CompactingFileWriter}. */
    @Internal
    enum Type {
        RECORD_WISE,
        OUTPUT_STREAM
    }
}
