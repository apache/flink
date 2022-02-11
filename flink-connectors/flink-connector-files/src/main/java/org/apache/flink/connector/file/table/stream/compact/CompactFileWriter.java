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

package org.apache.flink.connector.file.table.stream.compact;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.file.table.stream.AbstractStreamingWriter;
import org.apache.flink.connector.file.table.stream.compact.CompactMessages.CoordinatorInput;
import org.apache.flink.connector.file.table.stream.compact.CompactMessages.EndCheckpoint;
import org.apache.flink.connector.file.table.stream.compact.CompactMessages.InputFile;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/** Writer for emitting {@link InputFile} and {@link EndCheckpoint} to downstream. */
@Internal
public class CompactFileWriter<T> extends AbstractStreamingWriter<T, CoordinatorInput> {

    private static final long serialVersionUID = 1L;

    public CompactFileWriter(
            long bucketCheckInterval,
            StreamingFileSink.BucketsBuilder<
                            T, String, ? extends StreamingFileSink.BucketsBuilder<T, String, ?>>
                    bucketsBuilder) {
        super(bucketCheckInterval, bucketsBuilder);
    }

    @Override
    protected void partitionCreated(String partition) {}

    @Override
    protected void partitionInactive(String partition) {}

    @Override
    protected void onPartFileOpened(String partition, Path newPath) {
        output.collect(new StreamRecord<>(new InputFile(partition, newPath)));
    }

    @Override
    protected void commitUpToCheckpoint(long checkpointId) throws Exception {
        super.commitUpToCheckpoint(checkpointId);
        output.collect(
                new StreamRecord<>(
                        new EndCheckpoint(
                                checkpointId,
                                getRuntimeContext().getIndexOfThisSubtask(),
                                getRuntimeContext().getNumberOfParallelSubtasks())));
    }
}
