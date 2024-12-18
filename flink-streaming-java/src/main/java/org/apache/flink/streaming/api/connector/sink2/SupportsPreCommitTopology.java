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

package org.apache.flink.streaming.api.connector.sink2;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.datastream.DataStream;

/**
 * Allows expert users to implement a custom topology after {@link SinkWriter} and before {@link
 * Committer}.
 *
 * <p>It is recommended to use immutable committables because mutating committables can have
 * unexpected side-effects.
 *
 * <p>It's important that all {@link CommittableMessage}s are modified appropriately, such that all
 * messages with the same subtask id will also be processed by the same {@link Committer} subtask
 * and the {@link CommittableSummary} matches the respective count. If committables are combined or
 * split in any way, the summary needs to be adjusted.
 *
 * <p>There is also no requirement to keep the subtask ids of the writer, they can be changed as
 * long as there are no two summaries with the same subtask ids (and corresponding {@link
 * CommittableWithLineage}). Subtask ids don't need to be consecutive or small. The global committer
 * will use {@link CommittableSummary#getNumberOfSubtasks()} to determine if all committables have
 * been received, so that number needs to correctly reflect the number of distinct subtask ids. The
 * easiest way to guarantee all of this is to use {@link RuntimeContext#getTaskInfo()}.
 */
@Experimental
public interface SupportsPreCommitTopology<WriterResultT, CommittableT> {

    /**
     * Intercepts and modifies the committables sent on checkpoint or at end of input. Implementers
     * need to ensure to modify all {@link CommittableMessage}s appropriately.
     *
     * @param committables the stream of committables.
     * @return the custom topology before {@link Committer}.
     */
    DataStream<CommittableMessage<CommittableT>> addPreCommitTopology(
            DataStream<CommittableMessage<WriterResultT>> committables);

    /** Returns the serializer of the WriteResult type. */
    SimpleVersionedSerializer<WriterResultT> getWriteResultSerializer();
}
