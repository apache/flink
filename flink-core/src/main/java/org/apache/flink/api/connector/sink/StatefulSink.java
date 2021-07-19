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
 *
 */

package org.apache.flink.api.connector.sink;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * This interface lets the sink developer build a simple sink topology, which could guarantee the
 * exactly once semantics in both batch and stream execution mode if there is a {@link Committer} or
 * {@link GlobalCommitter}. 1. The {@link SinkWriter} is responsible for producing the committable.
 * 2. The {@link Committer} is responsible for committing a single committable. 3. The {@link
 * GlobalCommitter} is responsible for committing an aggregated committable, which we call the
 * global committable. The {@link GlobalCommitter} is always executed with a parallelism of 1. Note:
 * Developers need to ensure the idempotence of {@link Committer} and {@link GlobalCommitter}.
 *
 * @param <InputT> The type of the sink's input
 * @param <CommT> The type of information needed to commit data staged by the sink
 * @param <WriterStateT> The type of the sink writer's state
 * @param <GlobalCommT> The type of the aggregated committable
 */
@Experimental
public interface StatefulSink<InputT, WriterStateT> extends Sink<InputT> {
    /**
     * Create a {@link SinkWriter}.
     *
     * @param context the runtime context.
     * @param states the writer's state.
     * @return A sink writer.
     * @throws IOException if fail to create a writer.
     */
    StatefulSinkWriter<InputT, WriterStateT> createWriter(
            InitContext context, List<WriterStateT> states) throws IOException;

    @Override
    default SinkWriter<InputT> createWriter(InitContext context) throws IOException {
        return createWriter(context, Collections.emptyList());
    }

    /** Return the serializer of the writer's state type. */
    SimpleVersionedSerializer<WriterStateT> getWriterStateSerializer();
}
