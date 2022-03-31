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

package org.apache.flink.api.connector.sink2;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

/**
 * A {@link Sink} with a stateful {@link SinkWriter}.
 *
 * <p>The {@link StatefulSink} needs to be serializable. All configuration should be validated
 * eagerly. The respective sink writers are transient and will only be created in the subtasks on
 * the taskmanagers.
 *
 * @param <InputT> The type of the sink's input
 * @param <WriterStateT> The type of the sink writer's state
 */
@PublicEvolving
public interface StatefulSink<InputT, WriterStateT> extends Sink<InputT> {

    /**
     * Create a {@link StatefulSinkWriter}.
     *
     * @param context the runtime context.
     * @return A sink writer.
     * @throws IOException for any failure during creation.
     */
    StatefulSinkWriter<InputT, WriterStateT> createWriter(InitContext context) throws IOException;

    /**
     * Create a {@link StatefulSinkWriter} from a recovered state.
     *
     * @param context the runtime context.
     * @return A sink writer.
     * @throws IOException for any failure during creation.
     */
    StatefulSinkWriter<InputT, WriterStateT> restoreWriter(
            InitContext context, Collection<WriterStateT> recoveredState) throws IOException;

    /**
     * Any stateful sink needs to provide this state serializer and implement {@link
     * StatefulSinkWriter#snapshotState(long)} properly. The respective state is used in {@link
     * #restoreWriter(InitContext, Collection)} on recovery.
     *
     * @return the serializer of the writer's state type.
     */
    SimpleVersionedSerializer<WriterStateT> getWriterStateSerializer();

    /**
     * A mix-in for {@link StatefulSink} that allows users to migrate from a sink with a compatible
     * state to this sink.
     */
    @PublicEvolving
    interface WithCompatibleState {
        /**
         * A list of state names of sinks from which the state can be restored. For example, the new
         * {@code FileSink} can resume from the state of an old {@code StreamingFileSink} as a
         * drop-in replacement when resuming from a checkpoint/savepoint.
         */
        Collection<String> getCompatibleWriterStateNames();
    }

    /**
     * A {@link SinkWriter} whose state needs to be checkpointed.
     *
     * @param <InputT> The type of the sink writer's input
     * @param <WriterStateT> The type of the writer's state
     */
    @PublicEvolving
    interface StatefulSinkWriter<InputT, WriterStateT> extends SinkWriter<InputT> {
        /**
         * @return The writer's state.
         * @throws IOException if fail to snapshot writer's state.
         */
        List<WriterStateT> snapshotState(long checkpointId) throws IOException;
    }
}
