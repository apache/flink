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

import java.io.IOException;
import java.util.Collection;

/**
 * A {@link Sink} with a stateful {@link SinkWriter}.
 *
 * <p>The {@link StatefulSink} needs to be serializable. All configuration should be validated
 * eagerly. The respective sink writers are transient and will only be created in the subtasks on
 * the taskmanagers.
 *
 * @param <InputT> The type of the sink's input
 * @param <WriterStateT> The type of the sink writer's state
 * @deprecated Please implement {@link Sink} and {@link SupportsWriterState} instead.
 */
@PublicEvolving
@Deprecated
public interface StatefulSink<InputT, WriterStateT>
        extends Sink<InputT>, SupportsWriterState<InputT, WriterStateT> {

    /**
     * Create a {@link org.apache.flink.api.connector.sink2.StatefulSinkWriter} from a recovered
     * state.
     *
     * @param context the runtime context.
     * @return A sink writer.
     * @throws IOException for any failure during creation.
     */
    default StatefulSinkWriter<InputT, WriterStateT> restoreWriter(
            Sink.InitContext context, Collection<WriterStateT> recoveredState) throws IOException {
        throw new UnsupportedOperationException(
                "Deprecated, please use restoreWriter(WriterInitContext, Collection<WriterStateT>)");
    }

    /**
     * Create a {@link org.apache.flink.api.connector.sink2.StatefulSinkWriter} from a recovered
     * state.
     *
     * @param context the runtime context.
     * @return A sink writer.
     * @throws IOException for any failure during creation.
     */
    default StatefulSinkWriter<InputT, WriterStateT> restoreWriter(
            WriterInitContext context, Collection<WriterStateT> recoveredState) throws IOException {
        return restoreWriter(new InitContextWrapper(context), recoveredState);
    }

    /**
     * A mix-in for {@link StatefulSink} that allows users to migrate from a sink with a compatible
     * state to this sink.
     */
    @PublicEvolving
    interface WithCompatibleState extends SupportsWriterState.WithCompatibleState {}

    /**
     * A {@link SinkWriter} whose state needs to be checkpointed.
     *
     * @param <InputT> The type of the sink writer's input
     * @param <WriterStateT> The type of the writer's state
     */
    @PublicEvolving
    interface StatefulSinkWriter<InputT, WriterStateT>
            extends org.apache.flink.api.connector.sink2.StatefulSinkWriter<InputT, WriterStateT> {}
}
