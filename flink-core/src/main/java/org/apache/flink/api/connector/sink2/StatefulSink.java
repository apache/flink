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

/**
 * A {@link Sink} with a stateful {@link SinkWriter}.
 *
 * <p>This interface is retained as a deprecated bridge for connectors compiled against Flink 1.x
 * that reference this type. It was originally removed in Flink 2.0 (FLINK-36245) but restored to
 * ease migration, as the connector ecosystem had not fully migrated to the mixin pattern introduced
 * in FLIP-372.
 *
 * <p>New connector implementations should directly implement {@link Sink} and {@link
 * SupportsWriterState} instead.
 *
 * @param <InputT> The type of the sink's input
 * @param <WriterStateT> The type of the sink writer's state
 * @deprecated Implement {@link Sink} and {@link SupportsWriterState} instead. This interface exists
 *     solely as a binary compatibility bridge for connectors compiled against Flink 1.x and will be
 *     removed in a future release.
 */
@PublicEvolving
@Deprecated
public interface StatefulSink<InputT, WriterStateT>
        extends Sink<InputT>, SupportsWriterState<InputT, WriterStateT> {

    /**
     * A mix-in for {@link StatefulSink} that allows users to migrate from a sink with a compatible
     * state to this sink.
     *
     * @deprecated Use {@link SupportsWriterState.WithCompatibleState} instead.
     */
    @PublicEvolving
    @Deprecated
    interface WithCompatibleState extends SupportsWriterState.WithCompatibleState {}

    /**
     * A {@link SinkWriter} whose state needs to be checkpointed.
     *
     * @param <InputT> The type of the sink writer's input
     * @param <WriterStateT> The type of the writer's state
     * @deprecated Use {@link org.apache.flink.api.connector.sink2.StatefulSinkWriter} directly.
     */
    @PublicEvolving
    @Deprecated
    interface StatefulSinkWriter<InputT, WriterStateT>
            extends org.apache.flink.api.connector.sink2.StatefulSinkWriter<InputT, WriterStateT> {}
}
