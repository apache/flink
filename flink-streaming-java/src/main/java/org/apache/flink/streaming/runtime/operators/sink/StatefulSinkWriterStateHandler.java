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

package org.apache.flink.streaming.runtime.operators.sink;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.streaming.api.operators.util.SimpleVersionedListState;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.function.FunctionWithException;

import org.apache.flink.shaded.guava30.com.google.common.collect.Iterables;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/** {@link SinkWriterStateHandler} for stateful sinks. */
@Internal
final class StatefulSinkWriterStateHandler<WriterStateT>
        implements SinkWriterStateHandler<WriterStateT> {

    /** The operator's state descriptor. */
    private static final ListStateDescriptor<byte[]> WRITER_RAW_STATES_DESC =
            new ListStateDescriptor<>("writer_raw_states", BytePrimitiveArraySerializer.INSTANCE);

    /** The writer operator's state serializer. */
    private final SimpleVersionedSerializer<WriterStateT> writerStateSimpleVersionedSerializer;

    /**
     * The previous sink operator's state name. We allow restoring state from a different
     * (compatible) sink implementation such as {@link
     * org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink}. This allows
     * migration to newer Sink implementations.
     */
    private final Collection<String> previousSinkStateNames;

    // ------------------------------- runtime fields ---------------------------------------

    /**
     * The previous sink operator's state. We allow restoring state from a different (compatible)
     * sink implementation such as {@link
     * org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink}. This allows
     * migration to newer Sink implementations.
     */
    private List<ListState<WriterStateT>> previousSinkStates = new ArrayList<>();

    /** The operator's state. */
    private ListState<WriterStateT> writerState;

    public StatefulSinkWriterStateHandler(
            final SimpleVersionedSerializer<WriterStateT> writerStateSimpleVersionedSerializer,
            Collection<String> previousSinkStateNames) {
        this.writerStateSimpleVersionedSerializer = writerStateSimpleVersionedSerializer;
        this.previousSinkStateNames = previousSinkStateNames;
    }

    public List<WriterStateT> initializeState(StateInitializationContext context) throws Exception {
        final ListState<byte[]> rawState =
                context.getOperatorStateStore().getListState(WRITER_RAW_STATES_DESC);
        writerState =
                new SimpleVersionedListState<>(rawState, writerStateSimpleVersionedSerializer);
        final List<WriterStateT> writerStates = CollectionUtil.iterableToList(writerState.get());
        final List<WriterStateT> states = new ArrayList<>(writerStates);

        for (String previousSinkStateName : previousSinkStateNames) {
            final ListStateDescriptor<byte[]> preSinkStateDesc =
                    new ListStateDescriptor<>(
                            previousSinkStateName, BytePrimitiveArraySerializer.INSTANCE);

            final ListState<byte[]> preRawState =
                    context.getOperatorStateStore().getListState(preSinkStateDesc);
            SimpleVersionedListState<WriterStateT> previousSinkState =
                    new SimpleVersionedListState<>(
                            preRawState, writerStateSimpleVersionedSerializer);
            previousSinkStates.add(previousSinkState);
            Iterables.addAll(states, previousSinkState.get());
        }
        return states;
    }

    @Override
    public void snapshotState(
            FunctionWithException<Long, List<WriterStateT>, Exception> stateExtractor,
            long checkpointId)
            throws Exception {
        writerState.update(stateExtractor.apply(checkpointId));
        previousSinkStates.forEach(ListState::clear);
    }
}
