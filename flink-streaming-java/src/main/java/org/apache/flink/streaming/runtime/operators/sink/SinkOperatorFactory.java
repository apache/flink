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
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.api.operators.YieldingOperatorFactory;

import java.util.Optional;

/**
 * A {@link org.apache.flink.streaming.api.operators.StreamOperatorFactory} for {@link
 * SinkOperator}.
 *
 * @param <InputT> The input type of the {@link SinkWriter}.
 * @param <CommT> The committable type of the {@link SinkWriter}.
 * @param <WriterStateT> The type of the {@link SinkWriter Writer's} state.
 */
@Internal
public final class SinkOperatorFactory<InputT, CommT, WriterStateT, GlobalCommT>
        extends AbstractStreamOperatorFactory<byte[]>
        implements OneInputStreamOperatorFactory<InputT, byte[]>, YieldingOperatorFactory<byte[]> {

    private final Sink<InputT, CommT, WriterStateT, ?> sink;
    private final CommitterHandler.Factory<Sink<?, CommT, ?, ?>, CommT> committerHandlerFactory;
    private final boolean emitCommittables;

    public SinkOperatorFactory(
            Sink<InputT, CommT, WriterStateT, GlobalCommT> sink,
            CommitterHandler.Factory<Sink<?, CommT, ?, ?>, CommT> committerHandlerFactory,
            boolean emitCommittables) {
        this.sink = sink;
        this.committerHandlerFactory = committerHandlerFactory;
        this.emitCommittables = emitCommittables;
    }

    public <T extends StreamOperator<byte[]>> T createStreamOperator(
            StreamOperatorParameters<byte[]> parameters) {
        try {
            Optional<SimpleVersionedSerializer<WriterStateT>> writerStateSerializer =
                    sink.getWriterStateSerializer();
            SinkWriterStateHandler<WriterStateT> writerStateHandler;
            if (writerStateSerializer.isPresent()) {
                writerStateHandler =
                        new StatefulSinkWriterStateHandler<>(
                                writerStateSerializer.get(), sink.getCompatibleStateNames());
            } else {
                writerStateHandler = StatelessSinkWriterStateHandler.getInstance();
            }

            final SinkOperator<InputT, CommT, WriterStateT> sinkOperator =
                    new SinkOperator<>(
                            processingTimeService,
                            getMailboxExecutor(),
                            sink::createWriter,
                            writerStateHandler,
                            committerHandlerFactory.create(sink),
                            emitCommittables
                                    ? new CheckpointSummary.Serializer<>(
                                            sink.getCommittableSerializer().get())
                                    : null);
            sinkOperator.setup(
                    parameters.getContainingTask(),
                    parameters.getStreamConfig(),
                    parameters.getOutput());
            return (T) sinkOperator;
        } catch (Exception e) {
            throw new IllegalStateException(
                    "Cannot create sink operator for "
                            + parameters.getStreamConfig().getOperatorName(),
                    e);
        }
    }

    @Override
    public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
        return SinkOperator.class;
    }

    @VisibleForTesting
    public Sink<InputT, CommT, WriterStateT, ?> getSink() {
        return sink;
    }
}
