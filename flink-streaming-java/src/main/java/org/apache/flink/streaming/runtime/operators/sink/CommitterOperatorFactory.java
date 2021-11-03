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
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * A {@link org.apache.flink.streaming.api.operators.StreamOperatorFactory} for {@link
 * CommitterOperator}.
 *
 * @param <CommT> the type of the committable
 * @param <SinkT> the type of the sink to construct the {@link CommitterHandler}
 */
@Internal
public final class CommitterOperatorFactory<CommT, SinkT extends Sink<?, CommT, ?, ?>>
        extends AbstractStreamOperatorFactory<byte[]>
        implements OneInputStreamOperatorFactory<byte[], byte[]> {

    private final SinkT sink;
    private final CommitterHandler.Factory<? super SinkT, CommT> committerHandlerFactory;
    private final boolean emitDownstream;

    public CommitterOperatorFactory(
            SinkT sink,
            CommitterHandler.Factory<? super SinkT, CommT> committerHandlerFactory,
            boolean emitDownstream) {
        this.sink = checkNotNull(sink);
        this.committerHandlerFactory = checkNotNull(committerHandlerFactory);
        this.emitDownstream = emitDownstream;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T extends StreamOperator<byte[]>> T createStreamOperator(
            StreamOperatorParameters<byte[]> parameters) {

        try {
            CommitterHandler<CommT> committerHandler = committerHandlerFactory.create(sink);
            checkState(
                    !(committerHandler instanceof NoopCommitterHandler),
                    "committer operator without commmitter");
            final CommitterOperator<CommT> committerOperator =
                    new CommitterOperator<>(
                            processingTimeService,
                            new CheckpointSummary.Serializer<>(
                                    sink.getCommittableSerializer().get()),
                            committerHandler,
                            emitDownstream);
            committerOperator.setup(
                    parameters.getContainingTask(),
                    parameters.getStreamConfig(),
                    parameters.getOutput());
            return (T) committerOperator;
        } catch (Exception e) {
            throw new IllegalStateException(
                    "Cannot create commit operator for "
                            + parameters.getStreamConfig().getOperatorName(),
                    e);
        }
    }

    @Override
    @SuppressWarnings("rawtypes")
    public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
        return CommitterOperator.class;
    }
}
