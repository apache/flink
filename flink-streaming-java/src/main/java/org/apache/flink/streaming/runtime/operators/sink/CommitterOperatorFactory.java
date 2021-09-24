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
import org.apache.flink.api.connector.sink.Committer;
import org.apache.flink.api.connector.sink.GlobalCommitter;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;

import java.io.IOException;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * A {@link org.apache.flink.streaming.api.operators.StreamOperatorFactory} for {@link
 * CommitterOperator}.
 *
 * @param <CommT> the type of the committable
 * @param <GlobalCommT> the type of the global committable
 */
@Internal
public final class CommitterOperatorFactory<CommT, GlobalCommT>
        extends AbstractStreamOperatorFactory<byte[]>
        implements OneInputStreamOperatorFactory<byte[], byte[]> {

    private final Sink<?, CommT, ?, GlobalCommT> sink;
    private final boolean batch;

    public CommitterOperatorFactory(Sink<?, CommT, ?, GlobalCommT> sink, boolean batch) {
        this.sink = sink;
        this.batch = batch;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T extends StreamOperator<byte[]>> T createStreamOperator(
            StreamOperatorParameters<byte[]> parameters) {

        SimpleVersionedSerializer<CommT> committableSerializer =
                sink.getCommittableSerializer().orElseThrow(this::noSerializerFound);
        try {
            CommitterHandler<CommT, GlobalCommT> committerHandler = getGlobalCommitterHandler();
            if (batch) {
                Optional<Committer<CommT>> committer = sink.createCommitter();
                if (committer.isPresent()) {
                    committerHandler =
                            new BatchCommitterHandler<>(committer.get(), committerHandler);
                }
            }

            checkState(
                    !(committerHandler instanceof NoopCommitterHandler),
                    "committer operator without commmitter");
            final CommitterOperator<CommT, GlobalCommT> committerOperator =
                    new CommitterOperator<>(
                            processingTimeService, committableSerializer, committerHandler);
            committerOperator.setup(
                    parameters.getContainingTask(),
                    parameters.getStreamConfig(),
                    parameters.getOutput());
            return (T) committerOperator;
        } catch (Exception e) {
            throw new IllegalStateException("Cannot create commit operator of " + sink, e);
        }
    }

    private IllegalStateException noSerializerFound() {
        return new IllegalStateException(
                sink.getClass()
                        + " does not implement getCommittableSerializer which is needed for any (global) committer.");
    }

    private CommitterHandler<CommT, GlobalCommT> getGlobalCommitterHandler() throws IOException {
        Optional<GlobalCommitter<CommT, GlobalCommT>> globalCommitter =
                sink.createGlobalCommitter();
        if (!globalCommitter.isPresent()) {
            return NoopCommitterHandler.getInstance();
        }
        if (batch) {
            return new GlobalBatchCommitterHandler<>(globalCommitter.get());
        }
        SimpleVersionedSerializer<GlobalCommT> serializer =
                sink.getGlobalCommittableSerializer().orElseThrow(this::noGlobalSerializerFound);
        return new GlobalStreamingCommitterHandler<>(globalCommitter.get(), serializer);
    }

    private IllegalStateException noGlobalSerializerFound() {
        return new IllegalStateException(
                sink.getClass()
                        + " does not implement getGlobalCommittableSerializer which is needed for streaming global committers.");
    }

    @Override
    @SuppressWarnings("rawtypes")
    public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
        return CommitterOperator.class;
    }
}
