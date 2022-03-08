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
import org.apache.flink.api.connector.sink2.TwoPhaseCommittingSink;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.connector.sink2.WithPostCommitTopology;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link org.apache.flink.streaming.api.operators.StreamOperatorFactory} for {@link
 * CommitterOperator}.
 *
 * @param <CommT> the type of the committable
 */
@Internal
public final class CommitterOperatorFactory<CommT>
        extends AbstractStreamOperatorFactory<CommittableMessage<CommT>>
        implements OneInputStreamOperatorFactory<
                CommittableMessage<CommT>, CommittableMessage<CommT>> {

    private final TwoPhaseCommittingSink<?, CommT> sink;
    private final boolean isBatchMode;
    private final boolean isCheckpointingEnabled;

    public CommitterOperatorFactory(
            TwoPhaseCommittingSink<?, CommT> sink,
            boolean isBatchMode,
            boolean isCheckpointingEnabled) {
        this.sink = checkNotNull(sink);
        this.isBatchMode = isBatchMode;
        this.isCheckpointingEnabled = isCheckpointingEnabled;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T extends StreamOperator<CommittableMessage<CommT>>> T createStreamOperator(
            StreamOperatorParameters<CommittableMessage<CommT>> parameters) {

        try {
            final CommitterOperator<CommT> committerOperator =
                    new CommitterOperator<>(
                            processingTimeService,
                            sink.getCommittableSerializer(),
                            sink.createCommitter(),
                            sink instanceof WithPostCommitTopology,
                            isBatchMode,
                            isCheckpointingEnabled);
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
