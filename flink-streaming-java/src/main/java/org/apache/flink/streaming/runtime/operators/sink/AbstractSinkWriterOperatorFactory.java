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

import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;

/**
 * Base {@link OneInputStreamOperatorFactory} for subclasses of {@link AbstractSinkWriterOperator}.
 *
 * @param <InputT> The input type of the {@link SinkWriter}.
 * @param <CommT> The committable type of the {@link SinkWriter}.
 */
abstract class AbstractSinkWriterOperatorFactory<InputT, CommT>
        extends AbstractStreamOperatorFactory<CommT>
        implements OneInputStreamOperatorFactory<InputT, CommT> {

    @Override
    @SuppressWarnings("unchecked")
    public <T extends StreamOperator<CommT>> T createStreamOperator(
            StreamOperatorParameters<CommT> parameters) {
        final AbstractSinkWriterOperator<InputT, CommT> writerOperator =
                createWriterOperator(this.processingTimeService);
        writerOperator.setup(
                parameters.getContainingTask(),
                parameters.getStreamConfig(),
                parameters.getOutput());
        return (T) writerOperator;
    }

    abstract AbstractSinkWriterOperator<InputT, CommT> createWriterOperator(
            ProcessingTimeService processingTimeService);
}
