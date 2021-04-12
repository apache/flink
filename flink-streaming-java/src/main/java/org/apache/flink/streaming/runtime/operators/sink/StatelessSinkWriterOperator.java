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
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;

import java.io.IOException;
import java.util.Collections;

/**
 * Runtime {@link org.apache.flink.streaming.api.operators.StreamOperator} for executing {@link
 * SinkWriter Writers} that don't have state.
 *
 * @param <InputT> The input type of the {@link SinkWriter}.
 * @param <CommT> The committable type of the {@link SinkWriter}.
 */
@Internal
final class StatelessSinkWriterOperator<InputT, CommT>
        extends AbstractSinkWriterOperator<InputT, CommT> {

    /** Used to create the stateless {@link SinkWriter}. */
    private final Sink<InputT, CommT, ?, ?> sink;

    StatelessSinkWriterOperator(
            final ProcessingTimeService processingTimeService,
            final Sink<InputT, CommT, ?, ?> sink) {
        super(processingTimeService);
        this.sink = sink;
    }

    @Override
    SinkWriter<InputT, CommT, ?> createWriter() throws IOException {
        return sink.createWriter(createInitContext(), Collections.emptyList());
    }
}
