/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.operators.sink;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.Sink.InitContext;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.runtime.state.StateInitializationContext;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** {@link SinkWriterStateHandler} for stateless sinks. */
final class StatelessSinkWriterStateHandler<InputT> implements SinkWriterStateHandler<InputT> {

    private final Sink<InputT> sink;

    StatelessSinkWriterStateHandler(Sink<InputT> sink) {
        this.sink = checkNotNull(sink);
    }

    @Override
    public SinkWriter<InputT> createWriter(
            InitContext initContext, StateInitializationContext context) throws Exception {
        return sink.createWriter(initContext);
    }

    @Override
    public void snapshotState(long checkpointId) throws Exception {
        // no state to snapshot
    }
}
