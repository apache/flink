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

import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.util.function.FunctionWithException;

import java.util.Collections;
import java.util.List;

/** {@link SinkWriterStateHandler} for stateless sinks. */
enum StatelessSinkWriterStateHandler implements SinkWriterStateHandler<Object> {
    INSTANCE;

    @SuppressWarnings("unchecked")
    static <WriterStateT> SinkWriterStateHandler<WriterStateT> getInstance() {
        return (SinkWriterStateHandler<WriterStateT>) StatelessSinkWriterStateHandler.INSTANCE;
    }

    @Override
    public List<Object> initializeState(StateInitializationContext context) throws Exception {
        return Collections.emptyList();
    }

    @Override
    public void snapshotState(
            FunctionWithException<Long, List<Object>, Exception> stateExtractor, long checkpointId)
            throws Exception {}
}
