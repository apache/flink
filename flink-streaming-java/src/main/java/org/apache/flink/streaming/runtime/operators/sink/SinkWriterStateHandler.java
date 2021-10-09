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

import java.io.Serializable;
import java.util.List;

/**
 * Manages the state of a {@link org.apache.flink.api.connector.sink.SinkWriter}. There are only two
 * flavors: stateless handled by {@link StatelessSinkWriterStateHandler} and stateful handled with
 * {@link StatefulSinkWriterStateHandler}.
 *
 * @param <WriterStateT>
 */
interface SinkWriterStateHandler<WriterStateT> extends Serializable {
    /**
     * Extracts the writer state from the {@link StateInitializationContext}. The state will be used
     * to create the writer.
     */
    List<WriterStateT> initializeState(StateInitializationContext context) throws Exception;

    /**
     * Stores the state of the supplier. The supplier should only be queried once.
     *
     * @param stateExtractor
     * @param checkpointId
     */
    void snapshotState(
            FunctionWithException<Long, List<WriterStateT>, Exception> stateExtractor,
            long checkpointId)
            throws Exception;
}
