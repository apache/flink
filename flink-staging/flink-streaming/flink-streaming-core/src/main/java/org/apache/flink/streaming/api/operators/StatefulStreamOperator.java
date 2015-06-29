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

package org.apache.flink.streaming.api.operators;

import java.io.Serializable;
import java.util.Map;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.PartitionedStateHandle;
import org.apache.flink.runtime.state.StateHandle;

/**
 * Interface for Stream operators that can have state. This interface is used for checkpointing
 * and restoring that state.
 *
 * @param <OUT> The output type of the operator
 */
public interface StatefulStreamOperator<OUT> extends StreamOperator<OUT> {

	void restoreInitialState(Tuple2<StateHandle<Serializable>, Map<String, PartitionedStateHandle>> state) throws Exception;

	Tuple2<StateHandle<Serializable>, Map<String, PartitionedStateHandle>> getStateSnapshotFromFunction(long checkpointId, long timestamp) throws Exception;

	void confirmCheckpointCompleted(long checkpointId, String stateName, StateHandle<Serializable> checkpointedState) throws Exception;
}
