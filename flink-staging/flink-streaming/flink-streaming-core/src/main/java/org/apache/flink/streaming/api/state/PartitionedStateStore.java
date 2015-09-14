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

package org.apache.flink.streaming.api.state;

import java.io.IOException;
import java.io.Serializable;
import java.util.Map;

import org.apache.flink.api.common.state.StateCheckpointer;
import org.apache.flink.runtime.state.StateHandle;

/**
 * Interface for storing and accessing partitioned state. The interface is
 * designed in a way that allows implementations for lazily state access.
 * 
 * @param <S>
 *            Type of the state.
 * @param <C>
 *            Type of the state snapshot.
 */
public interface PartitionedStateStore<S, C extends Serializable> {

	S getStateForKey(Serializable key) throws IOException;

	void setStateForKey(Serializable key, S state);
	
	void removeStateForKey(Serializable key);

	Map<Serializable, S> getPartitionedState() throws IOException;

	StateHandle<Serializable> snapshotStates(long checkpointId, long checkpointTimestamp) throws IOException;

	void restoreStates(StateHandle<Serializable> snapshot, ClassLoader userCodeClassLoader) throws Exception;

	boolean containsKey(Serializable key);
	
	void setCheckPointer(StateCheckpointer<S, C> checkpointer);

}
