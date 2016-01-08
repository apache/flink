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

package org.apache.flink.runtime.state;

import java.io.Serializable;
import java.util.Map;

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

	S getStateForKey(Serializable key) throws Exception;

	void setStateForKey(Serializable key, S state);

	Map<Serializable, S> getPartitionedState() throws Exception;

	Map<Serializable, StateHandle<C>> snapshotStates(long checkpointId, long checkpointTimestamp) throws Exception;

	void restoreStates(Map<Serializable, StateHandle<C>> snapshots) throws Exception;

	boolean containsKey(Serializable key);

}
