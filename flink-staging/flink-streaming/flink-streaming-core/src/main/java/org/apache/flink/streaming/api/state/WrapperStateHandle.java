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

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import org.apache.flink.runtime.state.LocalStateHandle;
import org.apache.flink.runtime.state.PartitionedStateHandle;
import org.apache.flink.runtime.state.StateHandle;

/**
 * StateHandle that wraps the StateHandles for the operator states of chained
 * tasks. This is needed so the wrapped handles are properly discarded.
 * 
 */
public class WrapperStateHandle extends LocalStateHandle<Serializable> {

	private static final long serialVersionUID = 1L;

	public WrapperStateHandle(List<Map<String, PartitionedStateHandle>> state) {
		super((Serializable) state);
	}

	@Override
	public void discardState() throws Exception {
		@SuppressWarnings("unchecked")
		List<Map<String, PartitionedStateHandle>> chainedStates = (List<Map<String, PartitionedStateHandle>>) getState();
		for (Map<String, PartitionedStateHandle> stateMap : chainedStates) {
			if(stateMap != null) {
				for (PartitionedStateHandle statePartitions : stateMap.values()) {
					for (StateHandle<Serializable> handle : statePartitions.getState().values()) {
						handle.discardState();
					}
				}
			}
		}
	}

}
