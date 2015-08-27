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
 * Wrapper for storing the handles for each state in a partitioned form. It can
 * be used to repartition the state before re-injecting to the tasks.
 *
 * TODO: This class needs testing!
 */
public class PartitionedStateHandle implements
		StateHandle<Map<Serializable, StateHandle<Serializable>>> {

	private static final long serialVersionUID = 7505365403501402100L;

	Map<Serializable, StateHandle<Serializable>> handles;

	public PartitionedStateHandle(Map<Serializable, StateHandle<Serializable>> handles) {
		this.handles = handles;
	}

	@Override
	public Map<Serializable, StateHandle<Serializable>> getState(ClassLoader userCodeClassLoader) throws Exception {
		return handles;
	}

	@Override
	public void discardState() throws Exception {
		for (StateHandle<Serializable> handle : handles.values()) {
			handle.discardState();
		}
	}

}
