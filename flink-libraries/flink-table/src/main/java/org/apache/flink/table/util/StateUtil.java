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

package org.apache.flink.table.util;

import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;

/**
 * An Utility about state.
 */
public class StateUtil {

	private StateUtil() {
		// Deprecate default constructor
	}

	public static final String STATE_BACKEND_ON_HEAP = "statebackend.onheap";

	/**
	 * Message to indicate the state is cleared because of ttl restriction. The messgae could be
	 * used to output to log.
	 */
	public static final String STATE_CLEARED_WARN_MSG = "The state is cleared because of state ttl. " +
														"This will result in incorrect result. " +
														"You can increase the state ttl to avoid this.";

	/**
	 * Returns true when the statebackend is on heap.
	 */
	public static boolean isHeapState(StateBackend stateBackend) {
		return stateBackend == null ||
				stateBackend instanceof MemoryStateBackend ||
				stateBackend instanceof FsStateBackend;
	}
}
