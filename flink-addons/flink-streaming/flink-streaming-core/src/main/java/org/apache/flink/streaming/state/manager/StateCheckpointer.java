/**
 *
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
 *
 */

package org.apache.flink.streaming.state.manager;

import java.util.LinkedList;

import org.apache.flink.streaming.state.TableState;

public class StateCheckpointer {

	@SuppressWarnings("rawtypes")
	private LinkedList<TableState> stateList = new LinkedList<TableState>();

	@SuppressWarnings("rawtypes")
	public void RegisterState(TableState state) {
		stateList.add(state);
	}

	@SuppressWarnings({ "unused", "rawtypes" })
	public void CheckpointStates() {
		for (TableState state : stateList) {
			// take snapshot of every registered state.

		}
	}
}
