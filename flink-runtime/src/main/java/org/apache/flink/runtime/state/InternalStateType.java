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

/**
 * An enumeration of the types of supported internal states.
 */
public enum InternalStateType {

	KEYED_VALUE(true, StateType.VALUE),
	KEYED_LIST(true, StateType.LIST),
	KEYED_MAP(true, StateType.MAP),
	KEYED_SORTEDMAP(true, StateType.SORTEDMAP),
	SUBKEYED_VALUE(false, StateType.VALUE),
	SUBKEYED_LIST(false, StateType.LIST),
	SUBKEYED_MAP(false, StateType.MAP),
	SUBKEYED_SORTEDMAP(false, StateType.SORTEDMAP);

	private InternalStateType(boolean isKeyedState, StateType stateType){
		this.isKeyedState = isKeyedState;
		this.stateType = stateType;
	}

	public boolean isKeyedState() {
		return isKeyedState;
	}

	private enum StateType {VALUE, LIST, MAP, SORTEDMAP}

	private final boolean isKeyedState;
	private final StateType stateType;
}
