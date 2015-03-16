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

import org.apache.flink.util.InstantiationUtil;

import java.io.IOException;
import java.util.Map;

/**
 * A StateHandle that includes a copy of the state itself. This state handle is recommended for 
 * cases where the operatorState is lightweight enough to pass throughout the network.
 *
 * State is kept in a byte[] because it may contain userclasses, which akka is not able to handle.
 */
public class LocalStateHandle implements StateHandle{
	
	transient private Map<String, OperatorState<?>> stateMap;
	private final byte[] state;

	public LocalStateHandle(Map<String,OperatorState<?>> state) throws IOException {
		this.stateMap = state;
		this.state = InstantiationUtil.serializeObject(state);
	}

	@Override
	public Map<String,OperatorState<?>> getState(ClassLoader usercodeClassloader) {
		if(stateMap == null) {
			try {
				stateMap = (Map<String, OperatorState<?>>) InstantiationUtil.deserializeObject(this.state, usercodeClassloader);
			} catch (Exception e) {
				throw new RuntimeException("Error while deserializing the state", e);
			}
		}
		return stateMap;
	}
}
