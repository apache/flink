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

package org.apache.flink.streaming.api.state;

import java.io.Serializable;

import org.apache.flink.runtime.state.StateHandle;

public class OperatorStateHandle implements StateHandle<Serializable> {
	
	private static final long serialVersionUID = 1L;
	
	private final StateHandle<Serializable> handle;
	private final boolean isPartitioned;
	
	public OperatorStateHandle(StateHandle<Serializable> handle, boolean isPartitioned){
		this.handle = handle;
		this.isPartitioned = isPartitioned;
	}
	
	public boolean isPartitioned(){
		return isPartitioned;
	}

	@Override
	public Serializable getState() throws Exception {
		return handle.getState();
	}

	@Override
	public void discardState() throws Exception {
		handle.discardState();
	}
	
	public StateHandle<Serializable> getHandle() {
		return handle;
	}

}
