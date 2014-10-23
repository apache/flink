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

package org.apache.flink.streaming.state;

import org.apache.flink.streaming.state.checkpoint.StateCheckpoint;

/**
 * Basic {@link OperatorState} for storing and updating simple objects. By default the
 * whole stored object is checkpointed at each backup. Override checkpoint to
 * allow a more fine-grained behavior.
 *
 * @param <T>
 *            The type of the stored state object.
 */
public class SimpleState<T> extends OperatorState<T> {

	private static final long serialVersionUID = 1L;

	public SimpleState() {
		super();
	}

	public SimpleState(T initialState) {
		super(initialState);
	}

	@Override
	public StateCheckpoint<T> checkpoint() {
		return new StateCheckpoint<T>(this);
	}

	@Override
	public OperatorState<T> restore(StateCheckpoint<T> checkpoint) {
		this.state = checkpoint.getCheckpointedState();
		return this;
	}

}
