/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.tests.artificialstate.builder;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.util.Preconditions;

import java.util.List;

/**
 * An {@link ArtificialListStateBuilder} for user operator and keyed state.
 */
public class ArtificialListStateBuilder<IN, STATE> extends ArtificialStateBuilder<IN> {

	private static final long serialVersionUID = -1205814329756790916L;

	private transient ListState<STATE> listOperatorState;
	private transient ListState<STATE> listKeyedState;
	private final ListStateDescriptor<STATE> listStateDescriptor;
	private final JoinFunction<IN, Iterable<STATE>, List<STATE>> keyedStateGenerator;
	private final JoinFunction<IN, Iterable<STATE>, List<STATE>> operatorStateGenerator;

	public ArtificialListStateBuilder(
		String stateName,
		JoinFunction<IN, Iterable<STATE>, List<STATE>> keyedStateGenerator,
		JoinFunction<IN, Iterable<STATE>, List<STATE>> operatorStateGenerator,
		ListStateDescriptor<STATE> listStateDescriptor) {
		super(stateName);
		this.listStateDescriptor = Preconditions.checkNotNull(listStateDescriptor);
		this.keyedStateGenerator = Preconditions.checkNotNull(keyedStateGenerator);
		this.operatorStateGenerator = Preconditions.checkNotNull(operatorStateGenerator);
	}

	@Override
	public void artificialStateForElement(IN event) throws Exception {
		listOperatorState.update(keyedStateGenerator.join(event, listOperatorState.get()));
		listKeyedState.update(operatorStateGenerator.join(event, listKeyedState.get()));
	}

	@Override
	public void initialize(FunctionInitializationContext initializationContext) throws Exception {
		listOperatorState = initializationContext.getOperatorStateStore().getListState(listStateDescriptor);
		listKeyedState = initializationContext.getKeyedStateStore().getListState(listStateDescriptor);
	}
}
