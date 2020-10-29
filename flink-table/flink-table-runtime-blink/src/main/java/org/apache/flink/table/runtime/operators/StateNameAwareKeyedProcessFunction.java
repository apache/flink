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

package org.apache.flink.table.runtime.operators;

import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.table.runtime.operators.multipleinput.StreamMultipleInputStreamOperator;

/**
 * Base class for all keyed process functions which operator can be merge into {@link StreamMultipleInputStreamOperator}.
 * The sub-classes can get unique state name through getStateNameContext().getUniqueStateName(name);
 */
public abstract class StateNameAwareKeyedProcessFunction<K, IN, OUT>
		extends KeyedProcessFunction<K, IN, OUT>
		implements StateNameAware {
	private static final long serialVersionUID = 1;

	// this instance should never be null,
	// because this operator may not be merged into multiple input operator.
	private StateNameContext stateNameContext = new StateNameContext();

	@Override
	public StateNameContext getStateNameContext() {
		return stateNameContext;
	}

	@Override
	public void setStateNameContext(StateNameContext context) {
		this.stateNameContext = context;
	}
}
