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

import org.apache.flink.runtime.state.FunctionInitializationContext;

import java.io.Serializable;

/**
 * The state builder wraps the logic of registering state in user
 * functions, as well as how state is updated per input element..
 */
public abstract class ArtificialStateBuilder<T> implements Serializable {

	private static final long serialVersionUID = -5887676929924485788L;

	final String stateName;

	ArtificialStateBuilder(String stateName) {
		this.stateName = stateName;
	}

	public String getStateName() {
		return stateName;
	}

	/**
	 * Manipulate the state for an input element.
	 *
	 * @param element the current input element.
	 */
	public abstract void artificialStateForElement(T element) throws Exception;

	/**
	 * Registers the state.
	 *
	 * @param initializationContext the state initialization context, provided by the user function.
	 */
	public abstract void initialize(FunctionInitializationContext initializationContext) throws Exception;
}
