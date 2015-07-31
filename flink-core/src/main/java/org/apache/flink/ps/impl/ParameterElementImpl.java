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
package org.apache.flink.ps.impl;

import org.apache.flink.ps.model.ParameterElement;

/**
 * An implemention of parameter elements stored in a {@link org.apache.flink.ps.model.ParameterServer}
 * @param <T> the type of the parameter
 */
public class ParameterElementImpl<T> implements ParameterElement<T>{

	private int clock;
	private T value;

	public ParameterElementImpl(int clock, T value) {
		this.clock = clock;
		this.value = value;
	}

	@Override
	public int getClock() {
		return clock;
	}

	@Override
	public T getValue() {
		return value;
	}
}
