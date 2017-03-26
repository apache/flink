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

package org.apache.flink.graph.drivers.parameter;

import org.apache.flink.api.java.utils.ParameterTool;

/**
 * A {@link Parameter} storing a {@link Boolean}.
 */
public class BooleanParameter
extends SimpleParameter<Boolean> {

	/**
	 * Set the parameter name and add this parameter to the list of parameters
	 * stored by owner.
	 *
	 * @param owner the {@link Parameterized} using this {@link Parameter}
	 * @param name the parameter name
	 */
	public BooleanParameter(ParameterizedBase owner, String name) {
		super(owner, name);
	}

	@Override
	public String getUsage() {
		return "[--" + name + "]";
	}

	@Override
	public void configure(ParameterTool parameterTool) {
		value = parameterTool.has(name);
	}

	@Override
	public String toString() {
		return Boolean.toString(value);
	}
}
