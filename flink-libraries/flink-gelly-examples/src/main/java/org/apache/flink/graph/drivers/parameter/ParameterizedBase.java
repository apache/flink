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
import org.apache.flink.client.program.ProgramParametrizationException;

import org.apache.commons.lang3.text.StrBuilder;

import java.util.ArrayList;
import java.util.List;

/**
 * Base class for a {@link Parameterized} which maintains a list of parameters
 * used to print the command-line usage string and configure parameters.
 */
public abstract class ParameterizedBase
implements Parameterized {

	private List<Parameter<?>> parameters = new ArrayList<>();

	/**
	 * Adds a parameter to the list. Parameter order is preserved when printing
	 * the command-line usage string.
	 *
	 * @param parameter to add to the list of parameters
	 */
	public void addParameter(Parameter<?> parameter) {
		parameters.add(parameter);
	}

	@Override
	public String getName() {
		return this.getClass().getSimpleName();
	}

	@Override
	public String getUsage() {
		StrBuilder strBuilder = new StrBuilder();

		// print parameters as ordered list
		for (Parameter<?> parameter : parameters) {
			if (!parameter.isHidden()) {
				strBuilder
					.append(parameter.getUsage())
					.append(" ");
			}
		}

		return strBuilder.toString();
	}

	@Override
	public void configure(ParameterTool parameterTool) throws ProgramParametrizationException {
		for (Parameter<?> parameter : parameters) {
			parameter.configure(parameterTool);
		}
	}
}
