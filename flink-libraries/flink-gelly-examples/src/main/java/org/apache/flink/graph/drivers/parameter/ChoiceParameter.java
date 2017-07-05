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
import org.apache.flink.util.Preconditions;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.text.StrBuilder;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * A {@link Parameter} storing a list of {@link String} choices and parsing
 * the user's configured selection.
 */
public class ChoiceParameter
extends SimpleParameter<String> {

	private List<String> choices = new ArrayList<>();

	private List<String> hiddenChoices = new ArrayList<>();

	/**
	 * Set the parameter name and add this parameter to the list of parameters
	 * stored by owner.
	 *
	 * @param owner the {@link Parameterized} using this {@link Parameter}
	 * @param name the parameter name
	 */
	public ChoiceParameter(ParameterizedBase owner, String name) {
		super(owner, name);
	}

	@Override
	public ChoiceParameter setDefaultValue(String defaultValue) {
		super.setDefaultValue(defaultValue);
		choices.add(defaultValue);
		return this;
	}

	/**
	 * Add additional choices. This function can be called multiple times.
	 *
	 * @param choices additional choices
	 * @return this
	 */
	public ChoiceParameter addChoices(String... choices) {
		Collections.addAll(this.choices, choices);
		return this;
	}

	/**
	 * Add additional hidden choices. This function can be called multiple
	 * times. These choices will not be printed in the usage string.
	 *
	 * @param hiddenChoices additional hidden choices
	 * @return this
	 */
	public ChoiceParameter addHiddenChoices(String... hiddenChoices) {
		Collections.addAll(this.hiddenChoices, hiddenChoices);
		return this;
	}

	@Override
	public String getUsage() {
		String option = new StrBuilder()
			.append("--")
			.append(name)
			.append(" <")
			.append(StringUtils.join(choices, " | "))
			.append(">")
			.toString();

		return hasDefaultValue ? "[" + option + "]" : option;
	}

	@Override
	public void configure(ParameterTool parameterTool) {
		Preconditions.checkArgument(choices.size() > 0, "No choices provided");

		String selected = parameterTool.get(name);

		if (selected == null) {
			if (hasDefaultValue) {
				value = defaultValue;
				return;
			} else {
				throw new ProgramParametrizationException(
					"Must select a choice for option '" + name + "': '[" + StringUtils.join(choices, ", ") + "]'");
			}
		}

		for (String choice : choices) {
			if (choice.equals(selected)) {
				this.value = selected;
				return;
			}
		}

		for (String choice : hiddenChoices) {
			if (choice.equals(selected)) {
				this.value = selected;
				return;
			}
		}

		throw new ProgramParametrizationException(
			"Selection '" + selected + "' for option '" + name + "' is not in choices '[" + StringUtils.join(choices, ", ") + "]'");
	}

	@Override
	public String toString() {
		return this.value;
	}
}
