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

package org.apache.flink.api.java.utils;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Facility to manage required parameters in user defined functions.
 */
public class RequiredParameters {

	private static final String HELP_TEXT_PARAM_DELIMITER = "\t";
	private static final String HELP_TEXT_LINE_DELIMITER = "\n";

	private HashMap<String, Option> data;

	public RequiredParameters() {
		this.data = new HashMap<>();
	}

	public Option add(String name) throws RequiredParametersException {
		if (!this.data.containsKey(name)) {
			Option option = new Option(name);
			this.data.put(name, option);
			return option;
		} else {
			throw new RequiredParametersException("Option with passed key already exists. " +  name);
		}
	}

	public void add(Option option) throws RequiredParametersException {
		if (!this.data.containsKey(option.getName())) {
			this.data.put(option.getName(), option);
		} else {
			throw new RequiredParametersException("Option with passed key already exists. " +  option.getName());
		}
	}

	/**
	 * Check for all parameters in the parameterTool passed to the function:
	 * - has a value been passed
	 *   - if not, does the parameter have an associated default value
	 * - does the type of the parameter match the one defined in RequiredParameters
	 * - does the value provided in the parameterTool adhere to the choices defined in the option
	 *
	 * For any check which is not passed, a RequiredParametersException is thrown
	 *
	 * @param parameterTool - parameters supplied by the user.
	 */
	public void applyTo(ParameterTool parameterTool) throws RequiredParametersException {
		for (Option o : data.values()) {
			String key = o.getName();
			String shortKey = o.getAlt();

			boolean longKeyUndefined = keyIsUndefined(key, parameterTool.data);
			boolean shortKeyUndefined = keyIsUndefined(shortKey, parameterTool.data);

			// check that the parameterTool does not contain values for both full and short key
			if (parameterTool.data.containsKey(key) && parameterTool.data.containsKey(shortKey)) {
				throw new RequiredParametersException("Required parameter " + key +
						" is defined twice: on short and long version.");
			}

			// if the shortKey is not undefined
			if (!shortKeyUndefined && longKeyUndefined) {
				parameterTool.data.put(key, parameterTool.data.get(shortKey));
				parameterTool.data.remove(shortKey);
			}

			// if no value is provided and there is a default value, add it to the parameters.
			if (longKeyUndefined) {
				if (o.hasDefaultValue()) {
					parameterTool.data.put(key, o.getDefaultValue());
				} else {
					throw new RequiredParametersException("No default value for undefined parameter " + key);
				}
			} else {
				if (!parameterTool.data.containsKey(o.getName())) {
					throw new RequiredParametersException("Required parameter " + key + " not present.");
				}
				String value = parameterTool.data.get(key);
				// key is defined and has value, now check if it adheres to the type specified.
				if (o.hasType() && !o.isCastableToDefinedType(value)) {
					throw new RequiredParametersException("Parameter " + key +
							" cannot be cast to the specified type.");
				}

				// finally check if the value adheres to possibly defined choices.
				if (!o.getChoices().contains(value)) {
					throw new RequiredParametersException("Value " + value + " is not in the list of valid choices");
				}
			}
		}
	}

	/**
	 * Build a help text for the defined parameters.
	 *
	 * Formatted like: :name: \t :shortName: \t :helpText: \t :defaultValue: \n
	 *
	 * @return a formatted help String.
	 */
	public String getHelp() {
		StringBuilder sb = new StringBuilder(data.size() * 100);

		sb.append("Required Parameter");
		sb.append(HELP_TEXT_LINE_DELIMITER);
		sb.append("name, short name, help text, default value");

		for (Option o : data.values()) {
			sb.append(o.getName());
			sb.append(HELP_TEXT_PARAM_DELIMITER);
			if (o.hasAlt()) {
				sb.append(o.getAlt());
				sb.append(HELP_TEXT_PARAM_DELIMITER);
			}
			sb.append(o.getHelpText());
			sb.append(HELP_TEXT_PARAM_DELIMITER);
			if (o.hasDefaultValue()) {
				sb.append(o.getDefaultValue());
				sb.append(HELP_TEXT_PARAM_DELIMITER);
			}
			sb.append(HELP_TEXT_LINE_DELIMITER);
		}

		return sb.toString();
	}

	/**
	 * @param key - the key to look up
	 * @param data - the map to look up in
	 *
	 * @return true if the value for key in data is ParameterTool.NO_VALUE_KEY or the key is null
	 */
	private boolean keyIsUndefined(String key, Map<String, String> data) {
		return key == null || data.containsKey(key) && Objects.equals(data.get(key), ParameterTool.NO_VALUE_KEY);
	}
}
