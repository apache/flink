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
import java.util.List;
import java.util.LinkedList;
import java.util.Map;
import java.util.Objects;

/**
 * Facility to manage required parameters in user defined functions.
 */
public class RequiredParameters {

	private static final String HELP_TEXT_PARAM_DELIMITER = "\t";
	private static final String HELP_TEXT_LINE_DELIMITER = "\n";
	private static final int HELP_TEXT_LENGTH_PER_PARAM = 100;

	private HashMap<String, Option> data;

	public RequiredParameters() {
		this.data = new HashMap<>();
	}

	/**
	 * Add a parameter based on its name.
	 *
	 * @param name - the name of the parameter
	 * @return - an {@link Option} object representing the parameter
	 * @throws RequiredParametersException if an option with the same name is already defined
	 */
	public Option add(String name) throws RequiredParametersException {
		if (!this.data.containsKey(name)) {
			Option option = new Option(name);
			this.data.put(name, option);
			return option;
		} else {
			throw new RequiredParametersException("Option with key " + name + " already exists.");
		}
	}

	/**
	 * Add a parameter encapsulated in an {@link Option} object.
	 *
	 * @param option - the parameter
	 * @throws RequiredParametersException if an option with the same name is already defined
	 */
	public void add(Option option) throws RequiredParametersException {
		if (!this.data.containsKey(option.getName())) {
			this.data.put(option.getName(), option);
		} else {
			throw new RequiredParametersException("Option with key " + option.getName() + " already exists.");
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
	 * @throws RequiredParametersException if any of the specified checks fail
	 */
	public void applyTo(ParameterTool parameterTool) throws RequiredParametersException {
		List<String> missingArguments = new LinkedList<>();
		for (Option o : data.values()) {
			String key = o.getName();
			String shortKey = o.getAlt();

			boolean longKeyUndefined = keyIsUndefined(key, parameterTool.data);
			boolean shortKeyUndefined = keyIsUndefined(shortKey, parameterTool.data);

			// check that the parameterTool does not contain values for both full and short key
			if (parameterTool.data.containsKey(key) && parameterTool.data.containsKey(shortKey)) {
				throw new RequiredParametersException("Value passed for parameter " + key +
						" is ambiguous. Value passed for short and long name.");
			}

			// if the shortKey is not undefined
			if (!shortKeyUndefined && longKeyUndefined) {
				parameterTool.data.put(key, parameterTool.data.get(shortKey));
				parameterTool.data.remove(shortKey);
				// overwrite, as we invalidated the state checked above.
				longKeyUndefined = false;
			}

			// if no value is provided and there is a default value, add it to the parameters.
			if (longKeyUndefined && parameterTool.data.containsKey(key)) {
				if (o.hasDefaultValue()) {
					parameterTool.data.put(key, o.getDefaultValue());
				} else {
					throw new RequiredParametersException("No default value for undefined parameter " + key);
				}
			} else {
				if (!parameterTool.data.containsKey(key)) {
					missingArguments.add(key);
				}
				String value = parameterTool.data.get(key);
				// key is defined and has value, now check if it adheres to the type specified.
				if (o.hasType() && !o.isCastableToDefinedType(value)) {
					throw new RequiredParametersException("Value for parameter " + key +
							" cannot be cast to type " + o.getType());
				}

				// finally check if the value adheres to possibly defined choices.
				if (o.getChoices().size() > 0 && !o.getChoices().contains(value)) {
					throw new RequiredParametersException("Value " + value + " is not in the list of valid choices "
							+ "for key " + o.getName());
				}
			}
		}
		if (!missingArguments.isEmpty()) {
			throw new RequiredParametersException(this.missingArgumentsText(missingArguments), missingArguments);
		}
	}

	/**
	 * Build a help text for the defined parameters.
	 *
	 * The format of the help text will be:
	 * Required Parameters:
	 * \t -:shortName:, --:name: \t :helpText: \t default: :defaultValue: \t choices: :choices: \n
	 *
	 * @return a formatted help String.
	 */
	public String getHelp() {
		StringBuilder sb = new StringBuilder(data.size() * HELP_TEXT_LENGTH_PER_PARAM);

		sb.append("Required Parameters:");
		sb.append(HELP_TEXT_LINE_DELIMITER);

		for (Option o : data.values()) {
			sb.append(this.helpText(o));
		}
		sb.append(HELP_TEXT_LINE_DELIMITER);

		return sb.toString();
	}

	/**
	 * Build a help text for the defined parameters and list the missing arguments at the end of the text.
	 *
	 * The format of the help text will be:
	 * Required Parameters:
	 * \t -:shortName:, --:name: \t :helpText: \t default: :defaultValue: \t choices: :choices: \n
	 *
	 * Missing parameters:
	 * \t param1 param2 ... paramN
	 *
	 * @param missingArguments - a list of missing parameters
	 * @return a formatted help String.
	 */
	public String getHelp(List<String> missingArguments) {
		return this.getHelp() + this.missingArgumentsText(missingArguments);
	}

	/**
	 * for the given option create a line for the help text which looks like:
	 * \t -:shortName:, --:name: \t :helpText: \t default: :defaultValue: \t choices: :choices:
	 */
	private String helpText(Option option) {
		StringBuilder sb = new StringBuilder(HELP_TEXT_LENGTH_PER_PARAM);
		sb.append(HELP_TEXT_PARAM_DELIMITER);

		// if there is a short name, add it.
		if (option.hasAlt()) {
			sb.append("-");
			sb.append(option.getAlt());
			sb.append(", ");
		}

		// add the name
		sb.append("--");
		sb.append(option.getName());
		sb.append(HELP_TEXT_PARAM_DELIMITER);

		// if there is a help text, add it
		if (option.getHelpText() != null) {
			sb.append(option.getHelpText());
			sb.append(HELP_TEXT_PARAM_DELIMITER);
		}

		// if there is a default value, add it.
		if (option.hasDefaultValue()) {
			sb.append("default: ");
			sb.append(option.getDefaultValue());
			sb.append(HELP_TEXT_PARAM_DELIMITER);
		}

		// if there is a list of choices add it.
		if (!option.getChoices().isEmpty()) {
			sb.append("choices: ");
			for (String choice : option.getChoices()) {
				sb.append(choice);
				sb.append(" ");
			}
		}
		sb.append(HELP_TEXT_LINE_DELIMITER);

		return sb.toString();
	}

	private String missingArgumentsText(List<String> missingArguments) {
		StringBuilder sb = new StringBuilder(missingArguments.size() * 10);

		sb.append("Missing arguments for:");
		sb.append(HELP_TEXT_LINE_DELIMITER);

		for (String arg : missingArguments) {
			sb.append(arg);
			sb.append(" ");
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
		return key == null || data.containsKey(key) && Objects.equals(data.get(key), ParameterTool.NO_VALUE_KEY) ||
				data.get(key) == null;
	}
}
