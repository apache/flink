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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Facility to manage required parameters in user defined functions.
 *
 * @deprecated These classes will be dropped in the next version. Use {@link ParameterTool} or a third-party
 *             command line parsing library instead.
 */
@Deprecated
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
	 * Check for all required parameters defined:
	 * - has a value been passed
	 *   - if not, does the parameter have an associated default value
	 * - does the type of the parameter match the one defined in RequiredParameters
	 * - does the value provided in the parameterTool adhere to the choices defined in the option.
	 *
	 * <p>If any check fails, a RequiredParametersException is thrown
	 *
	 * @param parameterTool - parameters supplied by the user.
	 * @return the updated ParameterTool containing all the required parameters
	 * @throws RequiredParametersException if any of the specified checks fail
	 */
	public ParameterTool applyTo(ParameterTool parameterTool) throws RequiredParametersException {
		List<String> missingArguments = new LinkedList<>();

		HashMap<String, String> newParameters = new HashMap<>(parameterTool.toMap());

		for (Option o : data.values()) {
			if (newParameters.containsKey(o.getName())) {
				if (Objects.equals(newParameters.get(o.getName()), ParameterTool.NO_VALUE_KEY)) {
					// the parameter has been passed, but no value, check if there is a default value
					checkAndApplyDefaultValue(o, newParameters);
				} else {
					// a value has been passed in the parameterTool, now check if it adheres to all constraints
					checkAmbiguousValues(o, newParameters);
					checkIsCastableToDefinedType(o, newParameters);
					checkChoices(o, newParameters);
				}
			} else {
				// check if there is a default name or a value passed for a possibly defined alternative name.
				if (hasNoDefaultValueAndNoValuePassedOnAlternativeName(o, newParameters)) {
					missingArguments.add(o.getName());
				}
			}
		}
		if (!missingArguments.isEmpty()) {
			throw new RequiredParametersException(this.missingArgumentsText(missingArguments), missingArguments);
		}

		return ParameterTool.fromMap(newParameters);
	}

	// check if the given parameter has a default value and add it to the passed map if that is the case
	// else throw an exception
	private void checkAndApplyDefaultValue(Option o, Map<String, String> data) throws RequiredParametersException {
		if (hasNoDefaultValueAndNoValuePassedOnAlternativeName(o, data)) {
			throw new RequiredParametersException("No default value for undefined parameter " + o.getName());
		}
	}

	// check if the value in the given map which corresponds to the name of the given option
	// is castable to the type of the option (if any is defined)
	private void checkIsCastableToDefinedType(Option o, Map<String, String> data) throws RequiredParametersException {
		if (o.hasType() && !o.isCastableToDefinedType(data.get(o.getName()))) {
			throw new RequiredParametersException("Value for parameter " + o.getName() +
					" cannot be cast to type " + o.getType());
		}
	}

	// check if the value in the given map which corresponds to the name of the given option
	// adheres to the list of given choices for the param in the options (if any are defined)
	private void checkChoices(Option o, Map<String, String> data) throws RequiredParametersException {
		if (o.getChoices().size() > 0 && !o.getChoices().contains(data.get(o.getName()))) {
			throw new RequiredParametersException("Value " + data.get(o.getName()) +
					" is not in the list of valid choices for key " + o.getName());
		}
	}

	// move value passed on alternative name to standard name or apply default value if any defined
	// else return true to indicate parameter is 'really' missing
	private boolean hasNoDefaultValueAndNoValuePassedOnAlternativeName(Option o, Map<String, String> data)
			throws RequiredParametersException {
		if (o.hasAlt() && data.containsKey(o.getAlt())) {
			data.put(o.getName(), data.get(o.getAlt()));
		} else {
			if (o.hasDefaultValue()) {
				data.put(o.getName(), o.getDefaultValue());
				if (o.hasAlt()) {
					data.put(o.getAlt(), o.getDefaultValue());
				}
			} else {
				return true;
			}
		}
		return false;
	}

	// given that the map contains a value for the name of the option passed
	// check if it also contains a value for the shortName in option (if any is defined)
	private void checkAmbiguousValues(Option o, Map<String, String> data) throws RequiredParametersException{
		if (data.containsKey(o.getAlt()) && !Objects.equals(data.get(o.getAlt()), ParameterTool.NO_VALUE_KEY)) {
			throw new RequiredParametersException("Value passed for parameter " + o.getName() +
					" is ambiguous. Value passed for short and long name.");
		}
	}

	/**
	 * Build a help text for the defined parameters.
	 *
	 * <p>The format of the help text will be:
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
	 * <p>The format of the help text will be:
	 * Required Parameters:
	 * \t -:shortName:, --:name: \t :helpText: \t default: :defaultValue: \t choices: :choices: \n
	 *
	 * <p>Missing parameters:
	 * \t param1 param2 ... paramN
	 *
	 * @param missingArguments - a list of missing parameters
	 * @return a formatted help String.
	 */
	public String getHelp(List<String> missingArguments) {
		return this.getHelp() + this.missingArgumentsText(missingArguments);
	}

	/**
	 * for the given option create a line for the help text. The line looks like:
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
}
