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
 * Encapsulates the usage and configuration of a command-line parameter.
 *
 * @param <T> parameter value type
 */
public interface Parameter<T> {

	/**
	 * An informal usage string. Parameter names are prefixed with "--".
	 *
	 * <p>Optional parameters are enclosed by "[" and "]".
	 *
	 * <p>Generic values are represented by all-caps with specific values enclosed
	 * by "&lt;" and "&gt;".
	 *
	 * @return command-line usage string
	 */
	String getUsage();

	/**
	 * A hidden parameter is parsed from the command-line configuration but is
	 * not printed in the usage string. This can be used for power-user options
	 * not displayed to the general user.
	 *
	 * @return whether this parameter should be hidden from standard usage
	 */
	boolean isHidden();

	/**
	 * Read and parse the parameter value from command-line arguments.
	 *
	 * @param parameterTool parameter parser
	 */
	void configure(ParameterTool parameterTool);

	/**
	 * Get the parameter value.
	 *
	 * @return parameter value
	 */
	T getValue();
}
