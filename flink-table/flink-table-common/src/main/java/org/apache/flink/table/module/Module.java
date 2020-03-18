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

package org.apache.flink.table.module;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.functions.FunctionDefinition;

import java.util.Collections;
import java.util.Optional;
import java.util.Set;

/**
 * Modules define a set of metadata, including functions, user defined types, operators, rules, etc.
 * Metadata from modules are regarded as built-in or system metadata that users can take advantages of.
 */
@PublicEvolving
public interface Module {

	/**
	 * List names of all functions in this module.
	 *
	 * @return a set of function names
	 */
	default Set<String> listFunctions() {
		return Collections.emptySet();
	}

	/**
	 * Get an optional of {@link FunctionDefinition} by a give name.
	 *
	 * @param name name of the {@link FunctionDefinition}.
	 * @return an optional function definition
	 */
	default Optional<FunctionDefinition> getFunctionDefinition(String name) {
		return Optional.empty();
	}

	// user defined types, operators, rules, etc
}
