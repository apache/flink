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

package org.apache.flink.cep.nfa.compiler;

import org.apache.flink.cep.pattern.MalformedPatternException;
import org.apache.flink.util.Preconditions;

import java.util.HashSet;
import java.util.Set;

/**
 * A utility class used to handle name conventions and guarantee unique
 * names for the states of our {@link org.apache.flink.cep.nfa.NFA}.
 */
public class NFAStateNameHandler {

	private static final String STATE_NAME_DELIM = ":";

	private final Set<String> usedNames = new HashSet<>();

	/**
	 * Implements the reverse process of the {@link #getUniqueInternalName(String)}.
	 *
	 * @param internalName The name to be decoded.
	 * @return The original, user-specified name for the state.
	 */
	public static String getOriginalNameFromInternal(String internalName) {
		Preconditions.checkNotNull(internalName);
		return internalName.split(STATE_NAME_DELIM)[0];
	}

	/**
	 * Checks if the given name is already used or not. If yes, it
	 * throws a {@link MalformedPatternException}.
	 *
	 * @param name The name to be checked.
	 */
	public void checkNameUniqueness(String name) {
		if (usedNames.contains(name)) {
			throw new MalformedPatternException("Duplicate pattern name: " + name + ". Names must be unique.");
		}
		usedNames.add(name);
	}

	/**
	 * Clear the names added during checking name uniqueness.
	 */
	public void clear() {
		usedNames.clear();
	}

	/**
	 * Used to give a unique name to {@link org.apache.flink.cep.nfa.NFA} states
	 * created during the translation process. The name format will be
	 * {@code baseName:counter} , where the counter is increasing for states with
	 * the same {@code baseName}.
	 *
	 * @param baseName The base of the name.
	 * @return The (unique) name that is going to be used internally for the state.
	 */
	public String getUniqueInternalName(String baseName) {
		int counter = 0;
		String candidate = baseName;
		while (usedNames.contains(candidate)) {
			candidate = baseName + STATE_NAME_DELIM + counter++;
		}
		usedNames.add(candidate);
		return candidate;
	}

}
