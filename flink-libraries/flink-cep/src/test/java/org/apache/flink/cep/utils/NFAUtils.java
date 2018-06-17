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

package org.apache.flink.cep.utils;

import org.apache.flink.cep.nfa.NFA;
import org.apache.flink.cep.nfa.compiler.NFACompiler;
import org.apache.flink.cep.pattern.Pattern;

import static org.apache.flink.cep.nfa.compiler.NFACompiler.compileFactory;

/**
 * Utility methods for constructing NFA.
 */
public class NFAUtils {

	/**
	 * Compiles the given pattern into a {@link NFA}.
	 *
	 * @param pattern         Definition of sequence pattern
	 * @param timeoutHandling True if the NFA shall return timed out event patterns
	 * @param <T>             Type of the input events
	 * @return Non-deterministic finite automaton representing the given pattern
	 */
	public static <T> NFA<T> compile(Pattern<T, ?> pattern, boolean timeoutHandling) {
		NFACompiler.NFAFactory<T> factory = compileFactory(pattern, timeoutHandling);
		return factory.createNFA();
	}

	private NFAUtils() {
	}
}
