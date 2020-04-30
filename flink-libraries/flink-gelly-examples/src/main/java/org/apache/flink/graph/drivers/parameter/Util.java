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

import org.apache.flink.client.program.ProgramParametrizationException;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

/**
 * Utility methods for parsing command-line arguments.
 */
public class Util {

	private Util() {}

	// ------------------------------------------------------------------------
	//  Boolean Condition Checking (Argument)
	// ------------------------------------------------------------------------

	/**
	 * Checks the given boolean condition, and throws an {@code ProgramParametrizationException} if
	 * the condition is not met (evaluates to {@code false}). The exception will have the
	 * given error message.
	 *
	 * @param condition The condition to check
	 * @param errorMessage The message for the {@code ProgramParametrizationException} that is thrown if the check fails.
	 *
	 * @throws ProgramParametrizationException Thrown, if the condition is violated.
	 *
	 * @see Preconditions#checkNotNull(Object, String)
	 */
	public static void checkParameter(boolean condition, @Nullable Object errorMessage) {
		if (!condition) {
			throw new ProgramParametrizationException(String.valueOf(errorMessage));
		}
	}
}
