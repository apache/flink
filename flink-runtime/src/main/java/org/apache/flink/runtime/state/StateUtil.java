/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state;

/**
 * Helpers for {@link StateObject} related code.
 */
public class StateUtil {

	private StateUtil() {
		throw new AssertionError();
	}

	/**
	 * Iterates through the passed state handles and calls discardState() on each handle that is not null. All
	 * occurring exceptions are suppressed and collected until the iteration is over and emitted as a single exception.
	 *
	 * @param handlesToDiscard State handles to discard. Passed iterable is allowed to deliver null values.
	 * @throws Exception exception that is a collection of all suppressed exceptions that were caught during iteration
	 */
	public static void bestEffortDiscardAllStateObjects(
			Iterable<? extends StateObject> handlesToDiscard) throws Exception {

		if (handlesToDiscard != null) {

			Exception suppressedExceptions = null;

			for (StateObject state : handlesToDiscard) {

				if (state != null) {
					try {
						state.discardState();
					} catch (Exception ex) {
						//best effort to still cleanup other states and deliver exceptions in the end
						if (suppressedExceptions == null) {
							suppressedExceptions = new Exception(ex);
						}
						suppressedExceptions.addSuppressed(ex);
					}
				}
			}

			if (suppressedExceptions != null) {
				throw suppressedExceptions;
			}
		}
	}
}
