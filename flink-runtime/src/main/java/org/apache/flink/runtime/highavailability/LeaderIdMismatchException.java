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

package org.apache.flink.runtime.highavailability;

import java.util.UUID;

/**
 * An exception thrown when the leader session id attached to a message does not match
 * the leader session id that the receiver expects.
 */
public class LeaderIdMismatchException extends Exception {

	private static final long serialVersionUID = 1L;

	private final UUID expected;
	private final UUID actual;

	public LeaderIdMismatchException(UUID expected, UUID actual) {
		super("Leader session ID mismatch: expected=" + expected + ", actual=" + actual);
		this.expected = expected;
		this.actual = actual;
	}

	public UUID getExpected() {
		return expected;
	}

	public UUID getActual() {
		return actual;
	}
}
