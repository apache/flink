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

package org.apache.flink.runtime.util;

/**
 * Enumeration to indicate the associated process exit code.
 */
public enum ExitCode {

	/** The exit code of the succeeded application process. */
	SUCCESS(0),
	/** The exit code for running a TaskExecutor in the container. */
	RUNNER_INIT(31),
	/** The exit code of the JVM process killed by the safeguard. */
	JVM_SHUTDOWN(-17),
	/** The exit code of the fatal handler for uncaught exceptions. */
	FATAL_UNCAUGHT(-99);

	// --------------------------------------------------------------------------------------------

	private final int exitCode;

	ExitCode(int exitCode) {
		this.exitCode = exitCode;
	}

	public int getExitCode() {
		return exitCode;
	}
}
