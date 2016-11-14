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

package org.apache.flink.runtime.clusterframework;

/**
 * The status of an application.
 */
public enum ApplicationStatus {

	/** Application finished successfully */
	SUCCEEDED(0),

	/** Application encountered an unrecoverable failure or error */
	FAILED(1443),
	
	/** Application was canceled or killed on request */
	CANCELED(1444),

	/** Application status is not known */
	UNKNOWN(1445);
	
	// ------------------------------------------------------------------------

	/** The associated process exit code */
	private final int processExitCode;
	
	private ApplicationStatus(int exitCode) {
		this.processExitCode = exitCode;
	}

	/**
	 * Gets the process exit code associated with this status
	 * @return The associated process exit code.
	 */
	public int processExitCode() {
		return processExitCode;
	}

}
