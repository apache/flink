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

package org.apache.flink.api.common;

import org.apache.flink.annotation.Internal;

/**
 * A ProgramExecutor execute a Flink program's job.
 *
 * <p>The specific implementation (such as the org.apache.flink.client.LocalExecutor
 * and org.apache.flink.client.RemoteExecutor) determines where and how to run the dataflow.
 * The concrete implementations of the executors are loaded dynamically, because they depend on
 * the full set of all runtime classes.</p>
 */
@Internal
public interface ProgramExecutor {

	// ------------------------------------------------------------------------
	//  Config Options
	// ------------------------------------------------------------------------

	/**
	 * Sets whether the executor should print progress results to "standard out" ({@link System#out}).
	 * All progress messages are logged using the configured logging framework independent of the value
	 * set here.
	 *
	 * @param printStatus True, to print progress updates to standard out, false to not do that.
	 */
	void setPrintStatusDuringExecution(boolean printStatus);

	/**
	 * Gets whether the executor prints progress results to "standard out" ({@link System#out}).
	 *
	 * @return True, if the executor prints progress messages to standard out, false if not.
	 */
	boolean isPrintingStatusDuringExecution();

	/**
	 * Starts the program executor. After the executor has been started, it will keep
	 * running until {@link #stop()} is called.
	 *
	 * @throws Exception Thrown, if the executor startup failed.
	 */
	void start() throws Exception;

	/**
	 * Shuts down the executor and releases all local resources.
	 *
	 * <p>This method also ends all sessions created by this executor. Remote job executions
	 * may complete, but the session is not kept alive after that.</p>
	 *
	 * @throws Exception Thrown, if the proper shutdown failed.
	 */
	void stop() throws Exception;

	/**
	 * Checks if this executor is currently running.
	 *
	 * @return True is the executor is running, false otherwise.
	 */
	boolean isRunning();

	/**
	 * Ends the job session, identified by the given JobID. Jobs can be kept around as sessions,
	 * if a session timeout is specified. Keeping Jobs as sessions allows users to incrementally
	 * add new operations to their dataflow, that refer to previous intermediate results of the
	 * dataflow.
	 *
	 * @param jobID The JobID identifying the job session.
	 * @throws Exception Thrown, if the message to finish the session cannot be delivered.
	 */
	void endSession(JobID jobID) throws Exception;
}
