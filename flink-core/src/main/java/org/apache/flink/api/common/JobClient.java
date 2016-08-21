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
package org.apache.flink.api.common;

import java.util.Map;

/*
 * An Flink job client interface to interact with running Flink jobs.
 */
public interface JobClient {

	/**
	 * Gets the JobID associated with this JobClient.
	 */
	JobID getJobID();

	/**
	 * Returns a boolean indicating whether the job execution has finished.
	 */
	boolean hasFinished() throws Exception;

	/**
	 * Blocks until the result of the job execution is returned.
	 */
	JobExecutionResult waitForResult() throws Exception;

	/**
	 * Gets the accumulator map of a running job.
	 */
	Map<String, Object> getAccumulators() throws Exception;

	/**
	 * Cancels a running job.
	 */
	void cancel() throws Exception;

	/**
	 * Stops a running job if the job supports stopping.
	 */
	void stop() throws Exception;

	/**
	 * Adds a Runnable to this JobClient to be called
	 * when the client is shut down. Runnables are called
	 * in the order they are added.
	 */
	void addFinalizer(Runnable finalizer) throws Exception;

	/**
	 * Runs finalization code to shutdown the client
	 * and its dependencies.
	 */
	void shutdown();

}
