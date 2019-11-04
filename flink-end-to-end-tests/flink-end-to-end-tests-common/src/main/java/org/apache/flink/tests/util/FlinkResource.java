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

package org.apache.flink.tests.util;

import java.io.IOException;

/**
 * The interface to manage the Flink related cluster resource for end-to-end test suites. it can be used for starting/
 * stopping cluster/jobManager/taskManger, also we can create flink client or flink SQL client to submit job.
 */
public interface FlinkResource {

	/**
	 * Start the flink cluster with one jobManager and taskManager.
	 *
	 * @throws IOException
	 */
	default void startCluster() throws IOException {
		startCluster(1);
	}

	/**
	 * @param taskManagerNum number of taskMangers to start.
	 * @throws IOException
	 */
	void startCluster(int taskManagerNum) throws IOException;

	/**
	 * Stop the given jobManager.
	 *
	 * @throws IOException
	 */
	void stopJobManager() throws IOException;

	/**
	 * Stop all the taskManagers.
	 *
	 * @throws IOException
	 */
	void stopTaskMangers() throws IOException;

	/**
	 * Stop the cluster, means all taskManagers and jobManager resources will be stopped.
	 *
	 * @throws IOException
	 */
	void stopCluster() throws IOException;

	/**
	 * Create a {@link FlinkClient} to submit the job.
	 *
	 * @return the flink client which wrap the ./bin/flink commands.
	 * @throws IOException
	 */
	FlinkClient createFlinkClient() throws IOException;

	/**
	 * Create a {@link FlinkSQLClient} to submit SQL job.
	 *
	 * @return the flink SQL client instance, which wrap the flink interactive SQL client.
	 * @throws IOException
	 */
	FlinkSQLClient createFlinkSQLClient() throws IOException;
}
