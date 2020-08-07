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

package org.apache.flink.tests.util.flink;

import org.apache.flink.util.AutoCloseableAsync;

import java.io.IOException;

/**
 * Controller for interacting with a cluster.
 */
public interface ClusterController extends AutoCloseableAsync {

	/**
	 * Submits the given job to the cluster.
	 *
	 * @param job job to submit
	 * @return JobController for the submitted job
	 * @throws IOException
	 */
	JobController submitJob(JobSubmission job) throws IOException;

	/**
	 * Submits the given SQL job to the cluster.
	 *
	 * @param job job to submit.
	 * @throws IOException if any IO error happen.
	 */
	void submitSQLJob(SQLJobSubmission job) throws IOException;
}
