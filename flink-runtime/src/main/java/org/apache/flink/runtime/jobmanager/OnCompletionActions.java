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

package org.apache.flink.runtime.jobmanager;

import org.apache.flink.runtime.jobmaster.JobResult;

/**
 * Interface for completion actions once a Flink job has reached
 * a terminal state.
 */
public interface OnCompletionActions {

	/**
	 * Job finished successfully.
	 *
	 * @param result of the job execution
	 */
	void jobFinished(JobResult result);

	/**
	 * Job failed with an exception.
	 *
	 * @param result The result of the job carrying the failure cause.
	 */
	void jobFailed(JobResult result);

	/**
	 * Job was finished by another JobMaster.
	 */
	void jobFinishedByOther();
}
