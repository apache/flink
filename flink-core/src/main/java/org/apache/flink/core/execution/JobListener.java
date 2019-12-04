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

package org.apache.flink.core.execution;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.JobExecutionResult;

/**
 * A listener that is notified on specific job status changed, which should be firstly
 * registered by {@code #registerJobListener} of execution environments.
 */
@PublicEvolving
public interface JobListener {

	/**
	 * Callback on job submission succeeded.
	 *
	 * <p><b>ATTENTION:</b> the lifecycle of the passed {@link JobClient} has already
	 * been handled. Never call {@link JobClient#close()} on the passed {@link JobClient}.
	 * Also it means that the passed {@link JobClient} can be closed concurrently on job
	 * finished so that you should take care of the failure case.
	 *
	 * @param jobClient to communicate with the job
	 */
	void onJobSubmitted(JobClient jobClient);

	/**
	 * Callback on job execution finished. It is only called back when you call {@code #execute}
	 * instead of {@code executeAsync} methods of execution environments.
	 */
	void onJobExecuted(JobExecutionResult jobExecutionResult);

}
