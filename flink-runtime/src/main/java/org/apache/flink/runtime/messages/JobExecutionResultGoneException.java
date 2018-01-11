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

package org.apache.flink.runtime.messages;

import org.apache.flink.api.common.JobID;
import org.apache.flink.util.FlinkException;

/**
 * Exception indicating that the required {@link org.apache.flink.runtime.jobmaster.JobResult} was
 * garbage collected.
 * @see org.apache.flink.runtime.dispatcher.JobExecutionResultCache
 */
public class JobExecutionResultGoneException extends FlinkException {

	private static final long serialVersionUID = 1L;

	public JobExecutionResultGoneException(JobID jobId) {
		super(String.format("Job execution result for job [%s] is gone.", jobId));
	}
}
