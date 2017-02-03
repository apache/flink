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

package org.apache.flink.runtime.executiongraph;

import org.apache.flink.api.common.JobID;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.jobgraph.JobStatus;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * A job status listener that waits lets one block until the job is in a terminal state.
 */
public class TerminalJobStatusListener  implements JobStatusListener {

	private final OneShotLatch terminalStateLatch = new OneShotLatch();

	public void waitForTerminalState(long timeoutMillis) throws InterruptedException, TimeoutException {
		terminalStateLatch.await(timeoutMillis, TimeUnit.MILLISECONDS);
	}

	@Override
	public void jobStatusChanges(JobID jobId, JobStatus newJobStatus, long timestamp, Throwable error) {
		if (newJobStatus.isGloballyTerminalState() || newJobStatus == JobStatus.SUSPENDED) {
			terminalStateLatch.trigger();
		}
	}
}
