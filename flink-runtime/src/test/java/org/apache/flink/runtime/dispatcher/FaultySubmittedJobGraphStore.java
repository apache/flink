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

package org.apache.flink.runtime.dispatcher;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.jobmanager.SubmittedJobGraph;
import org.apache.flink.runtime.testutils.InMemorySubmittedJobGraphStore;

import javax.annotation.Nullable;

/**
 * {@link InMemorySubmittedJobGraphStore} implementation which can throw artifical errors for
 * testing purposes.
 */
final class FaultySubmittedJobGraphStore extends InMemorySubmittedJobGraphStore {

	@Nullable
	private Exception recoveryFailure = null;

	@Nullable
	private Exception removalFailure = null;

	void setRecoveryFailure(@Nullable Exception recoveryFailure) {
		this.recoveryFailure = recoveryFailure;
	}

	void setRemovalFailure(@Nullable Exception removalFailure) {
		this.removalFailure = removalFailure;
	}

	@Override
	public synchronized SubmittedJobGraph recoverJobGraph(JobID jobId) throws Exception {
		if (recoveryFailure != null) {
			throw recoveryFailure;
		} else {
			return super.recoverJobGraph(jobId);
		}
	}

	@Override
	public synchronized void removeJobGraph(JobID jobId) throws Exception {
		if (removalFailure != null) {
			throw removalFailure;
		} else {
			super.removeJobGraph(jobId);
		}
	}
}
