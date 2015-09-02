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

package org.apache.flink.runtime.leaderretrieval;

import com.google.common.base.Preconditions;

/**
 * Standalone implementation of the {@link LeaderRetrievalService}. The standalone implementation
 * assumes that there is only a single {@link org.apache.flink.runtime.jobmanager.JobManager} whose
 * address is given to the service when creating it. This address is directly given to the
 * {@link LeaderRetrievalListener} when the service is started.
 */
public class StandaloneLeaderRetrievalService implements LeaderRetrievalService {

	/** Address of the only JobManager */
	private final String jobManagerAddress;

	/** Listener which wants to be notified about the new leader */
	private LeaderRetrievalListener leaderListener;

	/**
	 * Creates a StandaloneLeaderRetrievalService with the given JobManager address.
	 *
	 * @param jobManagerAddress The JobManager's address which is returned to the
	 * 							{@link LeaderRetrievalListener}
	 */
	public StandaloneLeaderRetrievalService(String jobManagerAddress) {
		this.jobManagerAddress = jobManagerAddress;
	}

	public void start(LeaderRetrievalListener listener) {
		Preconditions.checkNotNull(listener, "Listener must not be null.");
		Preconditions.checkState(leaderListener == null, "StandaloneLeaderRetrievalService can " +
				"only be started once.");

		leaderListener = listener;

		// directly notify the listener, because we already know the leading JobManager's address
		leaderListener.notifyLeaderAddress(jobManagerAddress, null);
	}

	public void stop() {}
}
