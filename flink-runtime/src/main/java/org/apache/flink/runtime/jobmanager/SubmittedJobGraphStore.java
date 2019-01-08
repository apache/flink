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

import org.apache.flink.api.common.JobID;

import javax.annotation.Nullable;

import java.util.Collection;

/**
 * {@link SubmittedJobGraph} instances for recovery.
 */
public interface SubmittedJobGraphStore {

	/**
	 * Starts the {@link SubmittedJobGraphStore} service.
	 */
	void start(SubmittedJobGraphListener jobGraphListener) throws Exception;

	/**
	 * Stops the {@link SubmittedJobGraphStore} service.
	 */
	void stop() throws Exception;

	/**
	 * Returns the {@link SubmittedJobGraph} with the given {@link JobID} or
	 * {@code null} if no job was registered.
	 */
	@Nullable
	SubmittedJobGraph recoverJobGraph(JobID jobId) throws Exception;

	/**
	 * Adds the {@link SubmittedJobGraph} instance.
	 *
	 * <p>If a job graph with the same {@link JobID} exists, it is replaced.
	 */
	void putJobGraph(SubmittedJobGraph jobGraph) throws Exception;

	/**
	 * Removes the {@link SubmittedJobGraph} with the given {@link JobID} if it exists.
	 */
	void removeJobGraph(JobID jobId) throws Exception;

	/**
	 * Get all job ids of submitted job graphs to the submitted job graph store.
	 *
	 * @return Collection of submitted job ids
	 * @throws Exception if the operation fails
	 */
	Collection<JobID> getJobIds() throws Exception;

	/**
	 * A listener for {@link SubmittedJobGraph} instances. This is used to react to races between
	 * multiple running {@link SubmittedJobGraphStore} instances (on multiple job managers).
	 */
	interface SubmittedJobGraphListener {

		/**
		 * Callback for {@link SubmittedJobGraph} instances added by a different {@link
		 * SubmittedJobGraphStore} instance.
		 *
		 * <p><strong>Important:</strong> It is possible to get false positives and be notified
		 * about a job graph, which was added by this instance.
		 *
		 * @param jobId The {@link JobID} of the added job graph
		 */
		void onAddedJobGraph(JobID jobId);

		/**
		 * Callback for {@link SubmittedJobGraph} instances removed by a different {@link
		 * SubmittedJobGraphStore} instance.
		 *
		 * @param jobId The {@link JobID} of the removed job graph
		 */
		void onRemovedJobGraph(JobID jobId);
	}
}
