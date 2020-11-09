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

/**
 * A watcher on {@link JobGraphStore}. It could monitor all the changes on the job graph store and notify the
 * {@link JobGraphStore} via {@link JobGraphStore.JobGraphListener}.
 *
 * <p><strong>Important</strong>: The {@link JobGraphStoreWatcher} could not guarantee that there is no
 * {@link JobGraphStore.JobGraphListener} callbacks happen after {@link #stop()}. So the implementor is
 * responsible for filtering out these spurious callbacks.
 */
public interface JobGraphStoreWatcher {

	/**
	 * Start the watcher on {@link JobGraphStore}.
	 *
	 * @param jobGraphListener use jobGraphListener to notify the {@link DefaultJobGraphStore}
	 *
	 * @throws Exception when start internal services
	 */
	void start(JobGraphStore.JobGraphListener jobGraphListener) throws Exception;

	/**
	 * Stop the watcher on {@link JobGraphStore}.
	 *
	 * @throws Exception when stop internal services
	 */
	void stop() throws Exception;
}
