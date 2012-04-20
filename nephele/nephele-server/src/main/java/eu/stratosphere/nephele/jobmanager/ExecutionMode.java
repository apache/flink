/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.nephele.jobmanager;

import eu.stratosphere.nephele.taskmanager.TaskManager;

/**
 * This enumeration encompasses all execution modes Nephele can run in.
 * 
 * @author warneke
 */
enum ExecutionMode {

	/**
	 * In local mode, Nephele automatically launches a {@link TaskManager} when starting the {@link JobManager}.
	 */
	LOCAL,

	/**
	 * In cluster mode, Nephele only starts the {@link JobManager} and waits for the individual {@link TaskManager}
	 * instances to register themselves before executing the first incoming job.
	 */
	CLUSTER,

	/**
	 * In cloud mode, Nephele only starts the {@link JobManager} and waits for a job to arrive. Upon reception of a job,
	 * the {@link JobManager} will attempt to allocate the resources necessary to execute the job dynamically from an
	 * Infrastructure as a Service cloud.
	 */
	CLOUD;

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String toString() {

		if (this == ExecutionMode.CLOUD) {
			return "cloud";
		}
		if (this == ExecutionMode.CLUSTER) {
			return "cluster";
		}

		return "local";
	}
}
