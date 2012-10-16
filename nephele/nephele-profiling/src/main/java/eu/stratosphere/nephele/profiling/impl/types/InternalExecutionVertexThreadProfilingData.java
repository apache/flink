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

package eu.stratosphere.nephele.profiling.impl.types;

import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.jobgraph.JobID;

public class InternalExecutionVertexThreadProfilingData extends InternalExecutionVertexProfilingData {

	private final int profilingInterval;

	private final int userTime;

	private final int systemTime;

	private final int blockedTime;

	private final int waitedTime;

	public InternalExecutionVertexThreadProfilingData(JobID jobID, ExecutionVertexID executionVertexID,
			int profilingInterval, int userTime, int systemTime, int blockedTime, int waitedTime) {

		super(jobID, executionVertexID);

		this.profilingInterval = profilingInterval;
		this.userTime = userTime;
		this.systemTime = systemTime;
		this.blockedTime = blockedTime;
		this.waitedTime = waitedTime;
	}

	/**
	 * Default constructor required by kryo.
	 */
	@SuppressWarnings("unused")
	private InternalExecutionVertexThreadProfilingData() {

		this.profilingInterval = 0;
		this.userTime = 0;
		this.systemTime = 0;
		this.blockedTime = 0;
		this.waitedTime = 0;
	}

	public int getBlockedTime() {

		return this.blockedTime;
	}

	public int getProfilingInterval() {

		return this.profilingInterval;
	}

	public int getSystemTime() {

		return this.systemTime;
	}

	public int getUserTime() {

		return this.userTime;
	}

	public int getWaitedTime() {

		return this.waitedTime;
	}
}
