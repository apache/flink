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

package eu.stratosphere.nephele.profiling.types;

import eu.stratosphere.nephele.jobgraph.JobID;

public class InstanceSummaryProfilingEvent extends InstanceProfilingEvent {

	public InstanceSummaryProfilingEvent(int profilingInterval, int ioWaitCPU, int idleCPU, int userCPU, int systemCPU,
			int hardIrqCPU, int softIrqCPU, long totalMemory, long freeMemory, long bufferedMemory, long cachedMemory,
			long cachedSwapMemory, long receivedBytes, long transmittedBytes, JobID jobID, long timestamp,
			long profilingTimestamp) {
		super(profilingInterval, ioWaitCPU, idleCPU, userCPU, systemCPU, hardIrqCPU, softIrqCPU, totalMemory,
			freeMemory, bufferedMemory, cachedMemory, cachedSwapMemory, receivedBytes, transmittedBytes, jobID,
			timestamp, profilingTimestamp);
	}

	public InstanceSummaryProfilingEvent() {
		super();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean equals(Object obj) {

		if (!super.equals(obj)) {
			return false;
		}

		if (!(obj instanceof InstanceSummaryProfilingEvent)) {
			return false;
		}

		return true;
	}
}
