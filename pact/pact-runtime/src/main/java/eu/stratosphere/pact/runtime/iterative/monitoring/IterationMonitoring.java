/***********************************************************************************************************************
 *
 * Copyright (C) 2012 by the Stratosphere project (http://stratosphere.eu)
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

package eu.stratosphere.pact.runtime.iterative.monitoring;

import eu.stratosphere.nephele.jobgraph.JobID;

public class IterationMonitoring {

	public enum Event {
		HEAD_STARTING,
		HEAD_WAITING_FOR_OTHERS,
		HEAD_FINISHED,
		HEAD_PACT_STARTING,
		HEAD_PACT_FINISHED,
		TAIL_STARTING,
		TAIL_FINISHED,
		TAIL_PACT_STARTING,
		TAIL_PACT_FINISHED,
		INTERMEDIATE_STARTING,
		INTERMEDIATE_FINISHED,
		INTERMEDIATE_PACT_STARTING,
		INTERMEDIATE_PACT_FINISHED,
		SYNC_STARTING,
		SYNC_FINISHED
	}

	public static String logLine(JobID jobID, Event event, int iteration, int indexInSubtaskGroup) {
		StringBuilder line = new StringBuilder();
		line.append("[ITERATION-LOG]");
		line.append(jobID.toString());
		line.append('-');
		line.append(event.name());
		line.append('-');
		line.append(iteration);
		line.append('-');
		line.append(indexInSubtaskGroup);
		line.append('-');
		line.append(System.currentTimeMillis());
		line.append("[/ITERATION-LOG]");
		return line.toString();
	}

}
