/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.nephele.profiling.impl.types;

import java.io.IOException;

import eu.stratosphere.core.memory.DataInputView;
import eu.stratosphere.core.memory.DataOutputView;
import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.jobgraph.JobID;

public class InternalExecutionVertexThreadProfilingData extends InternalExecutionVertexProfilingData {

	private int profilingInterval = 0;

	private int userTime = 0;

	private int systemTime = 0;

	private int blockedTime = 0;

	private int waitedTime = 0;

	public InternalExecutionVertexThreadProfilingData(JobID jobID, ExecutionVertexID executionVertexID,
			int profilingInterval, int userTime, int systemTime, int blockedTime, int waitedTime) {

		super(jobID, executionVertexID);

		this.profilingInterval = profilingInterval;
		this.userTime = userTime;
		this.systemTime = systemTime;
		this.blockedTime = blockedTime;
		this.waitedTime = waitedTime;
	}

	public InternalExecutionVertexThreadProfilingData() {
	}

	@Override
	public void read(DataInputView in) throws IOException {

		super.read(in);

		this.profilingInterval = in.readInt();
		this.userTime = in.readInt();
		this.systemTime = in.readInt();
		this.blockedTime = in.readInt();
		this.waitedTime = in.readInt();
	}

	@Override
	public void write(DataOutputView out) throws IOException {

		super.write(out);

		out.writeInt(this.profilingInterval);
		out.writeInt(this.userTime);
		out.writeInt(this.systemTime);
		out.writeInt(this.blockedTime);
		out.writeInt(this.waitedTime);
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
