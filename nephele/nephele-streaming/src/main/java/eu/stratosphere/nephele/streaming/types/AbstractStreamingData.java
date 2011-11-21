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

package eu.stratosphere.nephele.streaming.types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import eu.stratosphere.nephele.io.IOReadableWritable;
import eu.stratosphere.nephele.jobgraph.JobID;

/**
 * Abstract base class to be used to for exchanging data between the different
 * components of the streaming plugin.
 * 
 * @author warneke
 */
public abstract class AbstractStreamingData implements IOReadableWritable {

	/**
	 * The ID of the job this piece of streaming data refers to
	 */
	private final JobID jobID;

	public AbstractStreamingData(JobID jobID) {
		if (jobID == null) {
			throw new IllegalArgumentException("jobID must not be null");
		}

		this.jobID = jobID;
	}

	/**
	 * Returns the ID of the job this path latency information refers to.
	 * 
	 * @return the ID of the job this path latency information refers to
	 */
	public JobID getJobID() {

		return this.jobID;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void write(final DataOutput out) throws IOException {
		this.jobID.write(out);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void read(final DataInput in) throws IOException {
		this.jobID.read(in);
	}
}
