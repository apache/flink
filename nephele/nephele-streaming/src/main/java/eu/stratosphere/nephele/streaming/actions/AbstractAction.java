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

package eu.stratosphere.nephele.streaming.actions;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.io.IOReadableWritable;
import eu.stratosphere.nephele.jobgraph.JobID;

/**
 * This class implements an abstract base class for actions the job manager component of the Nephele streaming plugin
 * can initiate to achieve particular latency or throughput goals.
 * 
 * @author warneke
 */
public abstract class AbstractAction implements IOReadableWritable {

	/**
	 * The ID of the job the initiated action applies to.
	 */
	private final JobID jobID;

	/**
	 * Constructs a new abstract action object.
	 * 
	 * @param jobID
	 *        the ID of the job the initiated action applies to
	 */
	AbstractAction(final JobID jobID) {

		if (jobID == null) {
			throw new IllegalArgumentException("Argument jobID must not be null");
		}

		this.jobID = jobID;
	}

	/**
	 * Default constructor required for deserialization.
	 */
	AbstractAction() {
		this.jobID = new JobID();
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

	/**
	 * Returns the ID of the job the initiated action applies to.
	 * 
	 * @return the ID of the job the initiated action applies to
	 */
	public JobID getJobID() {

		return this.jobID;
	}
	
	/**
	 * Returns the ID of the vertex the initiated action applies to.
	 * 
	 * @return the ID of the vertex the initiated action applies to
	 */
	public abstract ExecutionVertexID getVertexID();
}
