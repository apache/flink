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

package eu.stratosphere.nephele.client;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;

import eu.stratosphere.nephele.event.job.AbstractEvent;
import eu.stratosphere.nephele.util.SerializableArrayList;

/**
 * A <code>JobProgressResult</code> is used to report the current progress
 * of a job.
 * 
 * @author warneke
 */
public class JobProgressResult extends AbstractJobResult {

	/**
	 * The list containing the events.
	 */
	private final SerializableArrayList<AbstractEvent> events;

	/**
	 * Constructs a new job progress result object.
	 * 
	 * @param returnCode
	 *        the return code that shall be carried by this result object
	 * @param description
	 *        the description of the job status
	 * @param events
	 *        the job events to be transported within this object
	 */
	public JobProgressResult(final ReturnCode returnCode, final String description,
			final SerializableArrayList<AbstractEvent> events) {

		super(returnCode, description);

		this.events = events;
	}

	/**
	 * Empty constructor used for object deserialization.
	 */
	public JobProgressResult() {
		super();

		this.events = new SerializableArrayList<AbstractEvent>();
	}


	@Override
	public void read(final DataInput in) throws IOException {
		super.read(in);

		this.events.read(in);
	}


	@Override
	public void write(final DataOutput out) throws IOException {
		super.write(out);

		this.events.write(out);
	}

	/**
	 * Returns an iterator to the list of events transported within this job progress result object.
	 * 
	 * @return an iterator to the possibly empty list of events
	 */
	public Iterator<AbstractEvent> getEvents() {

		return this.events.iterator();
	}


	@Override
	public boolean equals(final Object obj) {

		if (!super.equals(obj)) {
			return false;
		}

		if (!(obj instanceof JobProgressResult)) {
			return false;
		}

		final JobProgressResult jpr = (JobProgressResult) obj;

		if (!this.events.equals(jpr.events)) {
			return false;
		}

		return true;
	}


	@Override
	public int hashCode() {

		return super.hashCode();
	}
}
