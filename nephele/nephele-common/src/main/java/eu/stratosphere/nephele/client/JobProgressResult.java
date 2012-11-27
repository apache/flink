/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2012 by the Stratosphere project (http://stratosphere.eu)
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

package eu.stratosphere.nephele.client;

import java.util.ArrayList;
import java.util.Iterator;

import eu.stratosphere.nephele.event.job.AbstractEvent;
import eu.stratosphere.nephele.util.UnmodifiableIterator;

/**
 * A job progress result is used to report the current progress of a job.
 * <p>
 * This class is thread-safe.
 * 
 * @author warneke
 */
public final class JobProgressResult extends AbstractJobResult {

	/**
	 * The list containing the events.
	 */
	private final ArrayList<AbstractEvent> events;

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
			final ArrayList<AbstractEvent> events) {

		super(returnCode, description);

		this.events = events;
	}

	/**
	 * Default constructor required by kryo.
	 */
	@SuppressWarnings("unused")
	private JobProgressResult() {
		this.events = null;
	}

	/**
	 * Returns an iterator to the list of events transported within this job progress result object.
	 * 
	 * @return an iterator to the possibly empty list of events
	 */
	public Iterator<AbstractEvent> getEvents() {

		return new UnmodifiableIterator<AbstractEvent>(this.events.iterator());
	}

	/**
	 * {@inheritDoc}
	 */
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

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int hashCode() {

		return super.hashCode();
	}
}
