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

package eu.stratosphere.nephele.visualization.swt;

/**
 * This class implements an abstract failure event which can be used to trigger either a planned vertex or planned
 * instance failure.
 * <p>
 * This class is thread-safe.
 * 
 * @author warneke
 */
public abstract class AbstractFailureEvent implements Comparable<AbstractFailureEvent> {

	/**
	 * The name of the event.
	 */
	private final String name;

	/**
	 * The interval in milliseconds until this event will occur.
	 */
	private final int interval;

	/**
	 * Constructs a new abstract failure event.
	 * 
	 * @param name
	 *        the name of the event
	 * @param interval
	 *        the interval in milliseconds until this event will occur
	 */
	AbstractFailureEvent(final String name, final int interval) {

		if (name == null) {
			throw new IllegalArgumentException("Argument name must not be null");
		}

		if (interval < 0) {
			throw new IllegalArgumentException("Argument interval must be larger than or equal to zero");
		}

		this.name = name;
		this.interval = interval;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int compareTo(final AbstractFailureEvent o) {

		return (this.interval - o.getInterval());
	}

	/**
	 * Returns the name of this event.
	 * 
	 * @return the name of this event
	 */
	public String getName() {

		return this.name;
	}

	/**
	 * Returns the interval in milliseconds until this event will occur.
	 * 
	 * @return the interval in milliseconds until this event will occur
	 */
	public int getInterval() {

		return this.interval;
	}
}
