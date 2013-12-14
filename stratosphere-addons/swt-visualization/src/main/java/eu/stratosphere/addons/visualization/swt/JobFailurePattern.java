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

package eu.stratosphere.addons.visualization.swt;

import java.util.Iterator;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * A job failure pattern consists of a series of {@link AbstractFailureEvent} objects that occur over time.
 * <p>
 * This class is not thread-safe.
 * 
 * @author warneke
 */
public final class JobFailurePattern {

	/**
	 * The name of the job failure pattern.
	 */
	private final String name;

	/**
	 * The series of events belonging to this job failure pattern.
	 */
	private final SortedSet<AbstractFailureEvent> events = new TreeSet<AbstractFailureEvent>();

	/**
	 * Constructs a new job failure pattern with a given name.
	 * 
	 * @param name
	 *        the name of the job failure pattern
	 */
	public JobFailurePattern(final String name) {

		if (name == null) {
			throw new IllegalArgumentException("Argument name must not be null");
		}

		if (name.isEmpty()) {
			throw new IllegalArgumentException("Argument name must not be empty");
		}

		this.name = name;
	}

	/**
	 * Returns the name of the job failure pattern.
	 * 
	 * @return the name of the job failure pattern
	 */
	public String getName() {

		return this.name;
	}

	/**
	 * Adds a failure event to this job failure pattern.
	 * 
	 * @param event
	 *        the event to be added
	 */
	public void addEvent(final AbstractFailureEvent event) {

		if (event == null) {
			throw new IllegalArgumentException("Argument event must not be null");
		}

		if (this.events.contains(event)) {
			return;
		}

		this.events.add(event);
	}

	/**
	 * Removes a failure event from this job failure pattern.
	 * 
	 * @param event
	 *        the event to be removed
	 */
	public void removeEvent(final AbstractFailureEvent event) {

		if (event == null) {
			throw new IllegalArgumentException("Argument event must not be null");
		}

		this.events.remove(event);
	}

	/**
	 * Returns an iterator to access all the events stored in this job failure pattern.
	 * 
	 * @return an iterator to access all the events stored in this job failure pattern
	 */
	public Iterator<AbstractFailureEvent> iterator() {

		return this.events.iterator();
	}
}
