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
	 * The series of events belonging to this job failure pattern.
	 */
	private final SortedSet<AbstractFailureEvent> events = new TreeSet<AbstractFailureEvent>();

	/**
	 * Adds or updates a failure event to this job failure pattern.
	 * 
	 * @param event
	 *        the event to be added or updated
	 */
	public void addOrUpdateEvent(final AbstractFailureEvent event) {

		if (event == null) {
			throw new IllegalArgumentException("Argument event must not be null");
		}

		if (this.events.contains(event)) {
			return;
		}

		this.events.add(event);
	}
}
