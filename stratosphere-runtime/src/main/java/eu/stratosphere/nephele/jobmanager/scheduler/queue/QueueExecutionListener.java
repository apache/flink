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

package eu.stratosphere.nephele.jobmanager.scheduler.queue;

import eu.stratosphere.nephele.executiongraph.ExecutionVertex;
import eu.stratosphere.nephele.jobmanager.scheduler.AbstractExecutionListener;

/**
 * This is a wrapper class for the {@link QueueScheduler} to receive
 * notifications about state changes of vertices belonging
 * to scheduled jobs.
 * <p>
 * This class is thread-safe.
 * 
 */
public final class QueueExecutionListener extends AbstractExecutionListener {

	/**
	 * Constructs a new queue execution listener.
	 * 
	 * @param scheduler
	 *        the scheduler this listener is connected with
	 * @param executionVertex
	 *        the execution vertex this listener is created for
	 */
	public QueueExecutionListener(final QueueScheduler scheduler, final ExecutionVertex executionVertex) {
		super(scheduler, executionVertex);
	}
}
