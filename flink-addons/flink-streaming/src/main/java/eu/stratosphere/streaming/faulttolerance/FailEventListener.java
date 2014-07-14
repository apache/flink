/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2014 by the Stratosphere project (http://stratosphere.eu)
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

package eu.stratosphere.streaming.faulttolerance;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.event.task.AbstractTaskEvent;
import eu.stratosphere.nephele.event.task.EventListener;

/**
 * EventListener for record fail events. When a FailEvent occurs, uses the
 * task's fault tolerance buffer to fail and re-emit the given record.
 */
public class FailEventListener implements EventListener {

	private static final Log log = LogFactory.getLog(FailEventListener.class);

	private String taskInstanceID;
	private FaultToleranceUtil recordBuffer;
	private int output;

	/**
	 * Creates a FailEventListener that monitors FailEvents sent to task with
	 * the given ID.
	 * 
	 * @param taskInstanceID
	 *            ID of the task that creates the listener
	 * @param recordBuffer
	 *            The fault tolerance buffer associated with this task
	 * @param output
	 *            output channel
	 */
	public FailEventListener(String taskInstanceID, FaultToleranceUtil recordBuffer, int output) {
		this.taskInstanceID = taskInstanceID;
		this.recordBuffer = recordBuffer;
		this.output = output;
	}

	/**
	 * When a FailEvent occurs checks if it was directed at this task, if so,
	 * fails the record given in the FailEvent
	 * 
	 */
	public void eventOccurred(AbstractTaskEvent event) {
		FailEvent failEvent = (FailEvent) event;
		String recordId = failEvent.getRecordId();
		String failCID = recordId.split("-", 2)[0];
		if (failCID.equals(taskInstanceID)) {
			recordBuffer.failRecord(recordId, output);
			log.warn("FAIL RECIEVED: "+output +" "+ recordId);
		}

	}
}
