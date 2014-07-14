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

package eu.stratosphere.streaming.api;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.event.task.AbstractTaskEvent;
import eu.stratosphere.nephele.event.task.EventListener;

/**
 * EventListener for record acknowledgement events. When an AckEvent occurs,
 * uses the task's fault tolerance buffer to acknowledge the given record.
 */
public class AckEventListener implements EventListener {

	private static final Log log = LogFactory.getLog(AckEventListener.class);

	private String taskInstanceID;
	private FaultToleranceBuffer recordBuffer;

	/**
	 * Creates an AckEventListener that monitors AckEvents sent to task with the
	 * given ID.
	 * 
	 * @param taskInstanceID
	 *            ID of the task that creates the listener
	 * @param recordBuffer
	 *            The fault tolerance buffer associated with this task
	 */
	public AckEventListener(String taskInstanceID,
			FaultToleranceBuffer recordBuffer) {
		this.taskInstanceID = taskInstanceID;
		this.recordBuffer = recordBuffer;
	}

	/**
	 * When an AckEvent occurs checks if it was directed at this task, if so,
	 * acknowledges the record given in the AckEvent
	 * 
	 */
	public void eventOccurred(AbstractTaskEvent event) {

		AckEvent ackEvent = (AckEvent) event;
		String recordId = ackEvent.getRecordId();
		String ackCID = recordId.split("-", 2)[0];
		if (ackCID.equals(taskInstanceID)) {

			Long nt = System.nanoTime();
			recordBuffer.ackRecord(ackEvent.getRecordId());

			if (log.isDebugEnabled()) {
				log.debug("Ack recieved " + ackEvent.getRecordId()
						+ "\nAck exec. time(ns): " + (System.nanoTime() - nt));
			}

			// System.out.println("Ack recieved " + ackEvent.getRecordId()
			// + "\nAck exec. time(ns): " + (System.nanoTime() - nt));
			// System.out.println("--------------");
		}

	}
}
