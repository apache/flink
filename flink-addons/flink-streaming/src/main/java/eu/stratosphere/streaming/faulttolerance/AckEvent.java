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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import eu.stratosphere.nephele.event.task.AbstractTaskEvent;

/**
 * TaskEvent for sending record acknowledgements to the input's fault tolerance
 * buffer
 */
public class AckEvent extends AbstractTaskEvent {

	private String recordId;

	/**
	 * Creates a new event to acknowledge the record with the given ID
	 * 
	 * @param recordId
	 *            ID of the record to be acknowledged
	 */
	public AckEvent(String recordId) {
		setRecordId(recordId);
	}

	@Override
	public void write(DataOutput out) throws IOException {
	}

	@Override
	public void read(DataInput in) throws IOException {
	}

	public void setRecordId(String recordId) {
		this.recordId = recordId;
	}

	public String getRecordId() {
		return this.recordId;
	}

}