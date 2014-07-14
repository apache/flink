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

package eu.stratosphere.streaming.state;

import org.apache.commons.collections.buffer.CircularFifoBuffer;

import eu.stratosphere.streaming.api.streamrecord.StreamRecord;

/**
 * The window state for window operator. To be general enough, this class
 * implements a count based window operator. It is possible for the user to
 * compose time based window operator by extending this class by splitting the
 * stream into multiple mini batches.
 */
public class WindowState<K> {
	private int currentRecordCount;
	private int fullRecordCount;
	private int slideRecordCount;

	CircularFifoBuffer buffer;

	public WindowState(int windowSize, int slidingStep, int computeGranularity) {
		this.currentRecordCount = 0;
		// here we assume that windowSize and slidingStep is divisible by
		// computeGranularity.
		this.fullRecordCount = windowSize / computeGranularity;
		this.slideRecordCount = slidingStep / computeGranularity;
		this.buffer = new CircularFifoBuffer(fullRecordCount);
	}

	public void pushBack(StreamRecord record) {
		buffer.add(record);
		currentRecordCount += 1;
	}

	public StreamRecord popFront() {
		StreamRecord frontRecord = (StreamRecord) buffer.get();
		buffer.remove();
		return frontRecord;
	}

	public boolean isFull() {
		return currentRecordCount >= fullRecordCount;
	}

	public boolean isEmittable() {
		if (currentRecordCount == fullRecordCount + slideRecordCount) {
			currentRecordCount -= slideRecordCount;
			return true;
		}
		return false;
	}

}
