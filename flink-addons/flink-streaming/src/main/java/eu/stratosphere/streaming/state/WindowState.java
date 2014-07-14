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

import java.util.HashMap;

import org.apache.commons.collections.buffer.CircularFifoBuffer;

import eu.stratosphere.streaming.api.streamrecord.StreamRecord;
import eu.stratosphere.streaming.index.IndexPair;

/**
 * The window state for window operator. To be general enough, this class
 * implements a count based window operator. It is possible for the user to
 * compose time based window operator by extending this class by splitting the
 * stream into multiple mini batches.
 */
public class WindowState<K> {
	private int windowSize;
	private int slidingStep;
	private int computeGranularity;
	private int windowFieldId;

	private int initTimestamp;
	private int nextTimestamp;
	private int currentRecordCount;
	private int fullRecordCount;
	private int slideRecordCount;

	HashMap<K, IndexPair> windowIndex;
	CircularFifoBuffer buffer;
	StreamRecord tempRecord;

	public WindowState(int windowSize, int slidingStep, int computeGranularity,
			int windowFieldId) {
		this.windowSize = windowSize;
		this.slidingStep = slidingStep;
		this.computeGranularity = computeGranularity;
		this.windowFieldId = windowFieldId;

		this.initTimestamp = -1;
		this.nextTimestamp = -1;
		this.currentRecordCount = 0;
		// here we assume that windowSize and slidingStep is divisible by
		// computeGranularity.
		this.fullRecordCount = windowSize / computeGranularity;
		this.slideRecordCount = slidingStep / computeGranularity;

		this.windowIndex = new HashMap<K, IndexPair>();
		this.buffer = new CircularFifoBuffer(fullRecordCount);
	}

	public void pushBack(StreamRecord record) {
		if (initTimestamp == -1) {
			initTimestamp = (Integer) record.getTuple(0).getField(windowFieldId);
			nextTimestamp = initTimestamp + computeGranularity;
			tempRecord = new StreamRecord(record.getNumOfFields());
		}
		for (int i = 0; i < record.getNumOfTuples(); ++i) {
			while ((Integer) record.getTuple(i).getField(windowFieldId) > nextTimestamp) {
				buffer.add(tempRecord);
				currentRecordCount += 1;
				tempRecord = new StreamRecord(record.getNumOfFields());
			}
			tempRecord.addTuple(record.getTuple(i));
		}
	}

	public StreamRecord popFront() {
		StreamRecord frontRecord = (StreamRecord) buffer.get();
		buffer.remove();
		return frontRecord;
	}

	public boolean isFull() {
		return currentRecordCount >= fullRecordCount;
	}

	public boolean isComputable() {
		if (currentRecordCount == fullRecordCount + slideRecordCount) {
			currentRecordCount -= slideRecordCount;
			return true;
		}
		return false;
	}

}
