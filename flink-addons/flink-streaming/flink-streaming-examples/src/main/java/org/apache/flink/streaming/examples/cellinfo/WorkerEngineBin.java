/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.flink.streaming.examples.cellinfo;

import org.apache.flink.streaming.examples.cellinfo.Util;

public class WorkerEngineBin implements java.io.Serializable, IWorkerEngine {

	private static final long serialVersionUID = 1L;
	private long unitLength_;
	private long lastTimeUpdated_;
	private int[][] counters_;
	private int pointer_;

	public WorkerEngineBin(long unitLength, int numOfCells, int bufferInterval, long currentTime) {
		lastTimeUpdated_ = currentTime / unitLength * unitLength;
		unitLength_ = unitLength;
		counters_ = new int[(int) (bufferInterval / unitLength) + 1][numOfCells];
	}

	private int getCell(int interval, int cell) {
		return counters_[Util.mod((interval + pointer_), counters_.length)][cell];
	}

	private void incrCell(int interval, int cell) {
		++counters_[Util.mod((interval + pointer_), counters_.length)][cell];
	}

	private void toZero(int interval) {
		for (int cell = 0; cell < counters_[0].length; ++cell) {
			counters_[Util.mod((interval + pointer_), counters_.length)][cell] = 0;
		}
	}

	private void shift(int shiftBy) {
		if (shiftBy > 0 && shiftBy < counters_.length) {
			pointer_ = Util.mod((pointer_ - shiftBy), counters_.length);
			for (int i = 0; i < shiftBy; ++i) {
				toZero(i);
			}
		} else if (shiftBy >= counters_.length) {
			pointer_ = 0;
			for (int i = 0; i < counters_.length; ++i) {
				toZero(i);
			}
		}
	}

	public int get(long timeStamp, long lastMillis, int cellId) {
		int shift = refresh(timeStamp);
		int numOfLastIntervals = (int) (lastMillis / unitLength_);
		if (shift >= counters_.length || numOfLastIntervals >= counters_.length) {
			return -1;
		}
		int sum = 0;
		for (int i = shift + 1; i < shift + numOfLastIntervals + 1; ++i) {
			sum += getCell(i, cellId);
		}
		return sum;
	}

	private int refresh(long timeStamp) {
		int shiftBy = (int) ((timeStamp - lastTimeUpdated_) / unitLength_);
		shift(shiftBy);
		int retVal;
		if (shiftBy > 0) {
			lastTimeUpdated_ = timeStamp / unitLength_ * unitLength_;
			retVal = 0;
		} else {
			retVal = -shiftBy;
		}
		return retVal;
	}

	public void put(int cellId, long timeStamp) {
		int shift = refresh(timeStamp);
		if (shift >= counters_.length) {
			return;
		}
		incrCell(shift, cellId);
	}
}
