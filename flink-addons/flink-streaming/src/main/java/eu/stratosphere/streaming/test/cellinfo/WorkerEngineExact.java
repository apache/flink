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

package eu.stratosphere.streaming.test.cellinfo;

import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

public class WorkerEngineExact implements java.io.Serializable, IWorkerEngine {
	private static final long serialVersionUID = 1L;
	private long lastTimeUpdated_;
	private long bufferInterval_;
	private TreeMap<Long, Integer>[] counters_;

	@SuppressWarnings("unchecked")
	public WorkerEngineExact(int numOfCells, int bufferInterval,
			long currentTime) {
		lastTimeUpdated_ = currentTime;
		bufferInterval_ = bufferInterval;
		counters_ = new TreeMap[numOfCells];
		for (int i = 0; i < numOfCells; ++i) {
			counters_[i] = new TreeMap<Long, Integer>();
		}
	}

	public int get(long timeStamp, long lastMillis, int cellId) {
		refresh(timeStamp);
		Map<Long, Integer> subMap = counters_[cellId].subMap(timeStamp
				- lastMillis, true, timeStamp, false);
		int retVal = 0;
		for (Map.Entry<Long, Integer> entry : subMap.entrySet()) {
			retVal += entry.getValue();
		}
		return retVal;
	}

	public void put(int cellId, long timeStamp) {
		refresh(timeStamp);
		TreeMap<Long, Integer> map = counters_[cellId];
		// System.out.println(map.size());
		if (map.containsKey(timeStamp)) {
			map.put(timeStamp, map.get(timeStamp) + 1);
		} else {
			map.put(timeStamp, 1);
		}
	}

	public void refresh(long timeStamp) {
		if (timeStamp - lastTimeUpdated_ > bufferInterval_) {
			// System.out.println("Refresh at " + timeStamp);
			for (int i = 0; i < counters_.length; ++i) {
				for (Iterator<Map.Entry<Long, Integer>> it = counters_[i]
						.entrySet().iterator(); it.hasNext();) {
					Map.Entry<Long, Integer> entry = it.next();
					long time = entry.getKey();
					if (timeStamp - time > bufferInterval_) {
						// System.out.println("Remove: " + i + "_" + time +
						// " at " +
						// timeStamp);
						it.remove();
					} else {
						break;
					}
				}
			}
			lastTimeUpdated_ = timeStamp;
		}
	}
}
