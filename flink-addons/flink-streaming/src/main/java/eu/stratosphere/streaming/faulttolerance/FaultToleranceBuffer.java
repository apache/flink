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

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.streaming.api.streamrecord.StreamRecord;

public abstract class FaultToleranceBuffer {

	private static final Log log = LogFactory.getLog(FaultToleranceBuffer.class);

	protected Map<String, StreamRecord> recordBuffer;
	protected Map<String, Long> recordTimestamps;
	protected SortedMap<Long, Set<String>> recordsByTime;

	protected int[] numberOfEffectiveChannels;
	protected int totalNumberOfEffectiveChannels;
	protected Long timeOfLastUpdate;
	protected String componentInstanceID;

	long timeout = 30000;

	public FaultToleranceBuffer(int[] numberOfChannels, String componentInstanceID) {
		this.numberOfEffectiveChannels = numberOfChannels;
		totalNumberOfEffectiveChannels = 0;
		for (int i : numberOfChannels) {
			totalNumberOfEffectiveChannels += i;
		}

		this.componentInstanceID = componentInstanceID;
		this.timeOfLastUpdate = System.currentTimeMillis();

		this.recordBuffer = new HashMap<String, StreamRecord>();
		this.recordsByTime = new TreeMap<Long, Set<String>>();
		this.recordTimestamps = new HashMap<String, Long>();
	}

	public synchronized void add(StreamRecord streamRecord) {

		StreamRecord record = streamRecord.copy();
		String id = record.getId();
		
		recordBuffer.put(id, record);

		addTimestamp(id);
		addToAckCounter(id);


		log.trace("Record added to buffer: " + id);
	}

	protected abstract void addToAckCounter(String id);

	protected abstract boolean removeFromAckCounter(String id);

	protected abstract void ack(String id, int channel);

	// TODO:count fails
	protected StreamRecord fail(String id) {
		if (recordBuffer.containsKey(id)) {
			StreamRecord newRecord = remove(id).setId(componentInstanceID);
			add(newRecord);
			return newRecord;
		} else {
			return null;
		}
	}

	protected abstract StreamRecord failChannel(String id, int channel);

	protected void addTimestamp(String id) {
		Long currentTime = System.currentTimeMillis();

		recordTimestamps.put(id, currentTime);
		
		Set<String> recordSet = recordsByTime.get(currentTime);

		if (recordSet == null) {
			recordSet = new HashSet<String>();
			recordsByTime.put(currentTime, recordSet);
		}

		recordSet.add(id);

	}

	public synchronized StreamRecord remove(String id) {

		if (removeFromAckCounter(id)) {
			
			recordsByTime.get(recordTimestamps.remove(id)).remove(id);
			
			log.trace("Record removed from buffer: " + id);
			return recordBuffer.remove(id);
		} else {
			log.warn("Record ALREADY REMOVED from buffer: " + id);
			return null;
		}

	}

	// TODO:test this
	public List<String> timeoutRecords(Long currentTime) {
		if (timeOfLastUpdate + timeout < currentTime) {
			log.trace("Updating record buffer");
			List<String> timedOutRecords = new LinkedList<String>();
			Map<Long, Set<String>> timedOut = recordsByTime.subMap(0L, currentTime - timeout);

			for (Set<String> recordSet : timedOut.values()) {
				if (!recordSet.isEmpty()) {
					for (String recordID : recordSet) {
						timedOutRecords.add(recordID);
					}
				}
			}

			for (String recordID : timedOutRecords) {
				fail(recordID);
			}

			timedOut.clear();

			timeOfLastUpdate = currentTime;
			return timedOutRecords;
		}
		return null;
	}

}
