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

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.streaming.api.streamrecord.StreamRecord;
import eu.stratosphere.streaming.api.streamrecord.UID;

public abstract class FaultToleranceBuffer {

	private static final Log log = LogFactory.getLog(FaultToleranceBuffer.class);

	protected Map<UID, StreamRecord> recordBuffer;
	protected Map<UID, Long> recordTimestamps;
	protected SortedMap<Long, Set<UID>> recordsByTime;

	protected int[] numberOfEffectiveChannels;
	protected int totalNumberOfEffectiveChannels;
	protected Long timeOfLastUpdate;
	protected int componentInstanceID;

	long timeout = 30000;

	public FaultToleranceBuffer(int[] numberOfChannels, int componentInstanceID) {
		this.numberOfEffectiveChannels = numberOfChannels;
		totalNumberOfEffectiveChannels = 0;
		for (int i : numberOfChannels) {
			totalNumberOfEffectiveChannels += i;
		}

		this.componentInstanceID = componentInstanceID;
		this.timeOfLastUpdate = System.currentTimeMillis();

		this.recordBuffer = new ConcurrentHashMap<UID, StreamRecord>();
		this.recordsByTime = new ConcurrentSkipListMap<Long, Set<UID>>();
		this.recordTimestamps = new ConcurrentHashMap<UID, Long>();
	}

	public void add(StreamRecord streamRecord) {

		StreamRecord record = streamRecord.copy();
		UID id = record.getId();

		recordBuffer.put(id, record);

		addTimestamp(id);
		addToAckCounter(id);

		if (log.isTraceEnabled()) {
			log.trace("Record added to buffer: " + id);
		}
	}

	public void add(StreamRecord streamRecord, int channel) {

		StreamRecord record = streamRecord.copy();

		UID id = record.getId();
		recordBuffer.put(id, record);
		addTimestamp(id);

		addToAckCounter(id, channel);

		if (log.isTraceEnabled()) {
			log.trace("Record added to buffer: " + id);
		}
	}

	protected abstract void addToAckCounter(UID id);

	protected void addToAckCounter(UID id, int channel) {
		addToAckCounter(id);
	}

	protected abstract boolean removeFromAckCounter(UID uid);

	protected abstract void ack(UID id, int channel);

	// TODO:count fails
	protected StreamRecord fail(UID uid) {
		if (recordBuffer.containsKey(uid)) {
			StreamRecord newRecord = remove(uid).setId(componentInstanceID);
			add(newRecord);
			return newRecord;
		} else {
			return null;
		}
	}

	protected abstract StreamRecord failChannel(UID id, int channel);

	protected void addTimestamp(UID id) {
		Long currentTime = System.currentTimeMillis();

		recordTimestamps.put(id, currentTime);

		Set<UID> recordSet = recordsByTime.get(currentTime);

		if (recordSet == null) {
			recordSet = new HashSet<UID>();
			recordsByTime.put(currentTime, recordSet);
		}

		recordSet.add(id);

	}

	public StreamRecord remove(UID uid) {

		if (removeFromAckCounter(uid)) {

			recordsByTime.get(recordTimestamps.remove(uid)).remove(uid);

			if (log.isTraceEnabled()) {
				log.trace("Record removed from buffer: " + uid);
			}
			return recordBuffer.remove(uid);
		} else {
			if (log.isWarnEnabled()) {
				log.warn("Record ALREADY REMOVED from buffer: " + uid);
			}
			return null;
		}

	}

	// TODO:test this
	public List<UID> timeoutRecords(Long currentTime) {
		if (timeOfLastUpdate + timeout < currentTime) {
			if (log.isTraceEnabled()) {
				log.trace("Updating record buffer");
			}
			List<UID> timedOutRecords = new LinkedList<UID>();
			Map<Long, Set<UID>> timedOut = recordsByTime.subMap(0L, currentTime - timeout);

			for (Set<UID> recordSet : timedOut.values()) {
				if (!recordSet.isEmpty()) {
					for (UID recordID : recordSet) {
						timedOutRecords.add(recordID);
					}
				}
			}

			for (UID recordID : timedOutRecords) {
				fail(recordID);
			}

			timedOut.clear();

			timeOfLastUpdate = currentTime;
			return timedOutRecords;
		}
		return null;
	}

}
