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

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import eu.stratosphere.nephele.io.RecordWriter;

/**
 * An object to provide fault tolerance for Stratosphere stream processing. It
 * works as a buffer to hold StreamRecords for a task for re-emitting failed, or
 * timed out records.
 */
public class FaultToleranceBuffer {

	private long TIMEOUT = 1000;

	private Long timeOfLastUpdate;
	private Map<String, StreamRecord> recordBuffer;
	private Map<String, Integer> ackCounter;
	private SortedMap<Long, Set<String>> recordsByTime;
	private Map<String, Long> recordTimestamps;

	private List<RecordWriter<StreamRecord>> outputs;
	private final String channelID;

	private int numberOfOutputs;

	/**
	 * Creates fault tolerance buffer object for the given output channels and
	 * channel ID
	 * 
	 * @param outputs
	 *          List of outputs
	 * @param channelID
	 *          ID of the task object that uses this buffer
	 */
	public FaultToleranceBuffer(List<RecordWriter<StreamRecord>> outputs,
			String channelID) {
		this.timeOfLastUpdate = System.currentTimeMillis();
		this.outputs = outputs;
		this.recordBuffer = new HashMap<String, StreamRecord>();
		this.ackCounter = new HashMap<String, Integer>();
		this.numberOfOutputs = outputs.size();
		this.channelID = channelID;
		this.recordsByTime = new TreeMap<Long, Set<String>>();
		this.recordTimestamps = new HashMap<String, Long>();
	}

	/**
	 * Adds the record to the fault tolerance buffer. This record will be
	 * monitored for acknowledgements and timeout.
	 * 
	 */
	public void addRecord(StreamRecord streamRecord) {

		recordBuffer.put(streamRecord.getId(), streamRecord);
		ackCounter.put(streamRecord.getId(), numberOfOutputs);
		addTimestamp(streamRecord.getId());
	}

	/**
	 * Checks for records that have timed out since the last check and fails them.
	 * 
	 * @param currentTime
	 *          Time when the check should be made, usually current system time.
	 * @return Returns the list of the records that have timed out.
	 */
	List<String> timeoutRecords(Long currentTime) {
		if (timeOfLastUpdate + TIMEOUT < currentTime) {
			List<String> timedOutRecords = new LinkedList<String>();
			Map<Long, Set<String>> timedOut = recordsByTime.subMap(0L, currentTime
					- TIMEOUT);

			for (Set<String> recordSet : timedOut.values()) {
				if (!recordSet.isEmpty()) {
					for (String recordID : recordSet) {
						timedOutRecords.add(recordID);
					}
				}
			}

			recordsByTime.keySet().removeAll(timedOut.keySet());
			for (String recordID : timedOutRecords) {
				failRecord(recordID);
			}

			timeOfLastUpdate = currentTime;
			return timedOutRecords;
		}
		return null;
	}

	/**
	 * Stores time stamp for a record by recordID and also adds the record to a
	 * map which maps a time stamp to the IDs of records that were emitted at that
	 * time.
	 * <p>
	 * Later used for timeouts.
	 * 
	 * @param recordID
	 *          ID of the record
	 */
	public void addTimestamp(String recordID) {
		Long currentTime = System.currentTimeMillis();
		recordTimestamps.put(recordID, currentTime);

		if (recordsByTime.containsKey(currentTime)) {
			recordsByTime.get(currentTime).add(recordID);
		} else {
			Set<String> recordSet = new HashSet<String>();
			recordSet.add(recordID);
			recordsByTime.put(currentTime, recordSet);
		}
	}

	/**
	 * Returns a StreamRecord after removing it from the buffer
	 * 
	 * @param recordID
	 *          The ID of the record that will be popped
	 */
	public StreamRecord popRecord(String recordID) {
		System.out.println("Pop ID: " + recordID);
		StreamRecord record = recordBuffer.get(recordID);
		removeRecord(recordID);
		return record;
	}

	/**
	 * Removes a StreamRecord by ID from the fault tolerance buffer, further acks
	 * will have no effects for this record.
	 * 
	 * @param recordID
	 *          The ID of the record that will be removed
	 * 
	 */
	void removeRecord(String recordID) {
		recordBuffer.remove(recordID);
		ackCounter.remove(recordID);
		try {
			Long ts = recordTimestamps.remove(recordID);
			recordsByTime.get(ts).remove(recordID);
		} catch (NullPointerException e) {

		} catch (Exception e) {
			e.printStackTrace();
			System.out.println(recordID);
		}
	}

	/**
	 * Acknowledges the record of the given ID, if all the outputs have sent
	 * acknowledgments, removes it from the buffer
	 * 
	 * @param recordID
	 *          ID of the record that has been acknowledged
	 */
	// TODO: find a place to call timeoutRecords
	public void ackRecord(String recordID) {
		if (ackCounter.containsKey(recordID)) {
			int ackCount = ackCounter.get(recordID) - 1;

			if (ackCount == 0) {
				removeRecord(recordID);
			} else {
				ackCounter.put(recordID, ackCount);
			}
		}
		// timeoutRecords(System.currentTimeMillis());
	}

	/**
	 * Re-emits the failed record for the given ID, removes the old record and
	 * stores it with a new ID.
	 * 
	 * @param recordID
	 *          ID of the record that has been failed
	 */
	public void failRecord(String recordID) {
		// Create new id to avoid double counting acks
		System.out.println("Fail ID: " + recordID);
		StreamRecord newRecord = popRecord(recordID).setId(channelID);
		addRecord(newRecord);
		reEmit(newRecord);
	}

	/**
	 * Emit give record to all output channels
	 * 
	 * @param record
	 *          Record to be re-emitted
	 */
	public void reEmit(StreamRecord record) {
		for (RecordWriter<StreamRecord> output : outputs) {
			try {
				output.emit(record);
				System.out.println("Re-emitted");
			} catch (Exception e) {
				System.out.println("Re-emit failed");
			}
		}

	}

	public long getTIMEOUT() {
		return this.TIMEOUT;
	}

	public void setTIMEOUT(long TIMEOUT) {
		this.TIMEOUT = TIMEOUT;
	}

	public Map<String, StreamRecord> getRecordBuffer() {
		return this.recordBuffer;
	}

	public Long getTimeOfLastUpdate() {
		return this.timeOfLastUpdate;
	}

	public Map<String, Integer> getAckCounter() {
		return this.ackCounter;
	}

	public SortedMap<Long, Set<String>> getRecordsByTime() {
		return this.recordsByTime;
	}

	public Map<String, Long> getRecordTimestamps() {
		return this.recordTimestamps;
	}

	public List<RecordWriter<StreamRecord>> getOutputs() {
		return this.outputs;
	}

	public String getChannelID() {
		return this.channelID;
	}

	public int getNumberOfOutputs() {
		return this.numberOfOutputs;
	}

	void setNumberOfOutputs(int numberOfOutputs) {
		this.numberOfOutputs = numberOfOutputs;
	}

}
