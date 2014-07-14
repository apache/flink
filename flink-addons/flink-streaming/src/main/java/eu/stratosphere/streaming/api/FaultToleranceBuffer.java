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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.io.RecordWriter;
import eu.stratosphere.streaming.api.streamrecord.StreamRecord;

/**
 * An object to provide fault tolerance for Stratosphere stream processing. It
 * works as a buffer to hold StreamRecords for a task for re-emitting failed, or
 * timed out records.
 */
public class FaultToleranceBuffer {

	private static final Log log = LogFactory
			.getLog(FaultToleranceBuffer.class);
	private long TIMEOUT = 10000;
	private Long timeOfLastUpdate;
	private Map<String, StreamRecord> recordBuffer;
	private Map<String, Integer> ackCounter;
	private Map<String, int[]> ackMap;
	private SortedMap<Long, Set<String>> recordsByTime;
	private Map<String, Long> recordTimestamps;

	private List<RecordWriter<StreamRecord>> outputs;
	private final String channelID;

	private int numberOfOutputs;
	private int[] numberOfOutputChannels;

	/**
	 * Creates fault tolerance buffer object for the given output channels and
	 * channel ID
	 * 
	 * @param outputs
	 *            List of outputs
	 * @param channelID
	 *            ID of the task object that uses this buffer
	 * @param numberOfChannels
	 *            Number of output channels for the output components
	 */
	
	public FaultToleranceBuffer(List<RecordWriter<StreamRecord>> outputs,
			String channelID, int[] numberOfChannels) {
		this.timeOfLastUpdate = System.currentTimeMillis();
		this.outputs = outputs;
		this.recordBuffer = new HashMap<String, StreamRecord>();
		this.ackCounter = new HashMap<String, Integer>();
		this.ackMap = new HashMap<String, int[]>();
		this.numberOfOutputChannels=numberOfChannels;
		
		int totalChannels = 0;

		for (int i : numberOfChannels)
			totalChannels += i;
		
		this.numberOfOutputs = totalChannels;
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
		String id = streamRecord.getId();
		recordBuffer.put(id, streamRecord.copy());
		ackCounter.put(id, numberOfOutputs);
		
		ackMap.put(id,numberOfOutputChannels.clone());
		
		addTimestamp(id);
		log.trace("Record added to buffer: " + id);
	}

	/**
	 * Checks for records that have timed out since the last check and fails
	 * them.
	 * 
	 * @param currentTime
	 *            Time when the check should be made, usually current system
	 *            time.
	 * @return Returns the list of the records that have timed out.
	 */
	List<String> timeoutRecords(Long currentTime) {
		if (timeOfLastUpdate + TIMEOUT < currentTime) {
			log.trace("Updating record buffer");
			List<String> timedOutRecords = new LinkedList<String>();
			Map<Long, Set<String>> timedOut = recordsByTime.subMap(0L,
					currentTime - TIMEOUT);

			for (Set<String> recordSet : timedOut.values()) {
				if (!recordSet.isEmpty()) {
					for (String recordID : recordSet) {
						timedOutRecords.add(recordID);
					}
				}
			}

			for (String recordID : timedOutRecords) {
				failRecord(recordID);
			}

			timedOut.clear();

			timeOfLastUpdate = currentTime;
			return timedOutRecords;
		}
		return null;
	}

	/**
	 * Stores time stamp for a record by recordID and also adds the record to a
	 * map which maps a time stamp to the IDs of records that were emitted at
	 * that time.
	 * <p>
	 * Later used for timeouts.
	 * 
	 * @param recordID
	 *            ID of the record
	 */
	public void addTimestamp(String recordID) {
		Long currentTime = System.currentTimeMillis();
		recordTimestamps.put(recordID, currentTime);

		Set<String> recordSet = recordsByTime.get(currentTime);

		if (recordSet != null) {
			recordSet.add(recordID);
		} else {
			recordSet = new HashSet<String>();
			recordSet.add(recordID);
			recordsByTime.put(currentTime, recordSet);
		}
	}

	/**
	 * Removes a StreamRecord by ID from the fault tolerance buffer, further
	 * acks will have no effects for this record.
	 * 
	 * @param recordID
	 *            The ID of the record that will be removed
	 * 
	 */
	public StreamRecord removeRecord(String recordID) {
		ackCounter.remove(recordID);

		recordsByTime.get(recordTimestamps.remove(recordID)).remove(recordID);

		log.trace("Record removed from buffer: " + recordID);

		return recordBuffer.remove(recordID);
	}

	/**
	 * Acknowledges the record of the given ID, if all the outputs have sent
	 * acknowledgments, removes it from the buffer
	 * 
	 * @param recordID
	 *            ID of the record that has been acknowledged
	 */
	// TODO: find a place to call timeoutRecords
	public void ackRecord(String recordID) {
		if (ackCounter.containsKey(recordID)) {
			Integer ackCount = ackCounter.get(recordID)-1;
			if (ackCount == 0) {
				removeRecord(recordID);
			} else {
				ackCounter.put(recordID, ackCount);
			}
		}
	}

	/**
	 * Acknowledges the record of the given ID from one output, if all the
	 * outputs have sent acknowledgments, removes it from the buffer
	 * 
	 * @param recordID
	 *            ID of the record that has been acknowledged
	 * 
	 * @param outputChannel
	 *            Number of the output channel that sent the ack
	 */
	public void ackRecord(String recordID, int outputChannel) {

		if (ackMap.containsKey(recordID)) {
			int[] acks = ackMap.get(recordID);
			acks[outputChannel]--;

			if (allZero(acks)) {
				removeRecord(recordID);
			}

		}
	}

	/**
	 * Checks whether an int array contains only zeros.
	 * @param values
	 * The array to check
	 * @return
	 * true only if the array contains only zeros
	 */
	private static boolean allZero(int[] values) {
		for (int value : values) {
			if (value!=0)
				return false;
		}
		return true;
	}

	/**
	 * Re-emits the failed record for the given ID, removes the old record and
	 * stores it with a new ID.
	 * 
	 * @param recordID
	 *            ID of the record that has been failed
	 */
	public void failRecord(String recordID) {
		// Create new id to avoid double counting acks
		StreamRecord newRecord = removeRecord(recordID).setId(channelID);
		addRecord(newRecord);
		reEmit(newRecord);
	}

	/**
	 * Re-emits the failed record for the given ID to a specific output with a
	 * new id
	 * 
	 * @param recordID
	 *            ID of the record that has been failed
	 * @param outputChannel
	 *            Number of the output channel
	 */
	public void failRecord(String recordID, int outputChannel) {
		// TODO: Implement functionality
	}

	/**
	 * Emit give record to all output channels
	 * 
	 * @param record
	 *            Record to be re-emitted
	 */
	public void reEmit(StreamRecord record) {
		for (RecordWriter<StreamRecord> output : outputs) {
			try {
				output.emit(record);
				log.warn("RE-EMITTED: " + record.getId());
			} catch (Exception e) {
				log.error("RE-EMIT FAILED, avoiding record: " + record.getId());
			}
		}

	}

	/**
	 * Emit give record to a specific output, added for exactly once processing
	 * 
	 * @param record
	 *            Record to be re-emitted
	 * @param outputChannel
	 *            Number of the output channel
	 */
	public void reEmit(StreamRecord record, int outputChannel) {
		{
			try {
				outputs.get(outputChannel).emit(record);
				log.warn("RE-EMITTED: " + record.getId());
			} catch (Exception e) {
				log.error("RE-EMIT FAILED, avoiding record: " + record.getId());
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
