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

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.io.RecordWriter;
import eu.stratosphere.streaming.api.streamrecord.StreamRecord;
import eu.stratosphere.streaming.api.streamrecord.UID;
import eu.stratosphere.streaming.util.PerformanceCounter;
import eu.stratosphere.streaming.util.PerformanceTracker;

/**
 * An object to provide fault tolerance for Stratosphere stream processing. It
 * works as a buffer to hold StreamRecords for a task for re-emitting failed, or
 * timed out records.
 */
public class FaultToleranceUtil {

	private static final Log log = LogFactory.getLog(FaultToleranceUtil.class);

	private List<RecordWriter<StreamRecord>> outputs;
	private final int componentID;

	private int numberOfChannels;
	
	private FaultToleranceBuffer buffer;
	public FaultToleranceType type;
	public PerformanceTracker tracker;
	public PerformanceCounter counter;

	/**
	 * Creates fault tolerance buffer object for the given output channels and
	 * channel ID
	 * 
	 * @param outputs
	 *            List of outputs
	 * @param sourceInstanceID
	 *            ID of the task object that uses this buffer
	 * @param numberOfChannels
	 *            Number of output channels for the output components
	 */
	// TODO:update logs for channel
	// acks and fails
	public FaultToleranceUtil(FaultToleranceType type, List<RecordWriter<StreamRecord>> outputs, int sourceInstanceID,
			int[] numberOfChannels) {
		this.outputs = outputs;

		this.componentID = sourceInstanceID;
		
		this.type = type;
		
		switch (type) {
		case EXACTLY_ONCE:
			this.buffer = new ExactlyOnceFaultToleranceBuffer(numberOfChannels, sourceInstanceID);
			break;
		case AT_LEAST_ONCE: case NONE: default:
			this.buffer = new AtLeastOnceFaultToleranceBuffer(numberOfChannels, sourceInstanceID);		
		}

		tracker = new PerformanceTracker("pc", 1000, 1000, 30000,
				"/home/strato/stratosphere-distrib/log/counter/Buffer" + sourceInstanceID);
		counter = new PerformanceCounter("pc", 1000, 1000, 30000,
				"/home/strato/stratosphere-distrib/log/counter/Emitted" + sourceInstanceID);

	}

	public FaultToleranceUtil(FaultToleranceType type, List<RecordWriter<StreamRecord>> outputs,
			int sourceInstanceID, String componentName, int[] numberOfChannels) {
		this.outputs = outputs;
		this.componentID = sourceInstanceID;

		switch (type) {
		case AT_LEAST_ONCE:
		default:
			this.buffer = new AtLeastOnceFaultToleranceBuffer(numberOfChannels, sourceInstanceID);
			break;
		case EXACTLY_ONCE:
			this.buffer = new ExactlyOnceFaultToleranceBuffer(numberOfChannels, sourceInstanceID);
			break;
		}

		tracker = new PerformanceTracker("pc", 1000, 1000, 10000,
				"/home/strato/stratosphere-distrib/log/counter/Buffer" + componentName
						+ sourceInstanceID);
		counter = new PerformanceCounter("pc", 1000, 1000, 10000,
				"/home/strato/stratosphere-distrib/log/counter/Emitted" + componentName
						+ sourceInstanceID);
	}

	/**
	 * Adds the record to the fault tolerance buffer. This record will be
	 * monitored for acknowledgements and timeout.
	 * 
	 * @param streamRecord
	 *            Record to add
	 */
	public void addRecord(StreamRecord streamRecord) {

		buffer.add(streamRecord);
		tracker.track(this.buffer.recordBuffer.size());
		counter.count();
	}

	public void addRecord(StreamRecord streamRecord, int output) {

		buffer.add(streamRecord, output);

	}

	/**
	 * Acknowledges the record of the given ID, if all the outputs have sent
	 * acknowledgments, removes it from the buffer
	 * 
	 * @param recordID
	 *            ID of the record that has been acknowledged
	 * @param channel
	 *            Number of channel to be acked
	 * 
	 */
	// TODO: find a place to call timeoutRecords
	public void ackRecord(UID recordID, int channel) {
		buffer.ack(recordID, channel);
		tracker.track(this.buffer.recordBuffer.size());
	}

	/**
	 * Re-emits the failed record for the given ID, removes the old record and
	 * stores it with a new ID.
	 * 
	 * @param recordID
	 *            ID of the record that has been failed
	 * @param channel
	 *            Number of channel to be failed
	 */
	public void failRecord(UID recordID, int channel) {
		// if by ft type
		if (type == FaultToleranceType.EXACTLY_ONCE) {
			StreamRecord failed = buffer.failChannel(recordID, channel);

			if (failed != null) {
				reEmit(failed, channel);
			}
		} else {
			failRecord(recordID);
		}
	}

	/**
	 * Re-emits the failed record for the given ID, removes the old record and
	 * stores it with a new ID.
	 * 
	 * @param uid
	 *            ID of the record that has been failed
	 */
	public void failRecord(UID uid) {
		StreamRecord failed = buffer.fail(uid);

		if (failed != null) {

			reEmit(failed);

		}

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
				log.warn("RE-EMITTED: " + record.getId() + " " + outputChannel);
			} catch (Exception e) {
				log.error("RE-EMIT FAILED, avoiding record: " + record.getId());
			}
		}

	}

	public List<RecordWriter<StreamRecord>> getOutputs() {
		return this.outputs;
	}

	public int getChannelID() {
		return this.componentID;
	}

	public int getNumberOfOutputs() {
		return this.numberOfChannels;
	}

	void setNumberOfOutputs(int numberOfOutputs) {
		this.numberOfChannels = numberOfOutputs;
	}

}
