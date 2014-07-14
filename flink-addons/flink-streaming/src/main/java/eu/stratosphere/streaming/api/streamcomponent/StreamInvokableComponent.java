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

package eu.stratosphere.streaming.api.streamcomponent;

import java.io.Serializable;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.io.RecordWriter;
import eu.stratosphere.streaming.api.streamrecord.StreamRecord;
import eu.stratosphere.streaming.faulttolerance.FaultToleranceType;
import eu.stratosphere.streaming.faulttolerance.FaultToleranceUtil;
import eu.stratosphere.streaming.util.PerformanceCounter;

public abstract class StreamInvokableComponent implements Serializable {

	private static final long serialVersionUID = 1L;

	private static final Log log = LogFactory.getLog(StreamInvokableComponent.class);

	private List<RecordWriter<StreamRecord>> outputs;

	protected int channelID;
	protected String name;
	private FaultToleranceUtil emittedRecords;
	protected PerformanceCounter performanceCounter;
	private boolean useFaultTolerance;

	public final void declareOutputs(List<RecordWriter<StreamRecord>> outputs, int channelID,
			String name, FaultToleranceUtil emittedRecords, FaultToleranceType faultToleranceType) {
		this.outputs = outputs;
		this.channelID = channelID;
		this.emittedRecords = emittedRecords;
		this.name = name;
		this.performanceCounter = new PerformanceCounter("pc", 1000, 1000, 30000,
				"/home/strato/stratosphere-distrib/log/counter/" + name + channelID);
		this.useFaultTolerance = faultToleranceType != FaultToleranceType.NONE;
	}

	public final void setPerfCounterDir(String dir) {
		performanceCounter.setFname(dir + "/" + name + channelID);
	}

	public final void emit(StreamRecord record) {
		record.setId(channelID);

		if (useFaultTolerance) {
			emittedRecords.addRecord(record);
		}

		try {
			for (RecordWriter<StreamRecord> output : outputs) {
				output.emit(record);
				output.flush();
				log.info("EMITTED: " + record.getId() + " -- " + name);
			}
		} catch (Exception e) {
			if (useFaultTolerance) {
				emittedRecords.failRecord(record.getId());
			}

			log.warn("FAILED: " + record.getId() + " -- " + name + " -- due to "
					+ e.getClass().getSimpleName());
		}
	}

	// TODO: Should we fail record at exception catch?
	public final void emit(StreamRecord record, int outputChannel) {
		record.setId(channelID);
		if (useFaultTolerance) {
			emittedRecords.addRecord(record, outputChannel);
		}
		try {
			outputs.get(outputChannel).emit(record);
		} catch (Exception e) {
			log.warn("EMIT ERROR: " + e.getClass().getSimpleName() + " -- " + name);
		}
	}

	public String getResult() {
		return "Override getResult() to pass your own results";
	}
}
