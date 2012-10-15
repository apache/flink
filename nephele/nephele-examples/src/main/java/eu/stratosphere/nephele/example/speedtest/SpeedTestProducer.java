/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
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

package eu.stratosphere.nephele.example.speedtest;

import eu.stratosphere.nephele.io.RecordWriter;
import eu.stratosphere.nephele.template.AbstractGenericInputTask;

/**
 * This class implements the producer task which produces test records for the speed test.
 * 
 * @author warneke
 */
public final class SpeedTestProducer extends AbstractGenericInputTask {

	/**
	 * The record writer to emit the produced records.
	 */
	private RecordWriter<SpeedTestRecord> writer;

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void registerInputOutput() {

		this.writer = new RecordWriter<SpeedTestRecord>(this, SpeedTestRecord.class);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void invoke() throws Exception {

		// Determine the amount of data to send per subtask
		final int dataVolumePerSubtaskInMB = getTaskConfiguration().getInteger(SpeedTest.DATA_VOLUME_CONFIG_KEY, 1)
			* 1024 / getCurrentNumberOfSubtasks();

		final long numberOfRecordsToEmit = (long) dataVolumePerSubtaskInMB * 1024L * 1024L
			/ (long) SpeedTestRecord.RECORD_SIZE;

		final SpeedTestRecord record = new SpeedTestRecord();

		for (long i = 0; i < numberOfRecordsToEmit; ++i) {
			this.writer.emit(record);
		}
	}

}
