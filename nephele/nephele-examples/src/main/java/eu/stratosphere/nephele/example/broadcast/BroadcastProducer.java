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

package eu.stratosphere.nephele.example.broadcast;

import eu.stratosphere.nephele.io.BroadcastRecordWriter;
import eu.stratosphere.nephele.template.AbstractFileInputTask;

/**
 * This is a sample producer task for the broadcast test job.
 * 
 * @author warneke
 */
public class BroadcastProducer extends AbstractFileInputTask {

	/**
	 * The configuration key to adjust be number of records to be emitted by this task.
	 */
	public static final String NUMBER_OF_RECORDS_KEY = "broadcast.record.number";

	/**
	 * The default number of records emitted by this task.
	 */
	private static final int DEFAULT_NUMBER_OF_RECORDS = (1024 * 1024 * 1024);

	/**
	 * The broadcast record writer the records will be emitted through.
	 */
	private BroadcastRecordWriter<BroadcastRecord> output;

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void registerInputOutput() {

		this.output = new BroadcastRecordWriter<BroadcastRecord>(this, BroadcastRecord.class);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void invoke() throws Exception {

		// Determine number of records to emit
		final int numberOfRecordsToEmit = getRuntimeConfiguration().getInteger(NUMBER_OF_RECORDS_KEY,
			DEFAULT_NUMBER_OF_RECORDS);

		// Create and prepare record
		final BroadcastRecord record = new BroadcastRecord();
		for (int i = 0; i < record.getSize(); i++) {
			record.setData(i, (byte) i);
		}

		// Emit record over and over
		for (int i = 0; i < numberOfRecordsToEmit; i++) {

			this.output.emit(record);
		}
	}

}
