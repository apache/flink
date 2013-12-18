/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.nephele.example.broadcast;

import eu.stratosphere.nephele.io.BroadcastRecordWriter;
import eu.stratosphere.nephele.template.AbstractGenericInputTask;

/**
 * This is a sample producer task for the broadcast test job.
 * 
 * @author warneke
 */
public class BroadcastProducer extends AbstractGenericInputTask {

	/**
	 * The configuration key to adjust be number of records to be emitted by this task.
	 */
	public static final String NUMBER_OF_RECORDS_KEY = "broadcast.record.number";

	/**
	 * The default number of records emitted by this task.
	 */
	private static final int DEFAULT_NUMBER_OF_RECORDS = (1024 * 1024 * 1024);

	/**
	 * The configuration key to set the run of the task.
	 */
	public static final String RUN_KEY = "broadcast.run";

	/**
	 * The default run on the task.
	 */
	public static final int DEFAULT_RUN = 0;

	/**
	 * The interval in which records with time stamps shall be sent.
	 */
	public static final int TIMESTAMP_INTERVAL = 100000;

	/**
	 * The broadcast record writer the records will be emitted through.
	 */
	private BroadcastRecordWriter<BroadcastRecord> output;


	@Override
	public void registerInputOutput() {

		this.output = new BroadcastRecordWriter<BroadcastRecord>(this, BroadcastRecord.class);
	}


	@Override
	public void invoke() throws Exception {

		// Determine number of records to emit
		final int numberOfRecordsToEmit = getTaskConfiguration().getInteger(NUMBER_OF_RECORDS_KEY,
			DEFAULT_NUMBER_OF_RECORDS);

		// Create and prepare record
		final BroadcastRecord record = new BroadcastRecord();

		// Emit record over and over
		for (int i = 0; i < numberOfRecordsToEmit; i++) {

			// Set the current time as timestamp every TIMESTAMP_INTERVAL times
			if ((i % TIMESTAMP_INTERVAL) == 0) {
				record.setTimestamp(System.currentTimeMillis());
			}

			this.output.emit(record);
		}
	}

}
