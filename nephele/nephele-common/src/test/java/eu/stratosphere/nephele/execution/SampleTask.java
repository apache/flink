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

package eu.stratosphere.nephele.execution;

import java.io.IOException;

import eu.stratosphere.nephele.io.RecordReader;
import eu.stratosphere.nephele.io.RecordWriter;
import eu.stratosphere.nephele.template.AbstractTask;
import eu.stratosphere.nephele.types.IntegerRecord;

/**
 * This is a sample task which is used during the unit tests for the {@link Environment} class and its components.
 * 
 * @author warneke
 */
public class SampleTask extends AbstractTask {

	/**
	 * The number of record readers (i.e. input gates) that this task has.
	 */
	public static final int NUMBER_OF_RECORD_READER = 2;

	/**
	 * The number of record writers (i.e. output gates) that this task has.
	 */
	public static final int NUMBER_OF_RECORD_WRITER = 1;

	/**
	 * The configuration key for the scenario "Exception".
	 */
	public static final String EXCEPTION_SCENARIO_KEY = "expection";

	/**
	 * The configuration key for the scenario "User Abort".
	 */
	public static final String USER_ABORT_SCENARIO_KEY = "userAbort";

	/**
	 * The maximum periode of time to sleep in the "User Abort" scenario.
	 */
	private static final long MAX_WAIT_TIME = 1000;

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void registerInputOutput() {

		new RecordReader<IntegerRecord>(this, IntegerRecord.class, null);
		new RecordReader<IntegerRecord>(this, IntegerRecord.class, null);

		new RecordWriter<IntegerRecord>(this, IntegerRecord.class);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void invoke() throws Exception {

		if (getRuntimeConfiguration().getBoolean(EXCEPTION_SCENARIO_KEY, false)) {
			throw new IOException("Test Exception");
		}

		if (getRuntimeConfiguration().getBoolean(USER_ABORT_SCENARIO_KEY, false)) {

			if (Thread.interrupted()) {
				throw new InterruptedException("Interrupted by user");
			}
			Thread.sleep(MAX_WAIT_TIME); // The user aborting it expected to result in an InterruptedException
		}
	}

}
