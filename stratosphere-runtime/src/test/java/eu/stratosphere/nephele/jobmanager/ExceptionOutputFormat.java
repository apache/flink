/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Apache Flink project (http://flink.incubator.apache.org)
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

package eu.stratosphere.nephele.jobmanager;

import java.io.IOException;

import org.apache.flink.api.common.io.InitializeOnMaster;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.StringRecord;


public class ExceptionOutputFormat implements OutputFormat<StringRecord>, InitializeOnMaster {

	private static final long serialVersionUID = 1L;
	
	/**
	 * The message which is used for the test runtime exception.
	 */
	public static final String RUNTIME_EXCEPTION_MESSAGE = "This is a test runtime exception";

	@Override
	public void configure(Configuration parameters) {}

	@Override
	public void open(int taskNumber, int numTasks) {}

	@Override
	public void writeRecord(StringRecord record) {}

	@Override
	public void close() {}

	@Override
	public void initializeGlobal(int parallelism) throws IOException {
		throw new RuntimeException(RUNTIME_EXCEPTION_MESSAGE);
	}
}
