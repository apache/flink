/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.test.io;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.Path;
import org.apache.flink.test.util.JavaProgramTestBase;

import java.io.IOException;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.junit.Assert.assertEquals;

/**
 * Tests for rich DataSource and DataSink input output formats accessing RuntimeContext by
 * checking accumulator values.
 */
public class RichInputOutputITCase extends JavaProgramTestBase {

	private String inputPath;
	private static ConcurrentLinkedQueue<Integer> readCalls;
	private static ConcurrentLinkedQueue<Integer> writeCalls;

	@Override
	protected void preSubmit() throws Exception {
		inputPath = createTempFile("input", "ab\n"
				+ "cd\n"
				+ "ef\n");
	}

	@Override
	protected void testProgram() throws Exception {
		// test verifying the number of records read and written vs the accumulator counts

		readCalls = new ConcurrentLinkedQueue<Integer>();
		writeCalls = new ConcurrentLinkedQueue<Integer>();
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.createInput(new TestInputFormat(new Path(inputPath))).output(new TestOutputFormat());

		JobExecutionResult result = env.execute();
		Object a = result.getAllAccumulatorResults().get("DATA_SOURCE_ACCUMULATOR");
		Object b = result.getAllAccumulatorResults().get("DATA_SINK_ACCUMULATOR");
		long recordsRead = (Long) a;
		long recordsWritten = (Long) b;
		assertEquals(recordsRead, readCalls.size());
		assertEquals(recordsWritten, writeCalls.size());
	}

	private static final class TestInputFormat extends TextInputFormat {
		private static final long serialVersionUID = 1L;

		private LongCounter counter = new LongCounter();

		public TestInputFormat(Path filePath) {
			super(filePath);
		}

		@Override
		public void initializeSplit(FileInputSplit split, Long offset) throws IOException {
			try {
				getRuntimeContext().addAccumulator("DATA_SOURCE_ACCUMULATOR", counter);
			} catch (UnsupportedOperationException e){
				// the accumulator is already added
			}
			super.initializeSplit(split, offset);
		}

		@Override
		public String nextRecord(String reuse) throws IOException{
			readCalls.add(1);
			counter.add(1);
			return super.nextRecord(reuse);
		}
	}

	private static final class TestOutputFormat extends RichOutputFormat<String> {

		private LongCounter counter = new LongCounter();

		@Override
		public void configure(Configuration parameters){}

		@Override
		public void open(int a, int b){
			try {
				getRuntimeContext().addAccumulator("DATA_SINK_ACCUMULATOR", counter);
			} catch (UnsupportedOperationException e){
				// the accumulator is already added
			}
		}

		@Override
		public void close() throws IOException{}

		@Override
		public void writeRecord(String record){
			writeCalls.add(1);
			counter.add(1);
		}
	}
}
