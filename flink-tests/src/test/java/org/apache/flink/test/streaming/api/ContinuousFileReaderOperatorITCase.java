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

package org.apache.flink.test.streaming.api;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.test.util.AbstractTestBase;

import org.junit.Test;

import java.io.File;
import java.io.PrintWriter;

import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.STRING_TYPE_INFO;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * Integration tests for {@link org.apache.flink.streaming.api.functions.source.ContinuousFileReaderOperator}.
 */
public class ContinuousFileReaderOperatorITCase extends AbstractTestBase {

	@Test
	public void testEndInput() throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		final File sourceFile = TEMPORARY_FOLDER.newFile();
		final int elementCount = 10000;
		try (PrintWriter printWriter = new PrintWriter(sourceFile)) {
			for (int i = 0; i < elementCount; i++) {
				printWriter.println(i);
			}
		}

		DataStreamSource<String> source = env.readTextFile(sourceFile.getAbsolutePath());

		// check the endInput is invoked at the right time
		TestBoundedOneInputStreamOperator checkingOperator = new TestBoundedOneInputStreamOperator(elementCount);
		DataStream<String> endInputChecking = source.transform("EndInputChecking", STRING_TYPE_INFO, checkingOperator);

		endInputChecking.addSink(new DiscardingSink<>());

		env.execute("ContinuousFileReaderOperatorITCase.testEndInput");
	}

	private static class TestBoundedOneInputStreamOperator extends AbstractStreamOperator<String>
		implements OneInputStreamOperator<String, String>, BoundedOneInput {

		private final int expectedProcessedElementCount;

		private boolean hasEnded = false;

		private int processedElementCount = 0;

		TestBoundedOneInputStreamOperator(int expectedProcessedElementCount) {
			// this operator must be chained with ContinuousFileReaderOperator
			// that way, this end input would be triggered after ContinuousFileReaderOperator
			chainingStrategy = ChainingStrategy.ALWAYS;
			this.expectedProcessedElementCount = expectedProcessedElementCount;
		}

		@Override
		public void endInput() throws Exception {
			assertEquals(expectedProcessedElementCount, processedElementCount);
			hasEnded = true;
		}

		@Override
		public void processElement(StreamRecord<String> element) throws Exception {
			assertFalse(hasEnded);
			output.collect(element);
			processedElementCount++;
		}
	}
}
