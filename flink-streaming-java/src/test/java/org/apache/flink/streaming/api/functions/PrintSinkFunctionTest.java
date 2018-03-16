/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.functions;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkContextUtil;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import static org.junit.Assert.assertEquals;

/**
 * Tests for the {@link org.apache.flink.streaming.api.functions.sink.PrintSinkFunction}.
 */
public class PrintSinkFunctionTest {

	public PrintStream printStreamOriginal = System.out;
	private String line = System.lineSeparator();

	@Test
	public void testPrintSinkStdOut() throws Exception {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		PrintStream stream = new PrintStream(baos);
		System.setOut(stream);

		final StreamingRuntimeContext ctx = Mockito.mock(StreamingRuntimeContext.class);

		PrintSinkFunction<String> printSink = new PrintSinkFunction<>();
		printSink.setRuntimeContext(ctx);
		try {
			printSink.open(new Configuration());
		} catch (Exception e) {
			Assert.fail();
		}
		printSink.setTargetToStandardOut();
		printSink.invoke("hello world!", SinkContextUtil.forTimestamp(0));

		assertEquals("Print to System.out", printSink.toString());
		assertEquals("hello world!" + line, baos.toString());

		printSink.close();
		stream.close();
	}

	@Test
	public void testPrintSinkStdErr() throws Exception {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		PrintStream stream = new PrintStream(baos);
		System.setOut(stream);

		final StreamingRuntimeContext ctx = Mockito.mock(StreamingRuntimeContext.class);

		PrintSinkFunction<String> printSink = new PrintSinkFunction<>();
		printSink.setRuntimeContext(ctx);
		try {
			printSink.open(new Configuration());
		} catch (Exception e) {
			Assert.fail();
		}
		printSink.setTargetToStandardErr();
		printSink.invoke("hello world!", SinkContextUtil.forTimestamp(0));

		assertEquals("Print to System.err", printSink.toString());
		assertEquals("hello world!" + line, baos.toString());

		printSink.close();
		stream.close();
	}

	@Test
	public void testPrintSinkWithPrefix() throws Exception {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		PrintStream stream = new PrintStream(baos);
		System.setOut(stream);

		final StreamingRuntimeContext ctx = Mockito.mock(StreamingRuntimeContext.class);
		Mockito.when(ctx.getNumberOfParallelSubtasks()).thenReturn(2);
		Mockito.when(ctx.getIndexOfThisSubtask()).thenReturn(1);

		PrintSinkFunction<String> printSink = new PrintSinkFunction<>();
		printSink.setRuntimeContext(ctx);
		try {
			printSink.open(new Configuration());
		} catch (Exception e) {
			Assert.fail();
		}
		printSink.setTargetToStandardErr();
		printSink.invoke("hello world!", SinkContextUtil.forTimestamp(0));

		assertEquals("Print to System.err", printSink.toString());
		assertEquals("2> hello world!" + line, baos.toString());

		printSink.close();
		stream.close();
	}

	@After
	public void restoreSystemOut() {
		System.setOut(printStreamOriginal);
	}

}
