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
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.runtime.tasks.StreamingRuntimeContext;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.*;

/**
 * Tests for the {@link org.apache.flink.streaming.api.functions.sink.PrintSinkFunction}.
 */
public class PrintSinkFunctionTest<IN> extends RichSinkFunction<IN> {

	private static final long serialVersionUID = -7194618347883773533L;

	public PrintStream printStreamOriginal = System.out;

	public class printStreamMock extends PrintStream{

		public String result;

		public printStreamMock(OutputStream out) {
			super(out);
		}

		@Override
		public void println(String x) {
			this.result = x;
		}
	}

	public OutputStream out = new OutputStream() {
		@Override
		public void write(int b) throws IOException {

		}
	};

	@Test
	public void testPrintSinkStdOut(){

		printStreamMock stream = new printStreamMock(out);
		System.setOut(stream);

		final StreamingRuntimeContext ctx = Mockito.mock(StreamingRuntimeContext.class);

		PrintSinkFunction<String> printSink = new PrintSinkFunction<>();
		printSink.setRuntimeContext(ctx);
		try {
			printSink.open(new Configuration());
		} catch (Exception e) {
			e.printStackTrace();
		}
		printSink.setTargetToStandardOut();
		printSink.invoke("hello world!");

		assertEquals("Print to System.out", printSink.toString());
		assertEquals("hello world!", stream.result);

		printSink.close();
	}

	@Test
	public void testPrintSinkStdErr(){

		printStreamMock stream = new printStreamMock(out);
		System.setOut(stream);

		final StreamingRuntimeContext ctx = Mockito.mock(StreamingRuntimeContext.class);

		PrintSinkFunction<String> printSink = new PrintSinkFunction<>();
		printSink.setRuntimeContext(ctx);
		try {
			printSink.open(new Configuration());
		} catch (Exception e) {
			e.printStackTrace();
		}
		printSink.setTargetToStandardErr();
		printSink.invoke("hello world!");

		assertEquals("Print to System.err", printSink.toString());
		assertEquals("hello world!", stream.result);

		printSink.close();
	}

	@Override
	public void invoke(IN record) {

	}

	@After
	public void restoreSystemOut() {
		System.setOut(printStreamOriginal);
	}

}