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
import org.apache.flink.streaming.util.MockStreamingRuntimeContext;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import static org.junit.Assert.assertEquals;

/** Tests for the {@link PrintSinkFunction}. */
public class PrintSinkFunctionTest {

    private final PrintStream originalSystemOut = System.out;
    private final PrintStream originalSystemErr = System.err;

    private final ByteArrayOutputStream arrayOutputStream = new ByteArrayOutputStream();
    private final ByteArrayOutputStream arrayErrorStream = new ByteArrayOutputStream();

    private final String line = System.lineSeparator();

    @Before
    public void setUp() {
        System.setOut(new PrintStream(arrayOutputStream));
        System.setErr(new PrintStream(arrayErrorStream));
    }

    @After
    public void tearDown() {
        if (System.out != originalSystemOut) {
            System.out.close();
        }
        if (System.err != originalSystemErr) {
            System.err.close();
        }
        System.setOut(originalSystemOut);
        System.setErr(originalSystemErr);
    }

    @Test
    public void testPrintSinkStdOut() throws Exception {
        PrintSinkFunction<String> printSink = new PrintSinkFunction<>();
        printSink.setRuntimeContext(new MockStreamingRuntimeContext(false, 1, 0));

        printSink.open(new Configuration());

        printSink.invoke("hello world!", SinkContextUtil.forTimestamp(0));

        assertEquals("Print to System.out", printSink.toString());
        assertEquals("hello world!" + line, arrayOutputStream.toString());
        printSink.close();
    }

    @Test
    public void testPrintSinkStdErr() throws Exception {
        PrintSinkFunction<String> printSink = new PrintSinkFunction<>(true);
        printSink.setRuntimeContext(new MockStreamingRuntimeContext(false, 1, 0));
        printSink.open(new Configuration());

        printSink.invoke("hello world!", SinkContextUtil.forTimestamp(0));

        assertEquals("Print to System.err", printSink.toString());
        assertEquals("hello world!" + line, arrayErrorStream.toString());
        printSink.close();
    }

    @Test
    public void testPrintSinkWithPrefix() throws Exception {
        PrintSinkFunction<String> printSink = new PrintSinkFunction<>();
        printSink.setRuntimeContext(new MockStreamingRuntimeContext(false, 2, 1));
        printSink.open(new Configuration());

        printSink.invoke("hello world!", SinkContextUtil.forTimestamp(0));

        assertEquals("Print to System.out", printSink.toString());
        assertEquals("2> hello world!" + line, arrayOutputStream.toString());
        printSink.close();
    }

    @Test
    public void testPrintSinkWithIdentifierAndPrefix() throws Exception {
        PrintSinkFunction<String> printSink = new PrintSinkFunction<>("mySink", false);
        printSink.setRuntimeContext(new MockStreamingRuntimeContext(false, 2, 1));
        printSink.open(new Configuration());

        printSink.invoke("hello world!", SinkContextUtil.forTimestamp(0));

        assertEquals("Print to System.out", printSink.toString());
        assertEquals("mySink:2> hello world!" + line, arrayOutputStream.toString());
        printSink.close();
    }

    @Test
    public void testPrintSinkWithIdentifierButNoPrefix() throws Exception {
        PrintSinkFunction<String> printSink = new PrintSinkFunction<>("mySink", false);
        printSink.setRuntimeContext(new MockStreamingRuntimeContext(false, 1, 0));
        printSink.open(new Configuration());

        printSink.invoke("hello world!", SinkContextUtil.forTimestamp(0));

        assertEquals("Print to System.out", printSink.toString());
        assertEquals("mySink> hello world!" + line, arrayOutputStream.toString());
        printSink.close();
    }
}
