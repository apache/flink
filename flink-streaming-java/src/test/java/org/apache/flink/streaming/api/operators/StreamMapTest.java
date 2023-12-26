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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.TestHarnessUtil;

import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Tests for {@link StreamMap}. These test that:
 *
 * <ul>
 *   <li>RichFunction methods are called correctly
 *   <li>Timestamps of processed elements match the input timestamp
 *   <li>Watermarks are correctly forwarded
 * </ul>
 */
public class StreamMapTest {

    private static class Map implements MapFunction<Integer, String> {
        private static final long serialVersionUID = 1L;

        @Override
        public String map(Integer value) throws Exception {
            return "+" + (value + 1);
        }
    }

    @Test
    public void testMap() throws Exception {
        StreamMap<Integer, String> operator = new StreamMap<Integer, String>(new Map());

        OneInputStreamOperatorTestHarness<Integer, String> testHarness =
                new OneInputStreamOperatorTestHarness<Integer, String>(operator);

        long initialTime = 0L;
        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<Object>();

        testHarness.open();

        testHarness.processElement(new StreamRecord<Integer>(1, initialTime + 1));
        testHarness.processElement(new StreamRecord<Integer>(2, initialTime + 2));
        testHarness.processWatermark(new Watermark(initialTime + 2));
        testHarness.processElement(new StreamRecord<Integer>(3, initialTime + 3));

        expectedOutput.add(new StreamRecord<String>("+2", initialTime + 1));
        expectedOutput.add(new StreamRecord<String>("+3", initialTime + 2));
        expectedOutput.add(new Watermark(initialTime + 2));
        expectedOutput.add(new StreamRecord<String>("+4", initialTime + 3));

        TestHarnessUtil.assertOutputEquals(
                "Output was not correct.", expectedOutput, testHarness.getOutput());
    }

    @Test
    public void testOpenClose() throws Exception {
        StreamMap<String, String> operator =
                new StreamMap<String, String>(new TestOpenCloseMapFunction());

        OneInputStreamOperatorTestHarness<String, String> testHarness =
                new OneInputStreamOperatorTestHarness<String, String>(operator);

        long initialTime = 0L;

        testHarness.open();

        testHarness.processElement(new StreamRecord<String>("Hello", initialTime));

        testHarness.close();

        Assert.assertTrue(
                "RichFunction methods where not called.", TestOpenCloseMapFunction.closeCalled);
        Assert.assertTrue("Output contains no elements.", testHarness.getOutput().size() > 0);
    }

    // This must only be used in one test, otherwise the static fields will be changed
    // by several tests concurrently
    private static class TestOpenCloseMapFunction extends RichMapFunction<String, String> {
        private static final long serialVersionUID = 1L;

        public static boolean openCalled = false;
        public static boolean closeCalled = false;

        @Override
        public void open(OpenContext openContext) throws Exception {
            super.open(openContext);
            if (closeCalled) {
                Assert.fail("Close called before open.");
            }
            openCalled = true;
        }

        @Override
        public void close() throws Exception {
            super.close();
            if (!openCalled) {
                Assert.fail("Open was not called before close.");
            }
            closeCalled = true;
        }

        @Override
        public String map(String value) throws Exception {
            if (!openCalled) {
                Assert.fail("Open was not called before run.");
            }
            return value;
        }
    }
}
