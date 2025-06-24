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

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.TestHarnessUtil;
import org.apache.flink.util.Collector;

import org.junit.jupiter.api.Test;

import java.util.concurrent.ConcurrentLinkedQueue;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link StreamMap}. These test that:
 *
 * <ul>
 *   <li>RichFunction methods are called correctly
 *   <li>Timestamps of processed elements match the input timestamp
 *   <li>Watermarks are correctly forwarded
 * </ul>
 */
class StreamFlatMapTest {

    private static final class MyFlatMap implements FlatMapFunction<Integer, Integer> {

        private static final long serialVersionUID = 1L;

        @Override
        public void flatMap(Integer value, Collector<Integer> out) throws Exception {
            if (value % 2 == 0) {
                out.collect(value);
                out.collect(value * value);
            }
        }
    }

    @Test
    void testFlatMap() throws Exception {
        StreamFlatMap<Integer, Integer> operator =
                new StreamFlatMap<Integer, Integer>(new MyFlatMap());

        OneInputStreamOperatorTestHarness<Integer, Integer> testHarness =
                new OneInputStreamOperatorTestHarness<Integer, Integer>(operator);

        long initialTime = 0L;
        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<Object>();

        testHarness.open();

        testHarness.processElement(new StreamRecord<Integer>(1, initialTime + 1));
        testHarness.processElement(new StreamRecord<Integer>(2, initialTime + 2));
        testHarness.processWatermark(new Watermark(initialTime + 2));
        testHarness.processElement(new StreamRecord<Integer>(3, initialTime + 3));
        testHarness.processElement(new StreamRecord<Integer>(4, initialTime + 4));
        testHarness.processElement(new StreamRecord<Integer>(5, initialTime + 5));
        testHarness.processElement(new StreamRecord<Integer>(6, initialTime + 6));
        testHarness.processElement(new StreamRecord<Integer>(7, initialTime + 7));
        testHarness.processElement(new StreamRecord<Integer>(8, initialTime + 8));

        expectedOutput.add(new StreamRecord<Integer>(2, initialTime + 2));
        expectedOutput.add(new StreamRecord<Integer>(4, initialTime + 2));
        expectedOutput.add(new Watermark(initialTime + 2));
        expectedOutput.add(new StreamRecord<Integer>(4, initialTime + 4));
        expectedOutput.add(new StreamRecord<Integer>(16, initialTime + 4));
        expectedOutput.add(new StreamRecord<Integer>(6, initialTime + 6));
        expectedOutput.add(new StreamRecord<Integer>(36, initialTime + 6));
        expectedOutput.add(new StreamRecord<Integer>(8, initialTime + 8));
        expectedOutput.add(new StreamRecord<Integer>(64, initialTime + 8));

        TestHarnessUtil.assertOutputEquals(
                "Output was not correct.", expectedOutput, testHarness.getOutput());
    }

    @Test
    void testOpenClose() throws Exception {
        StreamFlatMap<String, String> operator =
                new StreamFlatMap<String, String>(new TestOpenCloseFlatMapFunction());

        OneInputStreamOperatorTestHarness<String, String> testHarness =
                new OneInputStreamOperatorTestHarness<String, String>(operator);

        long initialTime = 0L;

        testHarness.open();

        testHarness.processElement(new StreamRecord<String>("Hello", initialTime));

        testHarness.close();

        assertThat(TestOpenCloseFlatMapFunction.closeCalled)
                .as("RichFunction methods where not called.")
                .isTrue();
        assertThat(testHarness.getOutput()).as("Output contains no elements.").isNotEmpty();
    }

    // This must only be used in one test, otherwise the static fields will be changed
    // by several tests concurrently
    private static class TestOpenCloseFlatMapFunction extends RichFlatMapFunction<String, String> {
        private static final long serialVersionUID = 1L;

        public static boolean openCalled = false;
        public static boolean closeCalled = false;

        @Override
        public void open(OpenContext openContext) throws Exception {
            super.open(openContext);
            assertThat(closeCalled).as("Close called before open.").isFalse();
            openCalled = true;
        }

        @Override
        public void close() throws Exception {
            super.close();
            assertThat(openCalled).as("Open was not called before close.").isTrue();
            closeCalled = true;
        }

        @Override
        public void flatMap(String value, Collector<String> out) throws Exception {
            assertThat(openCalled).as("Open was not called before run.").isTrue();
            out.collect(value);
        }
    }
}
