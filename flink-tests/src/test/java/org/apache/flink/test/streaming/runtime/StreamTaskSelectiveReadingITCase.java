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

package org.apache.flink.test.streaming.runtime;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.util.TestSequentialReadingStreamOperator;
import org.apache.flink.test.streaming.runtime.util.TestListResultSink;

import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;

/** Tests for selective reading of {@code TwoInputStreamTask}. */
public class StreamTaskSelectiveReadingITCase {
    @Test
    public void testSequentialReading() throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<String> source0 =
                env.addSource(
                        new TestStringSource(
                                "Source0",
                                new String[] {
                                    "Hello-1", "Hello-2", "Hello-3", "Hello-4", "Hello-5", "Hello-6"
                                }));
        DataStream<Integer> source1 =
                env.addSource(new TestIntegerSource("Source1", new Integer[] {1, 2, 3}))
                        .setParallelism(2);
        TestListResultSink<String> resultSink = new TestListResultSink<>();

        TestSequentialReadingStreamOperator twoInputStreamOperator =
                new TestSequentialReadingStreamOperator("Operator0");
        twoInputStreamOperator.setChainingStrategy(ChainingStrategy.NEVER);

        source0.connect(source1)
                .transform(
                        "Custom Operator", BasicTypeInfo.STRING_TYPE_INFO, twoInputStreamOperator)
                .addSink(resultSink);

        env.execute("Selective reading test");

        List<String> result = resultSink.getResult();

        List<String> expected1 =
                Arrays.asList(
                        "[Operator0-1]: [Source0-0]: Hello-1",
                        "[Operator0-1]: [Source0-0]: Hello-2",
                        "[Operator0-1]: [Source0-0]: Hello-3",
                        "[Operator0-1]: [Source0-0]: Hello-4",
                        "[Operator0-1]: [Source0-0]: Hello-5",
                        "[Operator0-1]: [Source0-0]: Hello-6");

        List<String> expected2 =
                Arrays.asList(
                        "[Operator0-2]: 1",
                        "[Operator0-2]: 2",
                        "[Operator0-2]: 3",
                        "[Operator0-2]: 2",
                        "[Operator0-2]: 4",
                        "[Operator0-2]: 6");
        Collections.sort(expected2);

        assertEquals(expected1.size() + expected2.size(), result.size());
        assertEquals(expected1, result.subList(0, expected1.size()));

        List<String> result2 =
                result.subList(expected1.size(), expected1.size() + expected2.size());
        Collections.sort(result2);
        assertEquals(expected2, result2);
    }

    private abstract static class TestSource<T> extends RichParallelSourceFunction<T> {
        private static final long serialVersionUID = 1L;

        protected final String name;

        private volatile boolean running = true;
        private transient RuntimeContext context;

        private final T[] elements;

        public TestSource(String name, T[] elements) {
            this.name = name;
            this.elements = elements;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            this.context = getRuntimeContext();
        }

        @Override
        public void run(SourceContext<T> ctx) throws Exception {
            int elementIndex = 0;
            while (running) {
                if (elementIndex < elements.length) {
                    synchronized (ctx.getCheckpointLock()) {
                        ctx.collect(
                                outValue(elements[elementIndex], context.getIndexOfThisSubtask()));
                        elementIndex++;
                    }
                } else {
                    break;
                }
            }
        }

        @Override
        public void cancel() {
            running = false;
        }

        protected abstract T outValue(T inValue, int subTaskIndex);
    }

    private static class TestStringSource extends TestSource<String> {

        public TestStringSource(String name, String[] elements) {
            super(name, elements);
        }

        @Override
        protected String outValue(String inValue, int subTaskIndex) {
            return "[" + name + "-" + subTaskIndex + "]: " + inValue;
        }
    }

    private static class TestIntegerSource extends TestSource<Integer> {

        public TestIntegerSource(String name, Integer[] elements) {
            super(name, elements);
        }

        @Override
        protected Integer outValue(Integer inValue, int subTaskIndex) {
            return inValue * (subTaskIndex + 1);
        }
    }
}
