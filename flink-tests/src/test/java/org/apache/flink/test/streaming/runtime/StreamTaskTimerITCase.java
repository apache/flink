/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.test.streaming.runtime;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeCallback;
import org.apache.flink.streaming.runtime.tasks.TimerException;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.util.ExceptionUtils;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.Semaphore;

import static org.junit.Assert.assertTrue;

/**
 * Tests for the timer service of {@code StreamTask}.
 *
 * <p>These tests ensure that exceptions are properly forwarded from the timer thread to the task
 * thread and that operator methods are not invoked concurrently.
 */
@RunWith(Parameterized.class)
public class StreamTaskTimerITCase extends AbstractTestBase {

    private final TimeCharacteristic timeCharacteristic;

    public StreamTaskTimerITCase(TimeCharacteristic characteristic) {
        timeCharacteristic = characteristic;
    }

    /**
     * Note: this test fails if we don't check for exceptions in the source contexts and do not
     * synchronize in the source contexts.
     */
    @Test
    public void testOperatorChainedToSource() throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(timeCharacteristic);
        env.setParallelism(1);

        DataStream<String> source = env.addSource(new InfiniteTestSource());

        source.transform(
                "Custom Operator",
                BasicTypeInfo.STRING_TYPE_INFO,
                new TimerOperator(ChainingStrategy.ALWAYS));

        try {
            env.execute("Timer test");
        } catch (JobExecutionException e) {
            verifyJobExecutionException(e);
        }
    }

    private void verifyJobExecutionException(JobExecutionException e) throws JobExecutionException {
        final Optional<TimerException> optionalTimerException =
                ExceptionUtils.findThrowable(e, TimerException.class);
        assertTrue(optionalTimerException.isPresent());

        TimerException te = optionalTimerException.get();
        if (te.getCause() instanceof RuntimeException) {
            RuntimeException re = (RuntimeException) te.getCause();
            if (!re.getMessage().equals("TEST SUCCESS")) {
                throw e;
            }
        } else {
            throw e;
        }
    }

    /**
     * Note: this test fails if we don't check for exceptions in the source contexts and do not
     * synchronize in the source contexts.
     */
    @Test
    public void testOneInputOperatorWithoutChaining() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(timeCharacteristic);
        env.setParallelism(1);

        DataStream<String> source = env.addSource(new InfiniteTestSource());

        source.transform(
                "Custom Operator",
                BasicTypeInfo.STRING_TYPE_INFO,
                new TimerOperator(ChainingStrategy.NEVER));

        try {
            env.execute("Timer test");
        } catch (JobExecutionException e) {
            verifyJobExecutionException(e);
        }
    }

    @Test
    public void testTwoInputOperatorWithoutChaining() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(timeCharacteristic);
        env.setParallelism(1);

        DataStream<String> source = env.addSource(new InfiniteTestSource());

        source.connect(source)
                .transform(
                        "Custom Operator",
                        BasicTypeInfo.STRING_TYPE_INFO,
                        new TwoInputTimerOperator(ChainingStrategy.NEVER));

        try {
            env.execute("Timer test");
        } catch (JobExecutionException e) {
            verifyJobExecutionException(e);
        }
    }

    private static class TimerOperator extends AbstractStreamOperator<String>
            implements OneInputStreamOperator<String, String>, ProcessingTimeCallback {
        private static final long serialVersionUID = 1L;

        int numTimers = 0;
        int numElements = 0;

        private boolean first = true;

        private Semaphore semaphore = new Semaphore(1);

        public TimerOperator(ChainingStrategy chainingStrategy) {
            setChainingStrategy(chainingStrategy);
        }

        @Override
        public void processElement(StreamRecord<String> element) throws Exception {
            if (!semaphore.tryAcquire()) {
                Assert.fail("Concurrent invocation of operator functions.");
            }

            if (first) {
                getProcessingTimeService().registerTimer(System.currentTimeMillis() + 100, this);
                first = false;
            }
            numElements++;

            semaphore.release();
        }

        @Override
        public void onProcessingTime(long time) throws Exception {
            if (!semaphore.tryAcquire()) {
                Assert.fail("Concurrent invocation of operator functions.");
            }

            try {
                numTimers++;
                throwIfDone();
                getProcessingTimeService().registerTimer(System.currentTimeMillis() + 1, this);
            } finally {
                semaphore.release();
            }
        }

        private void throwIfDone() {
            if (numTimers > 1000 && numElements > 10_000) {
                throw new RuntimeException("TEST SUCCESS");
            }
        }

        @Override
        public void processWatermark(Watermark mark) throws Exception {
            if (!semaphore.tryAcquire()) {
                Assert.fail("Concurrent invocation of operator functions.");
            }
            semaphore.release();
        }
    }

    private static class TwoInputTimerOperator extends AbstractStreamOperator<String>
            implements TwoInputStreamOperator<String, String, String>, ProcessingTimeCallback {
        private static final long serialVersionUID = 1L;

        int numTimers = 0;
        int numElements = 0;

        private boolean first = true;

        private Semaphore semaphore = new Semaphore(1);

        public TwoInputTimerOperator(ChainingStrategy chainingStrategy) {
            setChainingStrategy(chainingStrategy);
        }

        @Override
        public void processElement1(StreamRecord<String> element) throws Exception {
            if (!semaphore.tryAcquire()) {
                Assert.fail("Concurrent invocation of operator functions.");
            }

            if (first) {
                getProcessingTimeService().registerTimer(System.currentTimeMillis() + 100, this);
                first = false;
            }
            numElements++;

            semaphore.release();
        }

        @Override
        public void processElement2(StreamRecord<String> element) throws Exception {
            if (!semaphore.tryAcquire()) {
                Assert.fail("Concurrent invocation of operator functions.");
            }

            if (first) {
                getProcessingTimeService().registerTimer(System.currentTimeMillis() + 100, this);
                first = false;
            }
            numElements++;

            semaphore.release();
        }

        @Override
        public void onProcessingTime(long time) throws Exception {
            if (!semaphore.tryAcquire()) {
                Assert.fail("Concurrent invocation of operator functions.");
            }

            try {
                numTimers++;
                throwIfDone();
                getProcessingTimeService().registerTimer(System.currentTimeMillis() + 1, this);
            } finally {
                semaphore.release();
            }
        }

        private void throwIfDone() {
            if (numTimers > 1000 && numElements > 10_000) {
                throw new RuntimeException("TEST SUCCESS");
            }
        }

        @Override
        public void processWatermark1(Watermark mark) throws Exception {
            // ignore
        }

        @Override
        public void processWatermark2(Watermark mark) throws Exception {
            // ignore
        }
    }

    private static class InfiniteTestSource implements SourceFunction<String> {
        private static final long serialVersionUID = 1L;
        private volatile boolean running = true;

        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            while (running) {
                ctx.collect("hello");
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

    // ------------------------------------------------------------------------
    //  parametrization
    // ------------------------------------------------------------------------

    @Parameterized.Parameters(name = "Time Characteristic = {0}")
    public static Collection<Object[]> executionModes() {
        return Arrays.asList(
                new Object[] {TimeCharacteristic.ProcessingTime},
                new Object[] {TimeCharacteristic.IngestionTime},
                new Object[] {TimeCharacteristic.EventTime});
    }
}
