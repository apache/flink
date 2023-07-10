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

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.common.operators.ProcessingTimeService;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.runtime.metrics.groups.InternalSinkWriterMetricGroup;
import org.apache.flink.streaming.api.functions.sink.PrintSink;
import org.apache.flink.streaming.runtime.tasks.TestProcessingTimeService;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.SimpleUserCodeClassLoader;
import org.apache.flink.util.UserCodeClassLoader;
import org.apache.flink.util.function.ThrowingRunnable;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.OptionalLong;

import static org.junit.Assert.assertEquals;

/** Tests for the {@link PrintSink}. */
class PrintSinkTest {

    private final PrintStream originalSystemOut = System.out;
    private final PrintStream originalSystemErr = System.err;

    private final ByteArrayOutputStream arrayOutputStream = new ByteArrayOutputStream();
    private final ByteArrayOutputStream arrayErrorStream = new ByteArrayOutputStream();

    private final String line = System.lineSeparator();

    @BeforeEach
    void setUp() {
        System.setOut(new PrintStream(arrayOutputStream));
        System.setErr(new PrintStream(arrayErrorStream));
    }

    @AfterEach
    void tearDown() {
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
    void testPrintSinkStdOut() throws Exception {
        PrintSink<String> printSink = new PrintSink<>();

        try (SinkWriter<String> writer = printSink.createWriter(new MockInitContext(1))) {
            writer.write("hello world!", new MockContext());

            assertEquals("Print to System.out", printSink.toString());
            assertEquals("hello world!" + line, arrayOutputStream.toString());
        }
    }

    @Test
    void testPrintSinkStdErr() throws Exception {
        PrintSink<String> printSink = new PrintSink<>(true);

        try (SinkWriter<String> writer = printSink.createWriter(new MockInitContext(1))) {
            writer.write("hello world!", new MockContext());

            assertEquals("Print to System.err", printSink.toString());
            assertEquals("hello world!" + line, arrayErrorStream.toString());
        }
    }

    @Test
    void testPrintSinkStdErrWithIdentifier() throws Exception {
        PrintSink<String> printSink = new PrintSink<>("mySink", true);

        try (SinkWriter<String> writer = printSink.createWriter(new MockInitContext(1))) {
            writer.write("hello world!", new MockContext());

            assertEquals("Print to System.err", printSink.toString());
            assertEquals("mySink> hello world!" + line, arrayErrorStream.toString());
        }
    }

    @Test
    void testPrintSinkWithPrefix() throws Exception {
        PrintSink<String> printSink = new PrintSink<>();

        try (SinkWriter<String> writer = printSink.createWriter(new MockInitContext(2))) {
            writer.write("hello world!", new MockContext());

            assertEquals("Print to System.out", printSink.toString());
            assertEquals("1> hello world!" + line, arrayOutputStream.toString());
        }
    }

    @Test
    void testPrintSinkWithIdentifierAndPrefix() throws Exception {
        PrintSink<String> printSink = new PrintSink<>("mySink");

        try (SinkWriter<String> writer = printSink.createWriter(new MockInitContext(2))) {
            writer.write("hello world!", new MockContext());

            assertEquals("Print to System.out", printSink.toString());
            assertEquals("mySink:1> hello world!" + line, arrayOutputStream.toString());
        }
    }

    @Test
    void testPrintSinkWithIdentifierButNoPrefix() throws Exception {
        PrintSink<String> printSink = new PrintSink<>("mySink");

        try (SinkWriter<String> writer = printSink.createWriter(new MockInitContext(1))) {
            writer.write("hello world!", new MockContext());

            assertEquals("Print to System.out", printSink.toString());
            assertEquals("mySink> hello world!" + line, arrayOutputStream.toString());
        }
    }

    private static class MockContext implements SinkWriter.Context {

        @Override
        public long currentWatermark() {
            return 0;
        }

        @Override
        public Long timestamp() {
            return System.currentTimeMillis();
        }
    }

    private static class MockInitContext
            implements Sink.InitContext, SerializationSchema.InitializationContext {

        private final int numSubtasks;

        private MockInitContext(int numSubtasks) {
            this.numSubtasks = numSubtasks;
        }

        @Override
        public UserCodeClassLoader getUserCodeClassLoader() {
            return SimpleUserCodeClassLoader.create(PrintSinkTest.class.getClassLoader());
        }

        @Override
        public MailboxExecutor getMailboxExecutor() {
            return new DummyMailboxExecutor();
        }

        @Override
        public ProcessingTimeService getProcessingTimeService() {
            return new TestProcessingTimeService();
        }

        @Override
        public int getSubtaskId() {
            return 0;
        }

        @Override
        public int getNumberOfParallelSubtasks() {
            return numSubtasks;
        }

        @Override
        public int getAttemptNumber() {
            return 0;
        }

        @Override
        public SinkWriterMetricGroup metricGroup() {
            return InternalSinkWriterMetricGroup.mock(new UnregisteredMetricsGroup());
        }

        @Override
        public MetricGroup getMetricGroup() {
            return metricGroup();
        }

        @Override
        public OptionalLong getRestoredCheckpointId() {
            return OptionalLong.empty();
        }

        @Override
        public SerializationSchema.InitializationContext
                asSerializationSchemaInitializationContext() {
            return this;
        }

        @Override
        public boolean isObjectReuseEnabled() {
            return false;
        }

        @Override
        public <IN> TypeSerializer<IN> createInputSerializer() {
            return null;
        }

        @Override
        public JobID getJobId() {
            return null;
        }
    }

    private static class DummyMailboxExecutor implements MailboxExecutor {

        @Override
        public void execute(
                ThrowingRunnable<? extends Exception> command,
                String descriptionFormat,
                Object... descriptionArgs) {}

        @Override
        public void yield() throws InterruptedException, FlinkRuntimeException {}

        @Override
        public boolean tryYield() throws FlinkRuntimeException {
            return false;
        }
    }
}
