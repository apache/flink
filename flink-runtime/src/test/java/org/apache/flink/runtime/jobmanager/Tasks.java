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

package org.apache.flink.runtime.jobmanager;

import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.io.network.api.reader.RecordReader;
import org.apache.flink.runtime.io.network.api.writer.RecordWriter;
import org.apache.flink.runtime.io.network.api.writer.RecordWriterBuilder;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.types.IntValue;

import java.io.IOException;

/** Collection of {@link AbstractInvokable}s. */
public class Tasks {

    /** An {@link AbstractInvokable} that forwards all incoming elements. */
    public static class Forwarder extends AbstractInvokable {

        public Forwarder(Environment environment) {
            super(environment);
        }

        @Override
        public void invoke() throws Exception {

            final RecordReader<IntValue> reader =
                    new RecordReader<>(
                            getEnvironment().getInputGate(0),
                            IntValue.class,
                            getEnvironment().getTaskManagerInfo().getTmpDirectories());

            final RecordWriter<IntValue> writer =
                    new RecordWriterBuilder<IntValue>().build(getEnvironment().getWriter(0));

            try {
                while (true) {
                    final IntValue record = reader.next();

                    if (record == null) {
                        return;
                    }

                    writer.emit(record);
                }
            } finally {
                writer.close();
            }
        }
    }

    /** An {@link AbstractInvokable} that consumes 1 input channel. */
    public static class AgnosticReceiver extends AbstractInvokable {

        public AgnosticReceiver(Environment environment) {
            super(environment);
        }

        @Override
        public void invoke() throws Exception {
            consumeInputs(1, this);
        }
    }

    /** An {@link AbstractInvokable} that consumes 2 input channels. */
    public static class AgnosticBinaryReceiver extends AbstractInvokable {

        public AgnosticBinaryReceiver(Environment environment) {
            super(environment);
        }

        @Override
        public void invoke() throws Exception {
            consumeInputs(2, this);
        }
    }

    /** An {@link AbstractInvokable} that consumes 3 input channels. */
    public static class AgnosticTertiaryReceiver extends AbstractInvokable {

        public AgnosticTertiaryReceiver(Environment environment) {
            super(environment);
        }

        @Override
        public void invoke() throws Exception {
            consumeInputs(3, this);
        }
    }

    private static void consumeInputs(int numberOfInputs, AbstractInvokable consumer)
            throws IOException, InterruptedException {
        for (int i = 0; i < numberOfInputs; i++) {
            final RecordReader<IntValue> reader =
                    new RecordReader<>(
                            consumer.getEnvironment().getInputGate(i),
                            IntValue.class,
                            consumer.getEnvironment().getTaskManagerInfo().getTmpDirectories());
            while (reader.next() != null) {}
        }
    }

    /** An {@link AbstractInvokable} that throws an exception when being invoked. */
    public static class ExceptionSender extends AbstractInvokable {

        public ExceptionSender(Environment environment) {
            super(environment);
        }

        @Override
        public void invoke() throws Exception {
            throw new Exception("Test exception");
        }
    }

    /** An {@link AbstractInvokable} that throws an exception when being invoked. */
    public static class ExceptionReceiver extends AbstractInvokable {

        public ExceptionReceiver(Environment environment) {
            super(environment);
        }

        @Override
        public void invoke() throws Exception {
            throw new Exception("Test exception");
        }
    }

    /** An {@link AbstractInvokable} that throws an exception when being instantiated. */
    public static class InstantiationErrorSender extends AbstractInvokable {

        public InstantiationErrorSender(Environment environment) {
            super(environment);
            throw new RuntimeException("Test exception in constructor");
        }

        @Override
        public void invoke() throws Exception {}
    }
}
