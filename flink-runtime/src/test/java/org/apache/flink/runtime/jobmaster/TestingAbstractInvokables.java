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

package org.apache.flink.runtime.jobmaster;

import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.io.network.api.reader.RecordReader;
import org.apache.flink.runtime.io.network.api.writer.RecordWriter;
import org.apache.flink.runtime.io.network.api.writer.RecordWriterBuilder;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.types.IntValue;

import java.util.concurrent.CompletableFuture;

/** {@link AbstractInvokable} for testing purposes. */
public class TestingAbstractInvokables {

    private TestingAbstractInvokables() {
        throw new UnsupportedOperationException(
                getClass().getSimpleName() + " should not be instantiated.");
    }

    /** Basic sender {@link AbstractInvokable} which sends 42 and 1337 down stream. */
    public static class Sender extends AbstractInvokable {

        public Sender(Environment environment) {
            super(environment);
        }

        @Override
        public void invoke() throws Exception {
            final RecordWriter<IntValue> writer =
                    new RecordWriterBuilder<IntValue>().build(getEnvironment().getWriter(0));

            try {
                writer.emit(new IntValue(42));
                writer.emit(new IntValue(1337));
                writer.flushAll();
            } finally {
                writer.close();
            }
        }
    }

    /**
     * Basic receiver {@link AbstractInvokable} which verifies the sent elements from the {@link
     * Sender}.
     */
    public static class Receiver extends AbstractInvokable {

        public Receiver(Environment environment) {
            super(environment);
        }

        @Override
        public void invoke() throws Exception {
            final RecordReader<IntValue> reader =
                    new RecordReader<>(
                            getEnvironment().getInputGate(0),
                            IntValue.class,
                            getEnvironment().getTaskManagerInfo().getTmpDirectories());

            final IntValue i1 = reader.next();
            final IntValue i2 = reader.next();
            final IntValue i3 = reader.next();

            if (i1.getValue() != 42 || i2.getValue() != 1337 || i3 != null) {
                throw new Exception("Wrong data received.");
            }
        }
    }

    public static final class TestInvokableRecordCancel extends AbstractInvokable {

        private static CompletableFuture<Boolean> gotCanceledFuture = new CompletableFuture<>();

        public TestInvokableRecordCancel(Environment environment) {
            super(environment);
        }

        @Override
        public void invoke() throws Exception {
            final Object o = new Object();
            RecordWriter<IntValue> recordWriter =
                    new RecordWriterBuilder<IntValue>().build(getEnvironment().getWriter(0));
            for (int i = 0; i < 1024; i++) {
                recordWriter.emit(new IntValue(42));
            }

            gotCanceledFuture.get();
        }

        @Override
        public void cancel() {
            gotCanceledFuture.complete(true);
        }

        public static void resetGotCanceledFuture() {
            gotCanceledFuture = new CompletableFuture<>();
        }

        public static CompletableFuture<Boolean> gotCanceled() {
            return gotCanceledFuture;
        }
    }
}
