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

package org.apache.flink.changelog.fs;

import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.changelog.SequenceNumber;
import org.apache.flink.util.function.BiConsumerWithException;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.List;
import java.util.UUID;

import static java.util.Arrays.asList;
import static org.apache.flink.changelog.fs.FsStateChangelogWriterSqnTest.WriterSqnTestSettings.of;
import static org.junit.Assert.assertEquals;

/**
 * Test of incrementing {@link SequenceNumber sequence numbers} by {@link FsStateChangelogWriter}.
 */
@RunWith(Parameterized.class)
public class FsStateChangelogWriterSqnTest {

    @Parameterized.Parameters(name = "{0}")
    public static List<WriterSqnTestSettings> getSettings() {
        return asList(
                of(
                                (writer, sqn) -> writer.lastAppendedSequenceNumber(),
                                "lastAppendedSequenceNumber")
                        .withAppendCall(false)
                        .expectIncrement(false),
                of(
                                (writer, sqn) -> writer.lastAppendedSequenceNumber(),
                                "lastAppendedSequenceNumber")
                        .withAppendCall(true)
                        .expectIncrement(true),
                of((writer, sqn) -> writer.persist(sqn.next()), "persist")
                        .withAppendCall(false)
                        .expectIncrement(false),
                of((writer, sqn) -> writer.persist(sqn.next()), "persist")
                        .withAppendCall(true)
                        .expectIncrement(true),
                of((writer, sqn) -> writer.append(0, getBytes()), "append")
                        .withAppendCall(true)
                        .expectIncrement(false),
                of((writer, sqn) -> writer.append(0, getBytes()), "append")
                        .withAppendCall(false)
                        .expectIncrement(false),
                of(FsStateChangelogWriter::truncate, "truncate empty")
                        .withAppendCall(false)
                        .expectIncrement(false),
                of(FsStateChangelogWriter::truncate, "truncate old")
                        .withAppendCall(true)
                        .expectIncrement(false),
                of((writer, sqn) -> writer.truncate(sqn.next()), "truncate current")
                        .withAppendCall(true)
                        .expectIncrement(true));
    }

    private final WriterSqnTestSettings test;

    public FsStateChangelogWriterSqnTest(WriterSqnTestSettings test) {
        this.test = test;
    }

    @Test
    public void runTest() throws IOException {
        try (FsStateChangelogWriter writer =
                new FsStateChangelogWriter(
                        UUID.randomUUID(),
                        KeyGroupRange.of(0, 0),
                        new TestingStateChangeStore(),
                        Runnable::run)) {
            SequenceNumber startSqn = writer.lastAppendedSequenceNumber();
            if (test.withAppend) {
                writer.append(0, getBytes());
            }
            test.action.accept(writer, startSqn);
            assertEquals(
                    getMessage(),
                    test.expectIncrement ? startSqn.next() : startSqn,
                    writer.lastAppendedSqnUnsafe());
        }
    }

    private String getMessage() {
        return test.name
                + " should"
                + (test.expectIncrement ? " " : " NOT ")
                + "increment SQN"
                + (test.expectIncrement ? " after " : " without ")
                + "appends";
    }

    static class WriterSqnTestSettings {
        private final String name;
        private final BiConsumerWithException<FsStateChangelogWriter, SequenceNumber, IOException>
                action;
        private boolean withAppend;
        private boolean expectIncrement;

        public WriterSqnTestSettings(
                String name,
                BiConsumerWithException<FsStateChangelogWriter, SequenceNumber, IOException>
                        action) {
            this.name = name;
            this.action = action;
        }

        public static WriterSqnTestSettings of(
                BiConsumerWithException<FsStateChangelogWriter, SequenceNumber, IOException> action,
                String name) {
            return new WriterSqnTestSettings(name, action);
        }

        public WriterSqnTestSettings withAppendCall(boolean withAppend) {
            this.withAppend = withAppend;
            return this;
        }

        public WriterSqnTestSettings expectIncrement(boolean expectIncrement) {
            this.expectIncrement = expectIncrement;
            return this;
        }

        @Override
        public String toString() {
            return name + ", withAppend: " + withAppend + ", expectIncrement: " + expectIncrement;
        }
    }

    private static byte[] getBytes() {
        return new byte[] {1, 2, 3, 4};
    }
}
