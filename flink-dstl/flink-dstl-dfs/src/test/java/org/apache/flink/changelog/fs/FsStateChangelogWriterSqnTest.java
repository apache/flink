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

import org.apache.flink.runtime.mailbox.SyncMailboxExecutor;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.TestLocalRecoveryConfig;
import org.apache.flink.runtime.state.changelog.LocalChangelogRegistry;
import org.apache.flink.runtime.state.changelog.SequenceNumber;
import org.apache.flink.runtime.state.changelog.StateChangelogWriter;
import org.apache.flink.util.function.ThrowingConsumer;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.UUID;
import java.util.stream.Stream;

import static org.apache.flink.changelog.fs.FsStateChangelogWriterSqnTest.WriterSqnTestSettings.of;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test of incrementing {@link SequenceNumber sequence numbers} by {@link FsStateChangelogWriter}.
 */
public class FsStateChangelogWriterSqnTest {

    private static Stream<WriterSqnTestSettings> getSettings() {
        return Stream.of(
                of(StateChangelogWriter::nextSequenceNumber, "nextSequenceNumber")
                        .withAppendCall(false)
                        .expectIncrement(false),
                of(StateChangelogWriter::nextSequenceNumber, "nextSequenceNumber")
                        .withAppendCall(true)
                        .expectIncrement(true),
                of(FsStateChangelogWriterSqnTest::persistAll, "persist")
                        .withAppendCall(false)
                        .expectIncrement(false),
                of(FsStateChangelogWriterSqnTest::persistAll, "persist")
                        .withAppendCall(true)
                        .expectIncrement(true),
                of(FsStateChangelogWriterSqnTest::append, "append")
                        .withAppendCall(true)
                        .expectIncrement(false),
                of(FsStateChangelogWriterSqnTest::append, "append")
                        .withAppendCall(false)
                        .expectIncrement(false),
                of(FsStateChangelogWriterSqnTest::truncateAll, "truncate empty")
                        .withAppendCall(false)
                        .expectIncrement(false),
                of(FsStateChangelogWriterSqnTest::truncateAll, "truncate old")
                        .withAppendCall(true)
                        .expectIncrement(false),
                of(FsStateChangelogWriterSqnTest::truncateLast, "truncate last")
                        .withAppendCall(true)
                        .expectIncrement(true));
    }

    @MethodSource("getSettings")
    @ParameterizedTest(name = "writerSqnTestSettings = {0}")
    void runTest(WriterSqnTestSettings writerSqnTestSettings) throws IOException {
        try (FsStateChangelogWriter writer =
                new FsStateChangelogWriter(
                        UUID.randomUUID(),
                        KeyGroupRange.of(0, 0),
                        StateChangeUploadScheduler.directScheduler(
                                new TestingStateChangeUploader()),
                        Long.MAX_VALUE,
                        new SyncMailboxExecutor(),
                        TaskChangelogRegistry.NO_OP,
                        TestLocalRecoveryConfig.disabled(),
                        LocalChangelogRegistry.NO_OP)) {
            if (writerSqnTestSettings.withAppend) {
                append(writer);
            }
            writerSqnTestSettings.action.accept(writer);
            assertThat(writer.lastAppendedSqnUnsafe())
                    .as(writerSqnTestSettings.getMessage())
                    .isEqualTo(
                            writerSqnTestSettings.expectIncrement
                                    ? writer.initialSequenceNumber().next()
                                    : writer.initialSequenceNumber());
        }
    }

    static class WriterSqnTestSettings {
        private final String name;
        private final ThrowingConsumer<FsStateChangelogWriter, IOException> action;
        private boolean withAppend;
        private boolean expectIncrement;

        public WriterSqnTestSettings(
                String name, ThrowingConsumer<FsStateChangelogWriter, IOException> action) {
            this.name = name;
            this.action = action;
        }

        public String getMessage() {
            return this.name
                    + " should"
                    + (this.expectIncrement ? " " : " NOT ")
                    + "increment SQN"
                    + (this.expectIncrement ? " after " : " without ")
                    + "appends";
        }

        public static WriterSqnTestSettings of(
                ThrowingConsumer<FsStateChangelogWriter, IOException> action, String name) {
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

    private static void append(FsStateChangelogWriter writer) throws IOException {
        writer.append(0, new byte[] {1, 2, 3, 4});
    }

    private static void truncateLast(FsStateChangelogWriter writer) {
        writer.truncate(writer.nextSequenceNumber());
    }

    private static void truncateAll(FsStateChangelogWriter writer) {
        writer.truncate(writer.initialSequenceNumber());
    }

    private static void persistAll(FsStateChangelogWriter writer) throws IOException {
        writer.persist(writer.initialSequenceNumber(), 1L);
    }
}
