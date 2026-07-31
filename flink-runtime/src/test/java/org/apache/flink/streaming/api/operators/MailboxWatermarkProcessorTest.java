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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.KeyedStateCheckpointOutputStream;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTaskActionExecutor;
import org.apache.flink.streaming.runtime.tasks.mailbox.Mail;
import org.apache.flink.streaming.runtime.tasks.mailbox.MailboxExecutorImpl;
import org.apache.flink.streaming.runtime.tasks.mailbox.TaskMailbox;
import org.apache.flink.streaming.runtime.tasks.mailbox.TaskMailboxImpl;
import org.apache.flink.streaming.util.CollectorOutput;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link MailboxWatermarkProcessor}. */
class MailboxWatermarkProcessorTest {

    @Test
    void testEmitWatermarkInsideMailbox() throws Exception {
        int priority = 42;
        final List<StreamElement> emittedElements = new ArrayList<>();
        final TaskMailboxImpl mailbox = new TaskMailboxImpl();
        final InternalTimeServiceManager<?> timerService = new NoOpInternalTimeServiceManager();

        final MailboxWatermarkProcessor<StreamRecord<String>> watermarkProcessor =
                new MailboxWatermarkProcessor<>(
                        new CollectorOutput<>(emittedElements),
                        new MailboxExecutorImpl(
                                mailbox, priority, StreamTaskActionExecutor.IMMEDIATE),
                        timerService,
                        true);
        final List<Watermark> expectedOutput = new ArrayList<>();
        watermarkProcessor.emitWatermarkInsideMailbox(new Watermark(1));
        watermarkProcessor.emitWatermarkInsideMailbox(new Watermark(2));
        watermarkProcessor.emitWatermarkInsideMailbox(new Watermark(3));
        expectedOutput.add(new Watermark(1));
        expectedOutput.add(new Watermark(2));
        expectedOutput.add(new Watermark(3));

        assertThat(emittedElements).containsExactlyElementsOf(expectedOutput);

        mailbox.put(new Mail(() -> {}, TaskMailbox.MIN_PRIORITY, "checkpoint mail"));

        watermarkProcessor.emitWatermarkInsideMailbox(new Watermark(4));
        watermarkProcessor.emitWatermarkInsideMailbox(new Watermark(5));

        assertThat(emittedElements).containsExactlyElementsOf(expectedOutput);

        // FLINK-35528: do not allow yielding to continuation mails
        assertThat(mailbox.tryTake(priority)).isEqualTo(Optional.empty());
        assertThat(emittedElements).containsExactlyElementsOf(expectedOutput);

        while (mailbox.hasMail()) {
            mailbox.take(TaskMailbox.MIN_PRIORITY).run();
        }
        // Watermark(4) is processed together with Watermark(5)
        expectedOutput.add(new Watermark(5));

        assertThat(emittedElements).containsExactlyElementsOf(expectedOutput);
    }

    /**
     * An interruption unrelated to the intermediate-watermark nudge (e.g. a checkpoint mail) must
     * not surface an intermediate watermark when the feature is disabled.
     */
    @Test
    void testIntermediateWatermarkNotEmittedWhenDisabled() throws Exception {
        int priority = 42;
        final List<StreamElement> emittedElements = new ArrayList<>();
        final TaskMailboxImpl mailbox = new TaskMailboxImpl();
        final InternalTimeServiceManager<?> timerService =
                new NoOpInternalTimeServiceManager() {
                    @Override
                    public long getReachedWatermark() {
                        return 5;
                    }
                };

        final MailboxWatermarkProcessor<StreamRecord<String>> watermarkProcessor =
                new MailboxWatermarkProcessor<>(
                        new CollectorOutput<>(emittedElements),
                        new MailboxExecutorImpl(
                                mailbox, priority, StreamTaskActionExecutor.IMMEDIATE),
                        timerService,
                        false);

        // Unrelated mail interrupts the firing loop for a reason unrelated to the nudge.
        mailbox.put(new Mail(() -> {}, TaskMailbox.MIN_PRIORITY, "checkpoint mail"));

        watermarkProcessor.emitWatermarkInsideMailbox(new Watermark(10));

        // configureIntermediateWatermarkInterval() was never called on this manager.
        assertThat(emittedElements).isEmpty();
    }

    /**
     * Once a watermark has been fully emitted via the shortcut branch, a later interrupted advance
     * must not emit an intermediate watermark below it.
     */
    @Test
    void testIntermediateWatermarkNeverBelowAlreadyEmittedWatermark() throws Exception {
        int priority = 42;
        final List<StreamElement> emittedElements = new ArrayList<>();
        final TaskMailboxImpl mailbox = new TaskMailboxImpl();
        final boolean[] fullyAdvancedOnce = new boolean[] {false};
        final InternalTimeServiceManager<?> timerService =
                new NoOpInternalTimeServiceManager() {
                    @Override
                    public boolean tryAdvanceWatermark(
                            Watermark watermark, ShouldStopAdvancingFn shouldStopAdvancingFn) {
                        if (!fullyAdvancedOnce[0]) {
                            fullyAdvancedOnce[0] = true;
                            return true;
                        }
                        return false;
                    }

                    @Override
                    public long getReachedWatermark() {
                        return 5;
                    }
                };

        final MailboxWatermarkProcessor<StreamRecord<String>> watermarkProcessor =
                new MailboxWatermarkProcessor<>(
                        new CollectorOutput<>(emittedElements),
                        new MailboxExecutorImpl(
                                mailbox, priority, StreamTaskActionExecutor.IMMEDIATE),
                        timerService,
                        true);

        // Fully advances to 10 via the shortcut branch.
        watermarkProcessor.emitWatermarkInsideMailbox(new Watermark(10));
        // A new, higher watermark arrives but is interrupted; reachedWatermark(5) is below the
        // watermark(10) already emitted above.
        watermarkProcessor.emitWatermarkInsideMailbox(new Watermark(20));

        assertThat(emittedElements)
                .as("watermarks must be non-decreasing")
                .containsExactly(new Watermark(10));
    }

    private static class NoOpInternalTimeServiceManager
            implements InternalTimeServiceManager<Object> {
        @Override
        public <N> InternalTimerService<N> getInternalTimerService(
                String name,
                TypeSerializer<Object> keySerializer,
                TypeSerializer<N> namespaceSerializer,
                Triggerable<Object, N> triggerable) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void advanceWatermark(Watermark watermark) throws Exception {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean tryAdvanceWatermark(
                Watermark watermark, ShouldStopAdvancingFn shouldStopAdvancingFn) throws Exception {
            return !shouldStopAdvancingFn.test();
        }

        @Override
        public long getReachedWatermark() {
            return Long.MIN_VALUE;
        }

        @Override
        public void snapshotToRawKeyedState(
                KeyedStateCheckpointOutputStream stateCheckpointOutputStream, String operatorName)
                throws Exception {
            throw new UnsupportedOperationException();
        }
    }
}
