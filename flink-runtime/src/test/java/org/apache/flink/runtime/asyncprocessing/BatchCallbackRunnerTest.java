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

package org.apache.flink.runtime.asyncprocessing;

import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.function.ThrowingRunnable;

import org.junit.jupiter.api.Test;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link BatchCallbackRunner}. */
public class BatchCallbackRunnerTest {

    private static final ThrowingRunnable<? extends Exception> DUMMY = () -> {};

    @Test
    void testSingleSubmit() {
        ManualMailboxExecutor executor = new ManualMailboxExecutor();
        AtomicBoolean notified = new AtomicBoolean(false);
        BatchCallbackRunner runner = new BatchCallbackRunner(executor, () -> notified.set(true));
        runner.submit(DUMMY);
        assertThat(runner.isHasMail()).isTrue();
        assertThat(notified.get()).isTrue();
        executor.runOne();
        assertThat(runner.isHasMail()).isFalse();
    }

    @Test
    void testHugeBatch() {
        ManualMailboxExecutor executor = new ManualMailboxExecutor();
        AtomicBoolean notified = new AtomicBoolean(false);
        BatchCallbackRunner runner = new BatchCallbackRunner(executor, () -> notified.set(true));
        for (int i = 0; i < BatchCallbackRunner.DEFAULT_BATCH_SIZE + 1; i++) {
            runner.submit(DUMMY);
        }
        assertThat(runner.isHasMail()).isTrue();
        assertThat(notified.get()).isTrue();
        executor.runOne();
        assertThat(runner.isHasMail()).isTrue();
        notified.set(false);
        runner.submit(DUMMY);
        assertThat(notified.get()).isFalse();
        executor.runOne();
        assertThat(runner.isHasMail()).isFalse();
        runner.submit(DUMMY);
        assertThat(runner.isHasMail()).isTrue();
        assertThat(notified.get()).isTrue();
    }

    /** A mailbox executor that immediately executes code in the current thread. */
    public static class ManualMailboxExecutor implements MailboxExecutor {

        Deque<ThrowingRunnable<? extends Exception>> commands = new ArrayDeque<>();

        @Override
        public void execute(
                MailOptions mailOptions,
                ThrowingRunnable<? extends Exception> command,
                String descriptionFormat,
                Object... descriptionArgs) {
            commands.push(command);
        }

        public void runOne() {
            ThrowingRunnable<? extends Exception> command = commands.pop();
            if (command != null) {
                try {
                    command.run();
                } catch (Exception e) {
                    throw new FlinkRuntimeException("Cannot execute mail", e);
                }
            }
        }

        @Override
        public void yield() throws FlinkRuntimeException {}

        @Override
        public boolean tryYield() throws FlinkRuntimeException {
            return false;
        }

        @Override
        public boolean shouldInterrupt() {
            return false;
        }
    }
}
