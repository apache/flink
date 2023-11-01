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

package org.apache.flink.streaming.runtime.io.checkpointing;

import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.runtime.operators.testutils.ExpectedTestException;
import org.apache.flink.streaming.runtime.tasks.StreamTaskActionExecutor;
import org.apache.flink.streaming.runtime.tasks.TestProcessingTimeService;
import org.apache.flink.streaming.runtime.tasks.mailbox.MailboxExecutorImpl;
import org.apache.flink.streaming.runtime.tasks.mailbox.MailboxProcessor;
import org.apache.flink.streaming.runtime.tasks.mailbox.TaskMailbox;
import org.apache.flink.streaming.runtime.tasks.mailbox.TaskMailboxImpl;
import org.apache.flink.util.ExceptionUtils;

import org.junit.Test;

import java.time.Duration;

import static org.junit.Assert.assertThrows;

/** {@link BarrierAlignmentUtil} test. */
public class BarrierAlignmentUtilTest {

    @Test
    public void testDelayableTimerNotHiddenException() throws Exception {
        TaskMailbox mailbox = new TaskMailboxImpl();
        MailboxProcessor mailboxProcessor =
                new MailboxProcessor(controller -> {}, mailbox, StreamTaskActionExecutor.IMMEDIATE);
        MailboxExecutor mailboxExecutor =
                new MailboxExecutorImpl(
                        mailbox, 0, StreamTaskActionExecutor.IMMEDIATE, mailboxProcessor);

        TestProcessingTimeService timerService = new TestProcessingTimeService();
        timerService.setCurrentTime(System.currentTimeMillis());

        BarrierAlignmentUtil.DelayableTimer delayableTimer =
                BarrierAlignmentUtil.createRegisterTimerCallback(mailboxExecutor, timerService);

        Duration delay = Duration.ofMinutes(10);

        delayableTimer.registerTask(
                () -> {
                    // simulate Exception in checkpoint sync phase
                    throw new ExpectedTestException();
                },
                delay);

        timerService.advance(delay.toMillis());

        Throwable t =
                assertThrows(
                        "BarrierAlignmentUtil.DelayableTimer should not hidden exception",
                        Exception.class,
                        () -> mailboxProcessor.runMailboxStep());

        ExceptionUtils.assertThrowable(t, ExpectedTestException.class);
    }
}
