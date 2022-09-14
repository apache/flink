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

package org.apache.flink.streaming.runtime.operators;

import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.streaming.api.operators.StreamMap;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.OneInputStreamTask;
import org.apache.flink.streaming.runtime.tasks.StreamTaskMailboxTestHarness;
import org.apache.flink.streaming.runtime.tasks.StreamTaskMailboxTestHarnessBuilder;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.function.RunnableWithException;

import org.junit.Test;

import javax.annotation.Nonnull;

import java.util.concurrent.RejectedExecutionException;
import java.util.stream.IntStream;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertThat;

/**
 * Test to verify that timer triggers are run according to operator precedence (combined with
 * yield() at operator level).
 */
public class MailboxOperatorTest extends TestLogger {

    @Test
    public void testAvoidTaskStarvation() throws Exception {
        final int numRecords = 3;

        StreamTaskMailboxTestHarnessBuilder<Integer> builder =
                new StreamTaskMailboxTestHarnessBuilder<>(
                                OneInputStreamTask::new, BasicTypeInfo.INT_TYPE_INFO)
                        .addInput(BasicTypeInfo.INT_TYPE_INFO)
                        .setupOperatorChain(new StreamMap<>(i1 -> i1))
                        .chain(new StreamMap<>(i -> i), IntSerializer.INSTANCE)
                        .finish();

        try (StreamTaskMailboxTestHarness<Integer> testHarness = builder.build()) {
            final ReplicatingMail mail1 = createReplicatingMail(numRecords, testHarness, 0);
            final ReplicatingMail mail2 = createReplicatingMail(numRecords, testHarness, 1);
            for (int i = 0; i < numRecords; i++) {
                testHarness.processElement(new StreamRecord<>(i));
            }

            while (testHarness.getOutput().size() < numRecords) {
                testHarness.processSingleStep();
            }

            final int[] output =
                    testHarness.getOutput().stream()
                            .mapToInt(r -> ((StreamRecord<Integer>) r).getValue())
                            .toArray();
            assertArrayEquals(output, IntStream.range(0, numRecords).toArray());
            assertThat(mail1.getMailCount(), equalTo(numRecords + 1));
            assertThat(mail2.getMailCount(), equalTo(numRecords + 1));
        }
    }

    @Nonnull
    private ReplicatingMail createReplicatingMail(
            int numRecords, StreamTaskMailboxTestHarness<Integer> testHarness, int priority) {
        final MailboxExecutor mailboxExecutor = testHarness.getExecutor(priority);
        final ReplicatingMail mail1 = new ReplicatingMail(mailboxExecutor, numRecords + 1);
        mailboxExecutor.submit(mail1, "Initial mail");
        return mail1;
    }

    private static class ReplicatingMail implements RunnableWithException {
        private int mailCount = -1;
        private final MailboxExecutor mailboxExecutor;
        private final int maxMails;

        ReplicatingMail(final MailboxExecutor mailboxExecutor, int maxMails) {
            this.mailboxExecutor = mailboxExecutor;
            this.maxMails = maxMails;
        }

        @Override
        public void run() {
            try {
                if (mailCount < maxMails) {
                    mailboxExecutor.execute(this, "Blocking mail" + ++mailCount);
                }
            } catch (RejectedExecutionException e) {
                // during shutdown the executor will reject new mails, which is fine for us.
            }
        }

        int getMailCount() {
            return mailCount;
        }
    }
}
