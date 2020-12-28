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

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.MailboxExecutor;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.api.operators.YieldingOperatorFactory;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.OneInputStreamTask;
import org.apache.flink.streaming.runtime.tasks.OneInputStreamTaskTestHarness;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.function.RunnableWithException;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.RejectedExecutionException;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Test to verify that timer triggers are run according to operator precedence (combined with
 * yield() at operator level).
 */
public class MailboxOperatorTest extends TestLogger {

    @Test
    public void testAvoidTaskStarvation() throws Exception {
        final OneInputStreamTaskTestHarness<Integer, Integer> testHarness =
                new OneInputStreamTaskTestHarness<>(
                        OneInputStreamTask::new,
                        BasicTypeInfo.INT_TYPE_INFO,
                        BasicTypeInfo.INT_TYPE_INFO);

        final int maxProcessingElements = 3;

        testHarness
                .setupOperatorChain(
                        new OperatorID(), new ReplicatingMailOperatorFactory(maxProcessingElements))
                .chain(
                        new OperatorID(),
                        new ReplicatingMailOperatorFactory(maxProcessingElements),
                        IntSerializer.INSTANCE)
                .finish();

        testHarness.invoke();
        testHarness.waitForTaskRunning();

        for (int i = 0; i < maxProcessingElements; i++) {
            testHarness.processElement(new StreamRecord<>(0));
        }

        testHarness.endInput();
        testHarness.waitForTaskCompletion();

        // with each input two mails should be processed, one of each operator in the chain
        List<Integer> expected = new ArrayList<>();
        for (int i = 0; i < maxProcessingElements; i++) {
            expected.add(i * 2);
        }
        List<Integer> numMailsProcessed =
                testHarness.getOutput().stream()
                        .map(element -> ((StreamRecord<Integer>) element).getValue())
                        .collect(Collectors.toList());
        assertThat(numMailsProcessed, is(expected));
    }

    private static class ReplicatingMailOperatorFactory
            extends AbstractStreamOperatorFactory<Integer>
            implements OneInputStreamOperatorFactory<Integer, Integer>,
                    YieldingOperatorFactory<Integer> {

        private final int maxProcessingElements;

        private MailboxExecutor mailboxExecutor;

        ReplicatingMailOperatorFactory(final int maxProcessingElements) {
            this.maxProcessingElements = maxProcessingElements;
        }

        @Override
        public void setMailboxExecutor(MailboxExecutor mailboxExecutor) {
            this.mailboxExecutor = mailboxExecutor;
        }

        @Override
        public <Operator extends StreamOperator<Integer>> Operator createStreamOperator(
                StreamOperatorParameters<Integer> parameters) {
            ReplicatingMailOperator operator =
                    new ReplicatingMailOperator(maxProcessingElements, mailboxExecutor);
            operator.setProcessingTimeService(processingTimeService);
            operator.setup(
                    parameters.getContainingTask(),
                    parameters.getStreamConfig(),
                    parameters.getOutput());
            return (Operator) operator;
        }

        @Override
        public void setChainingStrategy(ChainingStrategy strategy) {}

        @Override
        public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
            return ReplicatingMailOperator.class;
        }
    }

    private static class ReplicatingMailOperator extends AbstractStreamOperator<Integer>
            implements OneInputStreamOperator<Integer, Integer> {

        private final int maxProcessingElements;

        private final ReplicatingMail replicatingMail;

        private long numProcessedElements = 0;

        ReplicatingMailOperator(
                final int maxProcessingElements, final MailboxExecutor mailboxExecutor) {
            this.maxProcessingElements = maxProcessingElements;
            this.replicatingMail = new ReplicatingMail(mailboxExecutor);
        }

        @Override
        public void processElement(StreamRecord<Integer> upstreamMailCount) throws Exception {
            if (numProcessedElements >= maxProcessingElements) {
                return;
            }

            // for the very first element, enqueue one mail that replicates itself
            if (!replicatingMail.hasBeenEnqueued()) {
                replicatingMail.run();
            }
            // output how many mails have been processed so far (from upstream and this operator)
            output.collect(
                    new StreamRecord<>(
                            replicatingMail.getMailCount() + upstreamMailCount.getValue()));

            if (++numProcessedElements == maxProcessingElements) {
                replicatingMail.stop();
            }
        }
    }

    private static class ReplicatingMail implements RunnableWithException {
        private int mailCount = -1;

        private boolean stopped = false;

        private final MailboxExecutor mailboxExecutor;

        ReplicatingMail(final MailboxExecutor mailboxExecutor) {
            this.mailboxExecutor = mailboxExecutor;
        }

        @Override
        public void run() {
            try {
                if (!stopped) {
                    mailboxExecutor.execute(this, "Blocking mail" + ++mailCount);
                }
            } catch (RejectedExecutionException e) {
                // during shutdown the executor will reject new mails, which is fine for us.
            }
        }

        boolean hasBeenEnqueued() {
            return mailCount > -1;
        }

        int getMailCount() {
            return mailCount;
        }

        void stop() {
            stopped = true;
        }
    }
}
