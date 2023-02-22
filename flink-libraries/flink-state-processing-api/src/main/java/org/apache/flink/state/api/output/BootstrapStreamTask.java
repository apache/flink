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

package org.apache.flink.state.api.output;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.state.api.runtime.NeverFireProcessingTimeService;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperatorFactoryUtil;
import org.apache.flink.streaming.runtime.io.RecordProcessorUtils;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.streaming.runtime.tasks.mailbox.MailboxDefaultAction;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.ThrowingConsumer;

import java.util.Optional;
import java.util.concurrent.BlockingQueue;

/**
 * A stream task that pulls elements from a {@link BlockingQueue} instead of the network. After all
 * elements are processed the task takes a snapshot of the subtask operator state.
 *
 * @param <IN> Type of the input.
 * @param <OUT> Type of the output.
 * @param <OP> Type of the operator this task runs.
 */
@Internal
class BootstrapStreamTask<IN, OUT, OP extends OneInputStreamOperator<IN, OUT> & BoundedOneInput>
        extends StreamTask<OUT, OP> {

    private final BlockingQueue<StreamElement> input;

    private final Output<StreamRecord<OUT>> output;

    private ThrowingConsumer<StreamRecord<IN>, Exception> recordProcessor;

    BootstrapStreamTask(
            Environment environment,
            BlockingQueue<StreamElement> input,
            Output<StreamRecord<OUT>> output)
            throws Exception {
        super(environment, new NeverFireProcessingTimeService());
        this.input = input;
        this.output = output;
    }

    @Override
    protected void init() throws Exception {
        Preconditions.checkState(
                operatorChain.getNumberOfOperators() == 1,
                "BoundedStreamTask's should only run a single operator");

        // re-initialize the operator with the correct collector.
        StreamOperatorFactory<OUT> operatorFactory =
                configuration.getStreamOperatorFactory(getUserCodeClassLoader());
        Tuple2<OP, Optional<ProcessingTimeService>> mainOperatorAndTimeService =
                StreamOperatorFactoryUtil.createOperator(
                        operatorFactory,
                        this,
                        configuration,
                        output,
                        operatorChain.getOperatorEventDispatcher());
        mainOperator = mainOperatorAndTimeService.f0;
        mainOperator.initializeState(createStreamTaskStateInitializer());
        mainOperator.open();
        recordProcessor = RecordProcessorUtils.getRecordProcessor(mainOperator);
    }

    @Override
    protected void processInput(MailboxDefaultAction.Controller controller) throws Exception {
        StreamElement element = input.take();
        if (element.isRecord()) {
            recordProcessor.accept(element.asRecord());
        } else {
            mainOperator.endInput();
            mainOperator.finish();
            controller.suspendDefaultAction();
            mailboxProcessor.suspend();
        }
    }

    @Override
    protected void cancelTask() {}

    @Override
    protected void cleanUpInternal() throws Exception {
        mainOperator.close();
    }
}
