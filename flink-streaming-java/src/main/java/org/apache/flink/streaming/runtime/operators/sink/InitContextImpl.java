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

package org.apache.flink.streaming.runtime.operators.sink;

import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.util.UserCodeClassLoader;

import static org.apache.flink.util.Preconditions.checkNotNull;

class InitContextImpl implements Sink.InitContext {

    private final UserCodeClassLoader userCodeClassLoader;

    private final int subtaskIdx;

    private final int numberOfParallelSubtasks;

    private final ProcessingTimeService processingTimeService;

    private final MailboxExecutor mailboxExecutor;

    private final MetricGroup metricGroup;

    public InitContextImpl(
            UserCodeClassLoader userCodeClassLoader,
            int subtaskIdx,
            int numberOfParallelSubtasks,
            ProcessingTimeService processingTimeService,
            MailboxExecutor mailboxExecutor,
            MetricGroup metricGroup) {
        this.userCodeClassLoader = userCodeClassLoader;
        this.subtaskIdx = subtaskIdx;
        this.numberOfParallelSubtasks = numberOfParallelSubtasks;
        this.mailboxExecutor = mailboxExecutor;
        this.processingTimeService = checkNotNull(processingTimeService);
        this.metricGroup = checkNotNull(metricGroup);
    }

    public static Sink.InitContext of(
            StreamingRuntimeContext runtimeContext,
            ProcessingTimeService processingTimeService,
            MailboxExecutor mailboxExecutor,
            MetricGroup metricGroup) {
        return new InitContextImpl(
                new UserCodeClassLoader() {
                    @Override
                    public ClassLoader asClassLoader() {
                        return runtimeContext.getUserCodeClassLoader();
                    }

                    @Override
                    public void registerReleaseHookIfAbsent(
                            String releaseHookName, Runnable releaseHook) {
                        runtimeContext.registerUserCodeClassLoaderReleaseHookIfAbsent(
                                releaseHookName, releaseHook);
                    }
                },
                runtimeContext.getIndexOfThisSubtask(),
                runtimeContext.getNumberOfParallelSubtasks(),
                processingTimeService,
                mailboxExecutor,
                metricGroup);
    }

    @Override
    public UserCodeClassLoader getUserCodeClassLoader() {
        return userCodeClassLoader;
    }

    @Override
    public int getNumberOfParallelSubtasks() {
        return numberOfParallelSubtasks;
    }

    @Override
    public MailboxExecutor getMailboxExecutor() {
        return mailboxExecutor;
    }

    @Override
    public Sink.ProcessingTimeService getProcessingTimeService() {
        return new ProcessingTimerServiceImpl(processingTimeService);
    }

    @Override
    public int getSubtaskId() {
        return subtaskIdx;
    }

    @Override
    public MetricGroup metricGroup() {
        return metricGroup;
    }

    static class ProcessingTimerServiceImpl implements Sink.ProcessingTimeService {

        private final ProcessingTimeService processingTimeService;

        public ProcessingTimerServiceImpl(ProcessingTimeService processingTimeService) {
            this.processingTimeService = checkNotNull(processingTimeService);
        }

        @Override
        public long getCurrentProcessingTime() {
            return processingTimeService.getCurrentProcessingTime();
        }

        @Override
        public void registerProcessingTimer(
                long time, ProcessingTimeCallback processingTimerCallback) {
            checkNotNull(processingTimerCallback);
            processingTimeService.registerTimer(time, processingTimerCallback::onProcessingTime);
        }
    }
}
