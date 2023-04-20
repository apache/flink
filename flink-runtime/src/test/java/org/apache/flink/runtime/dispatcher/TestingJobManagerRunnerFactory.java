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

package org.apache.flink.runtime.dispatcher;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.jobmaster.TestingJobManagerRunner;
import org.apache.flink.util.Preconditions;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Testing implementation of {@link JobManagerRunnerFactory} which returns a {@link
 * TestingJobManagerRunner}.
 */
public class TestingJobManagerRunnerFactory {

    private final BlockingQueue<TestingJobManagerRunner> createdJobManagerRunner =
            new ArrayBlockingQueue<>(16);

    private final AtomicInteger numBlockingJobManagerRunners;

    protected TestingJobManagerRunnerFactory(int numBlockingJobManagerRunners) {
        this.numBlockingJobManagerRunners = new AtomicInteger(numBlockingJobManagerRunners);
    }

    protected TestingJobManagerRunner offerTestingJobManagerRunner(JobID jobId) {
        final TestingJobManagerRunner testingJobManagerRunner =
                createTestingJobManagerRunner(jobId);
        Preconditions.checkState(
                createdJobManagerRunner.offer(testingJobManagerRunner),
                "Unable to persist created the new runner.");
        return testingJobManagerRunner;
    }

    private TestingJobManagerRunner createTestingJobManagerRunner(JobID jobId) {
        final boolean blockingTermination = numBlockingJobManagerRunners.getAndDecrement() > 0;
        return TestingJobManagerRunner.newBuilder()
                .setJobId(jobId)
                .setBlockingTermination(blockingTermination)
                .build();
    }

    public TestingJobManagerRunner takeCreatedJobManagerRunner() throws InterruptedException {
        return createdJobManagerRunner.take();
    }

    public int getQueueSize() {
        return createdJobManagerRunner.size();
    }
}
