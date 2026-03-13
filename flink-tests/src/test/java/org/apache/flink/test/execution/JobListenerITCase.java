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

package org.apache.flink.test.execution;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobID;
import org.apache.flink.client.deployment.executors.RemoteExecutor;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.JobListener;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.test.junit5.InjectClusterClient;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.util.TestLoggerExtension;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link JobListener}. */
@ExtendWith(TestLoggerExtension.class)
class JobListenerITCase {

    @RegisterExtension
    private static final MiniClusterExtension MINI_CLUSTER_EXTENSION =
            new MiniClusterExtension(new MiniClusterResourceConfiguration.Builder().build());

    private static Configuration getClientConfiguration(ClusterClient<?> clusterClient) {
        Configuration result = new Configuration(clusterClient.getFlinkConfiguration());
        result.set(DeploymentOptions.TARGET, RemoteExecutor.NAME);
        return result;
    }

    @Test
    void testExecuteCallsJobListenerOnBatchEnvironment(
            @InjectClusterClient ClusterClient<?> clusterClient) throws Exception {
        AtomicReference<JobID> jobIdReference = new AtomicReference<>();
        OneShotLatch submissionLatch = new OneShotLatch();
        OneShotLatch executionLatch = new OneShotLatch();

        StreamExecutionEnvironment env =
                new StreamExecutionEnvironment(getClientConfiguration(clusterClient));

        env.registerJobListener(
                new JobListener() {
                    @Override
                    public void onJobSubmitted(JobClient jobClient, Throwable t) {
                        jobIdReference.set(jobClient.getJobID());
                        submissionLatch.trigger();
                    }

                    @Override
                    public void onJobExecuted(
                            JobExecutionResult jobExecutionResult, Throwable throwable) {
                        executionLatch.trigger();
                    }
                });

        env.fromData(1, 2, 3, 4, 5).sinkTo(new DiscardingSink<>());
        JobExecutionResult jobExecutionResult = env.execute();

        submissionLatch.await(2000L, TimeUnit.MILLISECONDS);
        executionLatch.await(2000L, TimeUnit.MILLISECONDS);

        assertThat(jobExecutionResult.getJobID()).isEqualTo(jobIdReference.get());
    }

    @Test
    void testExecuteAsyncCallsJobListenerOnBatchEnvironment(
            @InjectClusterClient ClusterClient<?> clusterClient) throws Exception {
        AtomicReference<JobID> jobIdReference = new AtomicReference<>();
        OneShotLatch submissionLatch = new OneShotLatch();

        StreamExecutionEnvironment env =
                new StreamExecutionEnvironment(getClientConfiguration(clusterClient));

        env.registerJobListener(
                new JobListener() {
                    @Override
                    public void onJobSubmitted(JobClient jobClient, Throwable t) {
                        jobIdReference.set(jobClient.getJobID());
                        submissionLatch.trigger();
                    }

                    @Override
                    public void onJobExecuted(
                            JobExecutionResult jobExecutionResult, Throwable throwable) {}
                });

        env.fromData(1, 2, 3, 4, 5).sinkTo(new DiscardingSink<>());
        JobClient jobClient = env.executeAsync();

        submissionLatch.await(2000L, TimeUnit.MILLISECONDS);
        // when executing asynchronously we don't get an "executed" callback

        assertThat(jobClient.getJobID()).isEqualTo(jobIdReference.get());
    }

    @Test
    void testExecuteCallsJobListenerOnMainThreadOnBatchEnvironment(
            @InjectClusterClient ClusterClient<?> clusterClient) throws Exception {
        AtomicReference<Thread> threadReference = new AtomicReference<>();

        StreamExecutionEnvironment env =
                new StreamExecutionEnvironment(getClientConfiguration(clusterClient));

        env.registerJobListener(
                new JobListener() {
                    @Override
                    public void onJobSubmitted(JobClient jobClient, Throwable t) {
                        threadReference.set(Thread.currentThread());
                    }

                    @Override
                    public void onJobExecuted(
                            JobExecutionResult jobExecutionResult, Throwable throwable) {}
                });

        env.fromData(1, 2, 3, 4, 5).sinkTo(new DiscardingSink<>());
        env.execute();

        assertThat(Thread.currentThread()).isEqualTo(threadReference.get());
    }

    @Test
    void testExecuteAsyncCallsJobListenerOnMainThreadOnBatchEnvironment(
            @InjectClusterClient ClusterClient<?> clusterClient) throws Exception {
        AtomicReference<Thread> threadReference = new AtomicReference<>();

        StreamExecutionEnvironment env =
                new StreamExecutionEnvironment(getClientConfiguration(clusterClient));

        env.registerJobListener(
                new JobListener() {
                    @Override
                    public void onJobSubmitted(JobClient jobClient, Throwable t) {
                        threadReference.set(Thread.currentThread());
                    }

                    @Override
                    public void onJobExecuted(
                            JobExecutionResult jobExecutionResult, Throwable throwable) {}
                });

        env.fromData(1, 2, 3, 4, 5).sinkTo(new DiscardingSink<>());
        env.executeAsync();

        assertThat(Thread.currentThread()).isEqualTo(threadReference.get());
    }

    @Test
    void testExecuteCallsJobListenerOnStreamingEnvironment(
            @InjectClusterClient ClusterClient<?> clusterClient) throws Exception {
        AtomicReference<JobID> jobIdReference = new AtomicReference<>();
        OneShotLatch submissionLatch = new OneShotLatch();
        OneShotLatch executionLatch = new OneShotLatch();

        StreamExecutionEnvironment env =
                new StreamExecutionEnvironment(getClientConfiguration(clusterClient));

        env.registerJobListener(
                new JobListener() {
                    @Override
                    public void onJobSubmitted(JobClient jobClient, Throwable t) {
                        jobIdReference.set(jobClient.getJobID());
                        submissionLatch.trigger();
                    }

                    @Override
                    public void onJobExecuted(
                            JobExecutionResult jobExecutionResult, Throwable throwable) {
                        executionLatch.trigger();
                    }
                });

        env.fromData(1, 2, 3, 4, 5).sinkTo(new DiscardingSink<>());
        JobExecutionResult jobExecutionResult = env.execute();

        submissionLatch.await(2000L, TimeUnit.MILLISECONDS);
        executionLatch.await(2000L, TimeUnit.MILLISECONDS);

        assertThat(jobExecutionResult.getJobID()).isEqualTo(jobIdReference.get());
    }

    @Test
    void testExecuteAsyncCallsJobListenerOnStreamingEnvironment(
            @InjectClusterClient ClusterClient<?> clusterClient) throws Exception {
        AtomicReference<JobID> jobIdReference = new AtomicReference<>();
        OneShotLatch submissionLatch = new OneShotLatch();

        StreamExecutionEnvironment env =
                new StreamExecutionEnvironment(getClientConfiguration(clusterClient));

        env.registerJobListener(
                new JobListener() {
                    @Override
                    public void onJobSubmitted(JobClient jobClient, Throwable t) {
                        jobIdReference.set(jobClient.getJobID());
                        submissionLatch.trigger();
                    }

                    @Override
                    public void onJobExecuted(
                            JobExecutionResult jobExecutionResult, Throwable throwable) {}
                });

        env.fromData(1, 2, 3, 4, 5).sinkTo(new DiscardingSink<>());
        JobClient jobClient = env.executeAsync();

        submissionLatch.await(2000L, TimeUnit.MILLISECONDS);
        // when executing asynchronously we don't get an "executed" callback

        assertThat(jobClient.getJobID()).isEqualTo(jobIdReference.get());
    }

    @Test
    void testExecuteCallsJobListenerOnMainThreadOnStreamEnvironment(
            @InjectClusterClient ClusterClient<?> clusterClient) throws Exception {
        AtomicReference<Thread> threadReference = new AtomicReference<>();

        StreamExecutionEnvironment env =
                new StreamExecutionEnvironment(getClientConfiguration(clusterClient));

        env.registerJobListener(
                new JobListener() {
                    @Override
                    public void onJobSubmitted(JobClient jobClient, Throwable t) {
                        threadReference.set(Thread.currentThread());
                    }

                    @Override
                    public void onJobExecuted(
                            JobExecutionResult jobExecutionResult, Throwable throwable) {}
                });

        env.fromData(1, 2, 3, 4, 5).sinkTo(new DiscardingSink<>());
        env.execute();

        assertThat(Thread.currentThread()).isEqualTo(threadReference.get());
    }

    @Test
    void testExecuteAsyncCallsJobListenerOnMainThreadOnStreamEnvironment(
            @InjectClusterClient ClusterClient<?> clusterClient) throws Exception {
        AtomicReference<Thread> threadReference = new AtomicReference<>();

        StreamExecutionEnvironment env =
                new StreamExecutionEnvironment(getClientConfiguration(clusterClient));

        env.registerJobListener(
                new JobListener() {
                    @Override
                    public void onJobSubmitted(JobClient jobClient, Throwable t) {
                        threadReference.set(Thread.currentThread());
                    }

                    @Override
                    public void onJobExecuted(
                            JobExecutionResult jobExecutionResult, Throwable throwable) {}
                });

        env.fromData(1, 2, 3, 4, 5).sinkTo(new DiscardingSink<>());
        env.executeAsync();

        assertThat(Thread.currentThread()).isEqualTo(threadReference.get());
    }
}
