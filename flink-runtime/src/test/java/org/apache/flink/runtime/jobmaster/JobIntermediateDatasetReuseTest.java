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

package org.apache.flink.runtime.jobmaster;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.io.network.api.reader.RecordReader;
import org.apache.flink.runtime.io.network.api.writer.RecordWriter;
import org.apache.flink.runtime.io.network.api.writer.RecordWriterBuilder;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.minicluster.TestingMiniCluster;
import org.apache.flink.runtime.minicluster.TestingMiniClusterConfiguration;
import org.apache.flink.runtime.scheduler.CachedIntermediateDataSetCorruptedException;
import org.apache.flink.types.IntValue;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/** Integration tests for reusing persisted intermediate dataset */
public class JobIntermediateDatasetReuseTest {

    private static final Logger LOG =
            LoggerFactory.getLogger(JobIntermediateDatasetReuseTest.class);

    @Test
    public void testClusterPartitionReuse() throws Exception {
        internalTestClusterPartitionReuse(1, 1, jobResult -> assertTrue(jobResult.isSuccess()));
    }

    @Test
    public void testClusterPartitionReuseMultipleParallelism() throws Exception {
        internalTestClusterPartitionReuse(64, 64, jobResult -> assertTrue(jobResult.isSuccess()));
    }

    @Test
    public void testClusterPartitionReuseWithMoreConsumerParallelismThrowException()
            throws Exception {
        internalTestClusterPartitionReuse(
                1,
                2,
                jobResult -> {
                    assertFalse(jobResult.isSuccess());
                    assertNotNull(getCachedIntermediateDataSetCorruptedException(jobResult));
                });
    }

    @Test
    public void testClusterPartitionReuseWithLessConsumerParallelismThrowException()
            throws Exception {
        internalTestClusterPartitionReuse(
                2,
                1,
                jobResult -> {
                    assertFalse(jobResult.isSuccess());
                    assertNotNull(getCachedIntermediateDataSetCorruptedException(jobResult));
                });
    }

    private void internalTestClusterPartitionReuse(
            int producerParallelism,
            int consumerParallelism,
            Consumer<JobResult> jobResultVerification)
            throws Exception {
        final TestingMiniClusterConfiguration miniClusterConfiguration =
                TestingMiniClusterConfiguration.newBuilder().build();

        try (TestingMiniCluster miniCluster =
                TestingMiniCluster.newBuilder(miniClusterConfiguration).build()) {
            miniCluster.start();

            IntermediateDataSetID intermediateDataSetID = new IntermediateDataSetID();
            final JobGraph firstJobGraph =
                    createFirstJobGraph(producerParallelism, intermediateDataSetID);
            miniCluster.submitJob(firstJobGraph).get();
            CompletableFuture<JobResult> jobResultFuture =
                    miniCluster.requestJobResult(firstJobGraph.getJobID());
            JobResult jobResult = jobResultFuture.get();
            assertTrue(jobResult.isSuccess());

            final JobGraph secondJobGraph =
                    createSecondJobGraph(consumerParallelism, intermediateDataSetID);
            miniCluster.submitJob(secondJobGraph).get();
            jobResultFuture = miniCluster.requestJobResult(secondJobGraph.getJobID());
            jobResult = jobResultFuture.get();
            jobResultVerification.accept(jobResult);
        }
    }

    @Test
    public void testClusterPartitionReuseWithTMFail() throws Exception {
        final TestingMiniClusterConfiguration miniClusterConfiguration =
                TestingMiniClusterConfiguration.newBuilder().build();

        try (TestingMiniCluster miniCluster =
                TestingMiniCluster.newBuilder(miniClusterConfiguration).build()) {
            miniCluster.start();

            IntermediateDataSetID intermediateDataSetID = new IntermediateDataSetID();
            final JobGraph firstJobGraph = createFirstJobGraph(1, intermediateDataSetID);
            miniCluster.submitJob(firstJobGraph).get();
            CompletableFuture<JobResult> jobResultFuture =
                    miniCluster.requestJobResult(firstJobGraph.getJobID());
            JobResult jobResult = jobResultFuture.get();
            assertTrue(jobResult.isSuccess());

            miniCluster.terminateTaskManager(0);
            miniCluster.startTaskManager();

            final JobGraph secondJobGraph = createSecondJobGraph(1, intermediateDataSetID);
            final ExecutionConfig executionConfig = new ExecutionConfig();
            executionConfig.setRestartStrategy(RestartStrategies.fixedDelayRestart(1024, 1000));
            secondJobGraph.setExecutionConfig(executionConfig);
            miniCluster.submitJob(secondJobGraph).get();
            jobResultFuture = miniCluster.requestJobResult(secondJobGraph.getJobID());
            jobResult = jobResultFuture.get();
            assertFalse(jobResult.isSuccess());
            final CachedIntermediateDataSetCorruptedException exception =
                    getCachedIntermediateDataSetCorruptedException(jobResult);
            assertNotNull(exception);
            assertEquals(
                    intermediateDataSetID, exception.getCorruptedIntermediateDataSetID().get(0));

            firstJobGraph.setJobID(new JobID());
            miniCluster.submitJob(firstJobGraph).get();
            jobResultFuture = miniCluster.requestJobResult(firstJobGraph.getJobID());
            jobResult = jobResultFuture.get();
            assertTrue(jobResult.isSuccess());

            secondJobGraph.setJobID(new JobID());
            miniCluster.submitJob(secondJobGraph).get();
            jobResultFuture = miniCluster.requestJobResult(secondJobGraph.getJobID());
            jobResult = jobResultFuture.get();
            assertTrue(jobResult.isSuccess());
        }
    }

    private CachedIntermediateDataSetCorruptedException
            getCachedIntermediateDataSetCorruptedException(JobResult jobResult) {
        assertTrue(jobResult.getSerializedThrowable().isPresent());
        Throwable throwable =
                jobResult
                        .getSerializedThrowable()
                        .get()
                        .deserializeError(Thread.currentThread().getContextClassLoader());
        while (throwable != null) {
            if (throwable instanceof CachedIntermediateDataSetCorruptedException) {
                return (CachedIntermediateDataSetCorruptedException) throwable;
            }
            throwable = throwable.getCause();
        }
        return null;
    }

    private JobGraph createSecondJobGraph(
            int parallelism, IntermediateDataSetID intermediateDataSetID) {
        final JobVertex receiver = new JobVertex("Receiver 2", null);
        receiver.setParallelism(parallelism);
        receiver.setInvokableClass(Receiver.class);
        receiver.addIntermediateDataSetIdToConsume(intermediateDataSetID);

        return new JobGraph(null, "Second Job", receiver);
    }

    private JobGraph createFirstJobGraph(
            int parallelism, IntermediateDataSetID intermediateDataSetID) {
        final JobVertex sender = new JobVertex("Sender");
        sender.setParallelism(parallelism);
        sender.setInvokableClass(Sender.class);

        final JobVertex receiver = new JobVertex("Receiver");
        receiver.setParallelism(parallelism);
        receiver.setInvokableClass(Receiver.class);

        receiver.connectNewDataSetAsInput(
                sender,
                DistributionPattern.POINTWISE,
                ResultPartitionType.BLOCKING_PERSISTENT,
                intermediateDataSetID);

        return new JobGraph(null, "First Job", sender, receiver);
    }

    /**
     * Basic sender {@link AbstractInvokable} which sends 100 record base on its index to down
     * stream.
     */
    public static class Sender extends AbstractInvokable {

        public Sender(Environment environment) {
            super(environment);
        }

        @Override
        public void invoke() throws Exception {
            int index = getIndexInSubtaskGroup();
            final RecordWriter<IntValue> writer =
                    new RecordWriterBuilder<IntValue>().build(getEnvironment().getWriter(0));

            try {
                for (int i = index; i < index + 100; ++i) {
                    writer.emit(new IntValue(i));
                    LOG.debug("Sender({}) emit {}", index, i);
                }
                writer.flushAll();
            } finally {
                writer.close();
            }
        }
    }

    /**
     * Basic receiver {@link AbstractInvokable} which verifies the sent elements from the {@link
     * Sender}.
     */
    public static class Receiver extends AbstractInvokable {

        public Receiver(Environment environment) {
            super(environment);
        }

        @Override
        public void invoke() throws Exception {
            int index = getIndexInSubtaskGroup();
            final RecordReader<IntValue> reader =
                    new RecordReader<>(
                            getEnvironment().getInputGate(0),
                            IntValue.class,
                            getEnvironment().getTaskManagerInfo().getTmpDirectories());
            for (int i = index; i < index + 100; ++i) {
                final int value = reader.next().getValue();
                LOG.debug("Receiver({}) received {}", index, value);
                assertEquals(i, value);
            }

            assertNull(reader.next());
        }
    }
}
