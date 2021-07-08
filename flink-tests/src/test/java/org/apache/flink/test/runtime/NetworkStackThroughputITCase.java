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

package org.apache.flink.test.runtime;

import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.io.network.api.reader.RecordReader;
import org.apache.flink.runtime.io.network.api.writer.RecordWriter;
import org.apache.flink.runtime.io.network.api.writer.RecordWriterBuilder;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobGraphTestUtils;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.util.TestLogger;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/** Manually test the throughput of the network stack. */
public class NetworkStackThroughputITCase extends TestLogger {

    private static final Logger LOG = LoggerFactory.getLogger(NetworkStackThroughputITCase.class);

    private static final String DATA_VOLUME_GB_CONFIG_KEY = "data.volume.gb";

    private static final String IS_SLOW_SENDER_CONFIG_KEY = "is.slow.sender";

    private static final String IS_SLOW_RECEIVER_CONFIG_KEY = "is.slow.receiver";

    private static final int IS_SLOW_SLEEP_MS = 10;

    private static final int IS_SLOW_EVERY_NUM_RECORDS =
            (2 * 32 * 1024) / SpeedTestRecord.RECORD_SIZE;

    // ------------------------------------------------------------------------

    /**
     * Invokable that produces records and allows slowdown via {@link #IS_SLOW_EVERY_NUM_RECORDS}
     * and {@link #IS_SLOW_SENDER_CONFIG_KEY} and creates records of different data sizes via {@link
     * #DATA_VOLUME_GB_CONFIG_KEY}.
     *
     * <p>NOTE: needs to be <tt>public</tt> so that a task can be run with this!
     */
    public static class SpeedTestProducer extends AbstractInvokable {

        public SpeedTestProducer(Environment environment) {
            super(environment);
        }

        @Override
        public void invoke() throws Exception {
            RecordWriter<SpeedTestRecord> writer =
                    new RecordWriterBuilder<SpeedTestRecord>().build(getEnvironment().getWriter(0));

            try {
                // Determine the amount of data to send per subtask
                int dataVolumeGb =
                        getTaskConfiguration()
                                .getInteger(
                                        NetworkStackThroughputITCase.DATA_VOLUME_GB_CONFIG_KEY, 1);

                long dataMbPerSubtask = (dataVolumeGb * 10) / getCurrentNumberOfSubtasks();
                long numRecordsToEmit =
                        (dataMbPerSubtask * 1024 * 1024) / SpeedTestRecord.RECORD_SIZE;

                LOG.info(
                        String.format(
                                "%d/%d: Producing %d records (each record: %d bytes, total: %.2f GB)",
                                getIndexInSubtaskGroup() + 1,
                                getCurrentNumberOfSubtasks(),
                                numRecordsToEmit,
                                SpeedTestRecord.RECORD_SIZE,
                                dataMbPerSubtask / 1024.0));

                boolean isSlow =
                        getTaskConfiguration().getBoolean(IS_SLOW_SENDER_CONFIG_KEY, false);

                int numRecords = 0;
                SpeedTestRecord record = new SpeedTestRecord();
                for (long i = 0; i < numRecordsToEmit; i++) {
                    if (isSlow && (numRecords++ % IS_SLOW_EVERY_NUM_RECORDS) == 0) {
                        Thread.sleep(IS_SLOW_SLEEP_MS);
                    }

                    writer.emit(record);
                }
            } finally {
                writer.close();
                writer.flushAll();
            }
        }
    }

    /**
     * Invokable that forwards incoming records.
     *
     * <p>NOTE: needs to be <tt>public</tt> so that a task can be run with this!
     */
    public static class SpeedTestForwarder extends AbstractInvokable {

        public SpeedTestForwarder(Environment environment) {
            super(environment);
        }

        @Override
        public void invoke() throws Exception {
            RecordReader<SpeedTestRecord> reader =
                    new RecordReader<>(
                            getEnvironment().getInputGate(0),
                            SpeedTestRecord.class,
                            getEnvironment().getTaskManagerInfo().getTmpDirectories());

            RecordWriter<SpeedTestRecord> writer =
                    new RecordWriterBuilder<SpeedTestRecord>().build(getEnvironment().getWriter(0));

            try {
                SpeedTestRecord record;
                while ((record = reader.next()) != null) {
                    writer.emit(record);
                }
            } finally {
                reader.clearBuffers();
                writer.close();
                writer.flushAll();
            }
        }
    }

    /**
     * Invokable that consumes incoming records and allows slowdown via {@link
     * #IS_SLOW_EVERY_NUM_RECORDS}.
     *
     * <p>NOTE: needs to be <tt>public</tt> so that a task can be run with this!
     */
    public static class SpeedTestConsumer extends AbstractInvokable {

        public SpeedTestConsumer(Environment environment) {
            super(environment);
        }

        @Override
        public void invoke() throws Exception {
            RecordReader<SpeedTestRecord> reader =
                    new RecordReader<>(
                            getEnvironment().getInputGate(0),
                            SpeedTestRecord.class,
                            getEnvironment().getTaskManagerInfo().getTmpDirectories());

            try {
                boolean isSlow =
                        getTaskConfiguration().getBoolean(IS_SLOW_RECEIVER_CONFIG_KEY, false);

                int numRecords = 0;
                while (reader.next() != null) {
                    if (isSlow && (numRecords++ % IS_SLOW_EVERY_NUM_RECORDS) == 0) {
                        Thread.sleep(IS_SLOW_SLEEP_MS);
                    }
                }
            } finally {
                reader.clearBuffers();
            }
        }
    }

    /**
     * Record type for the speed test.
     *
     * <p>NOTE: needs to be <tt>public</tt> to allow deserialization!
     */
    public static class SpeedTestRecord implements IOReadableWritable {

        private static final int RECORD_SIZE = 128;

        private final byte[] buf = new byte[RECORD_SIZE];

        public SpeedTestRecord() {
            for (int i = 0; i < RECORD_SIZE; ++i) {
                this.buf[i] = (byte) (i % 128);
            }
        }

        @Override
        public void write(DataOutputView out) throws IOException {
            out.write(this.buf);
        }

        @Override
        public void read(DataInputView in) throws IOException {
            in.readFully(this.buf);
        }
    }

    // ------------------------------------------------------------------------

    @Test
    public void testThroughput() throws Exception {
        Object[][] configParams =
                new Object[][] {
                    new Object[] {1, false, false, false, 4, 2},
                    new Object[] {1, true, false, false, 4, 2},
                    new Object[] {1, true, true, false, 4, 2},
                    new Object[] {1, true, false, true, 4, 2},
                    new Object[] {2, true, false, false, 4, 2},
                    new Object[] {4, true, false, false, 4, 2},
                    new Object[] {4, true, false, false, 8, 4},
                };

        for (Object[] p : configParams) {
            final int dataVolumeGb = (Integer) p[0];
            final boolean useForwarder = (Boolean) p[1];
            final boolean isSlowSender = (Boolean) p[2];
            final boolean isSlowReceiver = (Boolean) p[3];
            final int parallelism = (Integer) p[4];
            final int numSlotsPerTaskManager = (Integer) p[5];

            if (parallelism % numSlotsPerTaskManager != 0) {
                throw new RuntimeException(
                        "The test case defines a parallelism that is not a multiple of the slots per task manager.");
            }

            final int numTaskManagers = parallelism / numSlotsPerTaskManager;

            final MiniClusterWithClientResource cluster =
                    new MiniClusterWithClientResource(
                            new MiniClusterResourceConfiguration.Builder()
                                    .setNumberTaskManagers(numTaskManagers)
                                    .setNumberSlotsPerTaskManager(numSlotsPerTaskManager)
                                    .build());
            cluster.before();

            try {
                System.out.println(
                        String.format(
                                "Running test with parameters: dataVolumeGB=%s, useForwarder=%s, isSlowSender=%s, isSlowReceiver=%s, parallelism=%s, numSlotsPerTM=%s",
                                dataVolumeGb,
                                useForwarder,
                                isSlowSender,
                                isSlowReceiver,
                                parallelism,
                                numSlotsPerTaskManager));
                testProgram(
                        cluster,
                        dataVolumeGb,
                        useForwarder,
                        isSlowSender,
                        isSlowReceiver,
                        parallelism);
            } finally {
                cluster.after();
            }
        }
    }

    private void testProgram(
            final MiniClusterWithClientResource cluster,
            final int dataVolumeGb,
            final boolean useForwarder,
            final boolean isSlowSender,
            final boolean isSlowReceiver,
            final int parallelism)
            throws Exception {
        final ClusterClient<?> client = cluster.getClusterClient();
        final JobGraph jobGraph =
                createJobGraph(
                        dataVolumeGb, useForwarder, isSlowSender, isSlowReceiver, parallelism);
        final JobResult jobResult =
                client.submitJob(jobGraph).thenCompose(client::requestJobResult).get();

        Assert.assertFalse(jobResult.getSerializedThrowable().isPresent());

        final long dataVolumeMbit = dataVolumeGb * 8192;
        final long runtimeSecs =
                TimeUnit.SECONDS.convert(jobResult.getNetRuntime(), TimeUnit.MILLISECONDS);
        final int mbitPerSecond = (int) (((double) dataVolumeMbit) / runtimeSecs);

        LOG.info(
                String.format(
                        "Test finished with throughput of %d MBit/s (runtime [secs]: %d, "
                                + "data volume [gb/mbits]: %d/%d)",
                        mbitPerSecond, runtimeSecs, dataVolumeGb, dataVolumeMbit));
    }

    private JobGraph createJobGraph(
            int dataVolumeGb,
            boolean useForwarder,
            boolean isSlowSender,
            boolean isSlowReceiver,
            int numSubtasks) {
        SlotSharingGroup sharingGroup = new SlotSharingGroup();

        final List<JobVertex> jobVertices = new ArrayList<>();

        JobVertex producer = new JobVertex("Speed Test Producer");
        producer.setSlotSharingGroup(sharingGroup);

        producer.setInvokableClass(SpeedTestProducer.class);
        producer.setParallelism(numSubtasks);
        producer.getConfiguration().setInteger(DATA_VOLUME_GB_CONFIG_KEY, dataVolumeGb);
        producer.getConfiguration().setBoolean(IS_SLOW_SENDER_CONFIG_KEY, isSlowSender);

        jobVertices.add(producer);

        JobVertex forwarder = null;
        if (useForwarder) {
            forwarder = new JobVertex("Speed Test Forwarder");
            forwarder.setSlotSharingGroup(sharingGroup);

            forwarder.setInvokableClass(SpeedTestForwarder.class);
            forwarder.setParallelism(numSubtasks);
            jobVertices.add(forwarder);
        }

        JobVertex consumer = new JobVertex("Speed Test Consumer");
        consumer.setSlotSharingGroup(sharingGroup);

        consumer.setInvokableClass(SpeedTestConsumer.class);
        consumer.setParallelism(numSubtasks);
        consumer.getConfiguration().setBoolean(IS_SLOW_RECEIVER_CONFIG_KEY, isSlowReceiver);

        jobVertices.add(consumer);

        if (useForwarder) {
            forwarder.connectNewDataSetAsInput(
                    producer, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);
            consumer.connectNewDataSetAsInput(
                    forwarder, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);
        } else {
            consumer.connectNewDataSetAsInput(
                    producer, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);
        }

        return JobGraphTestUtils.streamingJobGraph(jobVertices.toArray(new JobVertex[0]));
    }

    public static void main(String[] args) throws Exception {
        new NetworkStackThroughputITCase().testThroughput();

        System.out.println("Done.");
    }
}
