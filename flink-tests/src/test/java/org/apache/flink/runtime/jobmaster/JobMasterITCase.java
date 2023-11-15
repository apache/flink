/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.jobmaster;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobGraphTestUtils;
import org.apache.flink.runtime.testutils.MiniClusterResource;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import static org.apache.flink.core.testutils.FlinkMatchers.containsMessage;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/** Integration tests for the {@link JobMaster}. */
public class JobMasterITCase extends TestLogger {

    @Test
    public void testRejectionOfEmptyJobGraphs() throws Exception {
        MiniClusterResource miniCluster =
                new MiniClusterResource(
                        new MiniClusterResourceConfiguration.Builder()
                                .setNumberTaskManagers(1)
                                .setNumberSlotsPerTaskManager(1)
                                .build());
        miniCluster.before();
        JobGraph jobGraph = JobGraphTestUtils.emptyJobGraph();

        try {
            miniCluster.getMiniCluster().submitJob(jobGraph).get();
            fail("Expect failure");
        } catch (Throwable t) {
            assertThat(t, containsMessage("The given job is empty"));
        } finally {
            miniCluster.after();
        }
    }

    /**
     * This test is to guard against the issue reported in FLINK-22001, where any exception from the
     * JobManager initialization was not forwarded to the user.
     *
     * <p>TODO: This test relies on an internal error. Replace it with a more robust approach.
     */
    @Test
    public void testJobManagerInitializationExceptionsAreForwardedToTheUser() {
        // we must use the LocalStreamEnvironment to reproduce this issue.
        // It passes with the TestStreamEnvironment (which is initialized by the
        // MiniClusterResource). The LocalStreamEnvironment is polling the JobManager for the job
        // status, while TestStreamEnvironment is waiting on the resultFuture.
        StreamExecutionEnvironment see = StreamExecutionEnvironment.createLocalEnvironment();

        Source<String, MockSplit, Void> mySource = new FailOnInitializationSource();
        DataStream<String> stream =
                see.fromSource(mySource, WatermarkStrategy.noWatermarks(), "MySourceName");
        stream.sinkTo(new DiscardingSink<>());

        try {
            see.execute();
        } catch (Exception e) {
            assertThat(e, containsMessage("Context was not yet initialized"));
        }
    }

    private static class FailOnInitializationSource implements Source<String, MockSplit, Void> {
        @Override
        public Boundedness getBoundedness() {
            return Boundedness.CONTINUOUS_UNBOUNDED;
        }

        @Override
        public SourceReader<String, MockSplit> createReader(SourceReaderContext readerContext)
                throws Exception {
            throw new UnsupportedOperationException();
        }

        @Override
        public SplitEnumerator<MockSplit, Void> createEnumerator(
                SplitEnumeratorContext<MockSplit> enumContext) throws Exception {
            throw new RuntimeException();
        }

        @Override
        public SplitEnumerator<MockSplit, Void> restoreEnumerator(
                SplitEnumeratorContext<MockSplit> enumContext, Void checkpoint) throws Exception {
            throw new UnsupportedOperationException();
        }

        @Override
        public SimpleVersionedSerializer<MockSplit> getSplitSerializer() {
            throw new RuntimeException();
        }

        @Override
        public SimpleVersionedSerializer<Void> getEnumeratorCheckpointSerializer() {
            throw new UnsupportedOperationException();
        }
    }

    private static class MockSplit implements SourceSplit {
        @Override
        public String splitId() {
            throw new UnsupportedOperationException();
        }
    }
}
