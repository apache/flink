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

package org.apache.flink.connector.base.sink;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.util.TestLoggerExtension;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Integration tests of a baseline generic sink that implements the AsyncSinkBase. */
@ExtendWith(TestLoggerExtension.class)
public class AsyncSinkBaseITCase {

    @RegisterExtension
    private static final MiniClusterExtension MINI_CLUSTER_RESOURCE =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(1)
                            .build());

    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    @Test
    public void testWriteTwentyThousandRecordsToGenericSink() throws Exception {
        env.fromSequence(1, 20000).map(Object::toString).sinkTo(new ArrayListAsyncSink());
        env.execute("Integration Test: AsyncSinkBaseITCase").getJobExecutionResult();
    }

    @Test
    public void testFailuresOnPersistingToDestinationAreCaughtAndRaised() {
        env.fromSequence(999_999, 1_000_100)
                .map(Object::toString)
                .sinkTo(new ArrayListAsyncSink(1, 1, 2, 10, 1000, 10));
        assertThatThrownBy(() -> env.execute("Integration Test: AsyncSinkBaseITCase"))
                .isInstanceOf(JobExecutionException.class)
                .rootCause()
                .hasMessageContaining(
                        "Intentional error on persisting 1_000_000 to ArrayListDestination");
    }

    @Test
    public void testThatNoIssuesOccurWhenCheckpointingIsEnabled() throws Exception {
        env.enableCheckpointing(20);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, Time.milliseconds(200)));
        env.fromSequence(1, 10_000).map(Object::toString).sinkTo(new ArrayListAsyncSink());
        env.execute("Integration Test: AsyncSinkBaseITCase");
    }
}
