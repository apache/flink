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

package org.apache.flink.test.checkpointing;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.changelog.fs.FsStateChangelogStorageFactory;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.operators.lifecycle.TestJobExecutor;
import org.apache.flink.runtime.operators.lifecycle.TestJobWithDescription;
import org.apache.flink.runtime.operators.lifecycle.command.TestCommandDispatcher;
import org.apache.flink.runtime.operators.lifecycle.event.CheckpointCompletedEvent;
import org.apache.flink.runtime.operators.lifecycle.event.OperatorStartedEvent;
import org.apache.flink.runtime.operators.lifecycle.event.TestEventQueue;
import org.apache.flink.runtime.operators.lifecycle.graph.OneInputTestStreamOperatorFactory;
import org.apache.flink.runtime.operators.lifecycle.graph.TestDataElement;
import org.apache.flink.runtime.operators.lifecycle.graph.TestEventSource;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamUtils;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.streaming.util.RestartStrategyUtils;
import org.apache.flink.test.junit5.InjectMiniCluster;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.testutils.junit.SharedObjectsExtension;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.apache.flink.runtime.operators.lifecycle.command.TestCommand.DELAY_SNAPSHOT;
import static org.apache.flink.runtime.operators.lifecycle.command.TestCommand.FAIL_SNAPSHOT;
import static org.apache.flink.runtime.operators.lifecycle.command.TestCommand.FINISH_SOURCES;
import static org.apache.flink.runtime.operators.lifecycle.command.TestCommandDispatcher.TestCommandScope.ALL_SUBTASKS;
import static org.apache.flink.runtime.operators.lifecycle.command.TestCommandDispatcher.TestCommandScope.SINGLE_SUBTASK;

/**
 * A test suite to check the ability to recover from incremental checkpoint after the next one
 * failed, potentially discarding shared state. A chain of two keyed operators is created and
 * checkpointed as follows:
 *
 * <pre>
 * |                  | Head op.   | Tail op.    |
 * | Checkpoint 1     | empty      | File 1      |
 * | Checkpoint 2     | fail       | File 1 + 2  |
 * | Recover from CP1 |            | read File 1 |
 * </pre>
 */
class IncrementalStateReuseAfterFailureITCase {

    @RegisterExtension
    private final SharedObjectsExtension sharedObjects = SharedObjectsExtension.create();

    private static final String UID_SRC = asUidHash(0);
    private static final String UID_OP1 = asUidHash(1);
    private static final String UID_OP2 = asUidHash(2);

    @TempDir private static Path temporaryFolder;

    @RegisterExtension
    private static final MiniClusterExtension MINI_CLUSTER_RESOURCE =
            new MiniClusterExtension(
                    () ->
                            new MiniClusterResourceConfiguration.Builder()
                                    .setConfiguration(configuration())
                                    .setNumberTaskManagers(1)
                                    .setNumberSlotsPerTaskManager(4)
                                    .build());

    private static Configuration configuration() {
        Configuration conf = new Configuration();
        FsStateChangelogStorageFactory.configure(
                conf, temporaryFolder.toFile(), Duration.ofMinutes(1), 10);
        return conf;
    }

    @Test
    void testChangelogStateReuse(@InjectMiniCluster MiniCluster miniCluster) throws Exception {
        TestJobExecutor.execute(createJob(), miniCluster)
                .waitForAllRunning()

                // 1st checkpoint: accumulate some state and snapshot it
                .waitForEvent(CheckpointCompletedEvent.class)

                // 2nd checkpoint: try to discard incremental state of the 1st checkpoint
                // First, delay the 1st operator in chain (which is snapshotted last); this allows
                // uploads to start. Otherwise, upload futures are cancelled and state is not
                // discarded (see StateUtil.discardStateFuture)
                .sendOperatorCommand(UID_OP1, DELAY_SNAPSHOT, SINGLE_SUBTASK)
                // Now fail this operator snapshot - and discard other operators snapshots
                // (SubtaskCheckpointCoordinatorImpl.cleanup and AsyncCheckpointRunnable.cleanup)
                .sendOperatorCommand(UID_OP1, FAIL_SNAPSHOT, SINGLE_SUBTASK)

                // Expect successful recovery from the 1st checkpoint.
                .waitForEvent(OperatorStartedEvent.class)
                .waitForAllRunning()
                .waitForEvent(CheckpointCompletedEvent.class)
                .sendBroadcastCommand(FINISH_SOURCES, ALL_SUBTASKS)
                .waitForTermination()
                .assertFinishedSuccessfully();
    }

    private TestJobWithDescription createJob() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(200);

        // reliably fails Changelog with FLINK-25395, but might affect any incremental backend
        env.enableChangelogStateBackend(true);
        RestartStrategyUtils.configureFixedDelayRestartStrategy(env, 1, 1L);
        env.setMaxParallelism(1); // simplify debugging
        env.setParallelism(1); // simplify debugging

        TestEventQueue evQueue = TestEventQueue.createShared(sharedObjects);
        TestCommandDispatcher cmdQueue = TestCommandDispatcher.createShared(sharedObjects);

        DataStream<TestDataElement> src =
                env.addSource(new TestEventSource(UID_SRC, evQueue, cmdQueue)).setUidHash(UID_SRC);

        SingleOutputStreamOperator<TestDataElement> transform1 =
                src.keyBy(x -> x)
                        .transform(
                                "transform-1",
                                TypeInformation.of(TestDataElement.class),
                                new OneInputTestStreamOperatorFactory(UID_OP1, evQueue, cmdQueue))
                        .setUidHash(UID_OP1);

        SingleOutputStreamOperator<TestDataElement> transform2 =
                // chain two keyed operators, so that one is checkpointed and the other one fails
                DataStreamUtils.reinterpretAsKeyedStream(transform1, x -> x)
                        .transform(
                                "transform-2",
                                TypeInformation.of(TestDataElement.class),
                                new OneInputTestStreamOperatorFactory(UID_OP2, evQueue, cmdQueue))
                        .setUidHash(UID_OP2);

        transform2.sinkTo(new DiscardingSink<>());

        return new TestJobWithDescription(
                env.getStreamGraph().getJobGraph(),
                emptySet(),
                emptySet(),
                emptySet(),
                emptyMap(),
                evQueue,
                cmdQueue);
    }

    private static String asUidHash(int num) {
        return String.format("%032X", num);
    }
}
