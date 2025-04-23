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

package org.apache.flink.test.checkpointing;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.RpcServiceSharing;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.test.junit5.InjectMiniCluster;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.testutils.junit.SharedObjectsExtension;
import org.apache.flink.testutils.junit.SharedReference;
import org.apache.flink.util.Collector;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.SerializedThrowable;
import org.apache.flink.util.TestLogger;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.Random;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.configuration.CheckpointingOptions.CHECKPOINTING_TIMEOUT;
import static org.apache.flink.configuration.CheckpointingOptions.CHECKPOINT_STORAGE;
import static org.apache.flink.configuration.CheckpointingOptions.FS_SMALL_FILE_THRESHOLD;
import static org.apache.flink.configuration.RpcOptions.ASK_TIMEOUT_DURATION;
import static org.apache.flink.configuration.RpcOptions.FRAMESIZE;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** CheckpointAcknowledgeFailureITCase. */
public class CheckpointAcknowledgeFailureITCase extends TestLogger {

    // the minimum allowed by pekko
    private static final int MIN_PEKKO_FRAME_SIZE = 32000;

    // set frame size high enough to allow checkpoint decline RPC after failing to ACK
    private static final int PEKKO_FRAME_SIZE = MIN_PEKKO_FRAME_SIZE * 2;
    // set state size higher than frame size so that checkpoint can not be acked
    private static final int STATE_SIZE = PEKKO_FRAME_SIZE * 2;
    // let all the state go via checkpoint ACK RPC to exceed frame size limit
    private static final MemorySize IN_MEM_STATE_THRESHOLD = new MemorySize(STATE_SIZE * 2);

    // let pekko time out checkpoint ACK
    private static final Duration ASK_TIMEOUT = Duration.ofMillis(250);
    // do NOT let flink time out the checkpoint
    private static final Duration CHECKPOINT_TIMEOUT = ASK_TIMEOUT.multipliedBy(1000);

    @RegisterExtension
    static final MiniClusterExtension MINI_CLUSTER =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setConfiguration(getConfiguration())
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(1)
                            .setRpcServiceSharing(RpcServiceSharing.DEDICATED)
                            .build());

    @RegisterExtension
    private final SharedObjectsExtension sharedObjects = SharedObjectsExtension.create();

    private static Configuration getConfiguration() {
        Configuration cfg = new Configuration();
        cfg.set(FRAMESIZE, new MemorySize(PEKKO_FRAME_SIZE).toString());
        cfg.set(ASK_TIMEOUT_DURATION, ASK_TIMEOUT);
        cfg.set(FS_SMALL_FILE_THRESHOLD, IN_MEM_STATE_THRESHOLD);
        cfg.set(CHECKPOINT_STORAGE, "jobmanager");

        return cfg;
    }

    /**
     * Test that if a task is unable to acknowledge a checkpoint then the checkpoint fails
     * (immediately) with org.apache.pekko.pattern.AskTimeoutException caused. This requires the use
     * of pekko ask, and not pekko tell under the hood for the acknowledgment. Typical failure
     * reason is exceeding pekko framesize.
     */
    @Test
    void testCheckpointAckFailure(@InjectMiniCluster MiniCluster cluster) throws Exception {
        SharedReference<CompletableFuture<Object>> stateUpdatedFuture =
                sharedObjects.add(new CompletableFuture<>());
        Configuration cfg = new Configuration();
        cfg.set(CHECKPOINTING_TIMEOUT, CHECKPOINT_TIMEOUT);
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(cfg);
        JobID jobID = executeJobAsync(env, stateUpdatedFuture);
        stateUpdatedFuture.get().join();

        assertThatThrownBy(() -> cluster.triggerCheckpoint(jobID).get())
                .hasCauseInstanceOf(CheckpointException.class)
                .matches(CheckpointAcknowledgeFailureITCase::hasAskTimeoutException);
    }

    private static JobID executeJobAsync(
            StreamExecutionEnvironment env, SharedReference<CompletableFuture<Object>> stateUpdated)
            throws Exception {
        env.fromSequence(Long.MIN_VALUE, Long.MAX_VALUE)
                .keyBy(ign -> 0L)
                .process(
                        new KeyedProcessFunction<Long, Long, Long>() {

                            @Override
                            public void processElement(
                                    Long value,
                                    KeyedProcessFunction<Long, Long, Long>.Context ctx,
                                    Collector<Long> out)
                                    throws Exception {
                                if (value == Long.MIN_VALUE) {
                                    getRuntimeContext()
                                            .getState(
                                                    new ValueStateDescriptor<>(
                                                            "test", byte[].class))
                                            .update(buildState());
                                    stateUpdated.get().complete(null);
                                }
                            }
                        })
                .sinkTo(new DiscardingSink<>());
        return env.executeAsync().getJobID();
    }

    private static byte[] buildState() {
        // Random (incompressible) bytes — keeps the ACK above the pekko frame size even
        // when snapshot compression is enabled (e.g. via test-config randomization).
        byte[] state = new byte[STATE_SIZE];
        new Random(0).nextBytes(state);
        return state;
    }

    /**
     * We can't use the exception class to check for the AskTimeoutException because it's been
     * loaded by a ClassLoader we don't have access to.
     *
     * @param t the exception to check
     * @return true if the exception is an AskTimeoutException
     */
    private static boolean hasAskTimeoutException(Throwable t) {
        final String clazz = "org.apache.pekko.pattern.AskTimeoutException";
        return ExceptionUtils.findThrowable(
                        t,
                        cause -> {
                            if (cause instanceof SerializedThrowable) {
                                final SerializedThrowable serializedThrowable =
                                        (SerializedThrowable) cause;
                                return clazz.equals(
                                        serializedThrowable.getOriginalErrorClassName());
                            }
                            return clazz.equals(cause.getClass().getName());
                        })
                .isPresent();
    }
}
