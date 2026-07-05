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

package org.apache.flink.streaming.runtime.tasks;

import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Source-level invariants for the {@code RecoveryCheckpointTrigger} lifecycle in {@link
 * StreamTask}: the trigger starts NOT_READY, the non-CDR recovery path and the empty-input-gates
 * short-circuit go straight to NO_OP, and the CDR chain installs the barrier-inserting trigger
 * between conversion and state-consumed completion, ending in NO_OP — with all in-chain mutations
 * routed through the mailbox via {@code setRecoveryCheckpointTrigger}.
 *
 * <p>FLINK-38544 transitional: the in-memory trigger install is replaced by the disk drainer when
 * the spilling backend lands; this test is deleted/adapted together with it.
 */
class StreamTaskRecoveryTriggerLifecycleTest {

    @Test
    void testTriggerFieldStartsNotReady() throws Exception {
        String source = normalize(readSource());
        assertThat(source)
                .as("The trigger must start NOT_READY so early barriers are declined, not lost")
                .contains(
                        "private RecoveryCheckpointTrigger recoveryCheckpointTrigger = "
                                + "RecoveryCheckpointTrigger.NOT_READY;");
    }

    @Test
    void testNonCdrRecoveryGoesStraightToNoOp() throws Exception {
        String source = normalize(readSource());

        int methodStart =
                source.indexOf(
                        "private CompletableFuture<Void> recoverChannelsWithoutCheckpointing(");
        assertThat(methodStart).isNotNegative();
        int noOpIdx =
                source.indexOf(
                        "recoveryCheckpointTrigger = RecoveryCheckpointTrigger.NO_OP;",
                        methodStart);
        int readIdx = source.indexOf("channelIOExecutor.execute(", methodStart);

        assertThat(noOpIdx).as("non-CDR recovery sets the NO_OP trigger").isNotNegative();
        assertThat(readIdx).isNotNegative();
        assertThat(noOpIdx)
                .as("NO_OP must be installed before recovery work starts")
                .isLessThan(readIdx);
    }

    @Test
    void testEmptyGatesShortCircuitGoesStraightToNoOp() throws Exception {
        String source = normalize(readSource());

        int methodStart =
                source.indexOf("private CompletableFuture<Void> recoverChannelsWithCheckpointing(");
        assertThat(methodStart).isNotNegative();
        int noOpIdx =
                source.indexOf(
                        "recoveryCheckpointTrigger = RecoveryCheckpointTrigger.NO_OP;",
                        methodStart);
        int shortCircuitIdx =
                source.indexOf("return FutureUtils.completedVoidFuture();", methodStart);

        assertThat(noOpIdx)
                .as("the empty-input-gates short-circuit installs the NO_OP trigger")
                .isNotNegative();
        assertThat(shortCircuitIdx).isNotNegative();
        assertThat(noOpIdx).isLessThan(shortCircuitIdx);
    }

    @Test
    void testCdrChainTriggerLifecycleOrdering() throws Exception {
        String source = normalize(readSource());

        int methodStart =
                source.indexOf("private CompletableFuture<Void> recoverChannelsWithCheckpointing(");
        assertThat(methodStart).isNotNegative();

        // Comments inside the method mention some of these tokens, so anchor on the chain start
        // and require each stage to appear AFTER the previous one (sequential find = ordering).
        int idx =
                source.indexOf(
                        "return setRecoveryCheckpointTrigger(RecoveryCheckpointTrigger.NOT_READY)",
                        methodStart);
        assertThat(idx)
                .as("chain starts NOT_READY (checkpoints declined while filtering)")
                .isNotNegative();

        idx = source.indexOf("readInputChannelState(reader, inputGates)", idx);
        assertThat(idx).as("filtering/reading runs after NOT_READY is installed").isNotNegative();

        idx = source.indexOf("requestPartitions(inputGates, true)", idx);
        assertThat(idx)
                .as("conversion (strict filter-then-consume) follows reading")
                .isNotNegative();

        idx = source.indexOf("new InMemoryRecoveryCheckpointTrigger(", idx);
        assertThat(idx)
                .as("the barrier-inserting trigger is installed after conversion")
                .isNotNegative();

        idx = source.indexOf("completeAll(", idx);
        assertThat(idx)
                .as("the trigger stays live while recovered state is being consumed")
                .isNotNegative();

        idx = source.indexOf("setRecoveryCheckpointTrigger(RecoveryCheckpointTrigger.NO_OP)", idx);
        assertThat(idx).as("NO_OP only after all gates report state consumed").isNotNegative();
    }

    @Test
    void testChainMutationsGoThroughMailbox() throws Exception {
        String source = normalize(readSource());

        int methodStart =
                source.indexOf("private CompletableFuture<Void> recoverChannelsWithCheckpointing(");
        assertThat(methodStart).isNotNegative();
        int chainStart = source.indexOf("return setRecoveryCheckpointTrigger(", methodStart);
        assertThat(chainStart)
                .as("the CDR chain opens with a mailbox trigger update")
                .isNotNegative();
        int methodEnd =
                source.indexOf("private CompletableFuture<Void> setRecoveryCheckpointTrigger(");
        assertThat(methodEnd).isNotNegative();

        String chain = source.substring(chainStart, methodEnd);
        assertThat(chain)
                .as(
                        "Inside the asynchronous chain the trigger field may only be mutated via "
                                + "setRecoveryCheckpointTrigger (a mailbox mail)")
                .doesNotContain("recoveryCheckpointTrigger =");

        int helperBodyEnd = source.indexOf('}', methodEnd);
        String helper = source.substring(methodEnd, helperBodyEnd);
        assertThat(helper)
                .as("setRecoveryCheckpointTrigger routes through the main mailbox executor")
                .contains("mainMailboxExecutor.execute(");
    }

    private static String readSource() throws Exception {
        Path candidate =
                Paths.get("src/main/java/org/apache/flink/streaming/runtime/tasks/StreamTask.java");
        if (!Files.exists(candidate)) {
            candidate =
                    Paths.get(
                            "flink-runtime/src/main/java/org/apache/flink/streaming/runtime/tasks/StreamTask.java");
        }
        assertThat(Files.exists(candidate))
                .as("Located StreamTask.java for the source-level invariant check")
                .isTrue();
        return new String(Files.readAllBytes(candidate));
    }

    /** Collapses all whitespace runs to single spaces so assertions survive re-formatting. */
    private static String normalize(String source) {
        return source.replaceAll("\\s+", " ");
    }
}
