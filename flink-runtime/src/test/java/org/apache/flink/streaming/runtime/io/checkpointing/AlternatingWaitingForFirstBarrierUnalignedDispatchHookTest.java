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

package org.apache.flink.streaming.runtime.io.checkpointing;

import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Source-level invariants for the recovery-checkpoint dispatcher hook in {@link
 * AlternatingWaitingForFirstBarrierUnaligned}:
 *
 * <ol>
 *   <li>The dispatcher call exists.
 *   <li>It runs after {@code controller.initInputsCheckpoint} (so the cpId's {@code
 *       ChannelStateWriteResult} is registered before the writer is invoked) and before {@code
 *       controller.triggerGlobalCheckpoint}.
 *   <li>No vestigial per-input {@code checkpointStarted} loop remains alongside the dispatcher.
 * </ol>
 */
class AlternatingWaitingForFirstBarrierUnalignedDispatchHookTest {

    @Test
    void testDispatcherIsCalledBeforeTriggerGlobalCheckpoint() throws Exception {
        String source = readSource();

        int initIdx = source.indexOf("controller.initInputsCheckpoint(unalignedBarrier)");
        int dispatchIdx = source.indexOf("onCheckpointStartedForAllInputs(unalignedBarrier)");
        int triggerIdx = source.indexOf("controller.triggerGlobalCheckpoint(unalignedBarrier)");

        assertThat(initIdx).as("initInputsCheckpoint call exists").isNotNegative();
        assertThat(dispatchIdx).as("dispatcher call exists").isNotNegative();
        assertThat(triggerIdx).as("triggerGlobalCheckpoint call exists").isNotNegative();

        assertThat(initIdx)
                .as(
                        "initInputsCheckpoint must precede the dispatcher so the cpId result is "
                                + "registered when Step 3 fires")
                .isLessThan(dispatchIdx);
        assertThat(dispatchIdx)
                .as("dispatcher must precede triggerGlobalCheckpoint (master ordering)")
                .isLessThan(triggerIdx);
    }

    @Test
    void testNoLegacyPerInputCheckpointStartedLoopInBarrierReceived() throws Exception {
        String source = readSource();

        // barrierReceived still iterates input.checkpointStopped in the allBarriersReceived
        // branch — that loop is fine. We only forbid a parallel input.checkpointStarted loop,
        // which the dispatcher now owns.
        int methodStart = source.indexOf("public BarrierHandlerState barrierReceived(");
        assertThat(methodStart).isNotNegative();

        int methodEnd = findMethodEnd(source, methodStart);
        String body = source.substring(methodStart, methodEnd);

        assertThat(body)
                .as(
                        "Pre-trigger per-input checkpointStarted loop should be removed; dispatcher "
                                + "now owns the fan-out")
                .doesNotContain("input.checkpointStarted(unalignedBarrier)");
    }

    private static String readSource() throws Exception {
        Path candidate =
                Paths.get(
                        "src/main/java/org/apache/flink/streaming/runtime/io/checkpointing/AlternatingWaitingForFirstBarrierUnaligned.java");
        if (!Files.exists(candidate)) {
            candidate =
                    Paths.get(
                            "flink-runtime/src/main/java/org/apache/flink/streaming/runtime/io/checkpointing/AlternatingWaitingForFirstBarrierUnaligned.java");
        }
        return new String(Files.readAllBytes(candidate));
    }

    private static int findMethodEnd(String source, int start) {
        int firstBrace = source.indexOf('{', start);
        int depth = 1;
        for (int i = firstBrace + 1; i < source.length(); i++) {
            char c = source.charAt(i);
            if (c == '{') {
                depth++;
            } else if (c == '}') {
                depth--;
                if (depth == 0) {
                    return i + 1;
                }
            }
        }
        return source.length();
    }
}
