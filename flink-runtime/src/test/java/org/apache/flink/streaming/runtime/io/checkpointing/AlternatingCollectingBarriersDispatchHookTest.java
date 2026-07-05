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
 * Source-level invariants for the {@code alignedCheckpointTimeout} → UC switch in {@link
 * AlternatingCollectingBarriers}. Note this is NOT a {@code barrierReceived} hook — the parent
 * {@code AbstractAlternatingAlignedBarrierHandlerState.barrierReceived} is {@code final}; the only
 * place to switch from aligned to UC is {@code alignedCheckpointTimeout}.
 */
class AlternatingCollectingBarriersDispatchHookTest {

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
                        "initInputsCheckpoint precedes dispatcher (cpId result registered before "
                                + "Step 3)")
                .isLessThan(dispatchIdx);
        assertThat(dispatchIdx)
                .as("dispatcher precedes triggerGlobalCheckpoint (master ordering)")
                .isLessThan(triggerIdx);
    }

    @Test
    void testHookIsInAlignedCheckpointTimeoutNotBarrierReceived() throws Exception {
        String source = readSource();

        // Hard guard: AbstractAlternatingAlignedBarrierHandlerState.barrierReceived is final, so
        // AlternatingCollectingBarriers cannot override it. The UC switch lives in
        // alignedCheckpointTimeout only.
        assertThat(source)
                .as("alignedCheckpointTimeout is the UC switch entry on this state class")
                .contains("public BarrierHandlerState alignedCheckpointTimeout(");
        assertThat(source)
                .as("No barrierReceived override should exist on AlternatingCollectingBarriers")
                .doesNotContain("public BarrierHandlerState barrierReceived(");

        int timeoutIdx = source.indexOf("alignedCheckpointTimeout(");
        int dispatchIdx = source.indexOf("onCheckpointStartedForAllInputs(", timeoutIdx);
        assertThat(dispatchIdx)
                .as("dispatcher call is inside alignedCheckpointTimeout body")
                .isNotNegative();
    }

    @Test
    void testNoLegacyPerInputCheckpointStartedLoopInTimeout() throws Exception {
        String source = readSource();

        int methodStart = source.indexOf("public BarrierHandlerState alignedCheckpointTimeout(");
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
                        "src/main/java/org/apache/flink/streaming/runtime/io/checkpointing/AlternatingCollectingBarriers.java");
        if (!Files.exists(candidate)) {
            candidate =
                    Paths.get(
                            "flink-runtime/src/main/java/org/apache/flink/streaming/runtime/io/checkpointing/AlternatingCollectingBarriers.java");
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
