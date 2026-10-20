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

package org.apache.flink.api.common.state;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CheckpointListenerRegionalTest {

    @Test
    void testGlobalCheckpointInfoIsGlobal() {
        RegionalCheckpointInfo info = RegionalCheckpointInfo.globalCheckpoint();
        assertThat(info.isGlobalCheckpoint()).isTrue();
        assertThat(info.getFallbackCheckpointSubtasks()).isEmpty();
        assertThat(info.getFallbackCheckpointIds()).isEmpty();
    }

    @Test
    void testRegionalCheckpointInfoNotGlobal() {
        Map<Long, Set<String>> fallback = new HashMap<>();
        fallback.put(99L, Set.of("Source: kafka_source#0", "Map#0", "Sink#0"));
        RegionalCheckpointInfo info = new RegionalCheckpointInfo(fallback);

        assertThat(info.isGlobalCheckpoint()).isFalse();
        assertThat(info.getFallbackCheckpointIds()).containsExactly(99L);
        assertThat(info.getFallbackCheckpointSubtasks().get(99L))
                .containsExactlyInAnyOrder("Source: kafka_source#0", "Map#0", "Sink#0");
    }

    @Test
    void testDefaultMethodDelegatesToOriginal() throws Exception {
        AtomicLong receivedId = new AtomicLong(-1);

        CheckpointListener listener =
                new CheckpointListener() {
                    @Override
                    public void notifyCheckpointComplete(long checkpointId) {
                        receivedId.set(checkpointId);
                    }
                };

        Map<Long, Set<String>> fallback = new HashMap<>();
        fallback.put(99L, Set.of("Source#0"));
        RegionalCheckpointInfo info = new RegionalCheckpointInfo(fallback);

        listener.notifyRegionalCheckpointComplete(100L, info);
        assertThat(receivedId.get()).isEqualTo(100L);
    }

    @Test
    void testOverriddenMethodReceivesRegionalInfo() throws Exception {
        AtomicBoolean wasRegional = new AtomicBoolean(false);

        CheckpointListener listener =
                new CheckpointListener() {
                    @Override
                    public void notifyCheckpointComplete(long checkpointId) {}

                    @Override
                    public void notifyRegionalCheckpointComplete(
                            long checkpointId, RegionalCheckpointInfo info) {
                        wasRegional.set(!info.isGlobalCheckpoint());
                    }
                };

        Map<Long, Set<String>> fallback = new HashMap<>();
        fallback.put(99L, Set.of("Source#0"));
        listener.notifyRegionalCheckpointComplete(100L, new RegionalCheckpointInfo(fallback));
        assertThat(wasRegional.get()).isTrue();

        listener.notifyRegionalCheckpointComplete(101L, RegionalCheckpointInfo.globalCheckpoint());
        assertThat(wasRegional.get()).isFalse();
    }

    @Test
    void testNotifyRegionalCheckpointFallbackDefaultNoOp() {
        // Default implementation should be no-op and not throw
        CheckpointListener listener =
                new CheckpointListener() {
                    @Override
                    public void notifyCheckpointComplete(long checkpointId) {}
                };
        listener.notifyRegionalCheckpointFallback(100L, 99L);
    }

    @Test
    void testNotifyRegionalCheckpointFallbackOverridden() {
        AtomicLong receivedCheckpointId = new AtomicLong(-1);
        AtomicLong receivedFallbackId = new AtomicLong(-1);

        CheckpointListener listener =
                new CheckpointListener() {
                    @Override
                    public void notifyCheckpointComplete(long checkpointId) {}

                    @Override
                    public void notifyRegionalCheckpointFallback(
                            long checkpointId, long fallbackCheckpointId) {
                        receivedCheckpointId.set(checkpointId);
                        receivedFallbackId.set(fallbackCheckpointId);
                    }
                };

        listener.notifyRegionalCheckpointFallback(100L, 99L);
        assertThat(receivedCheckpointId.get()).isEqualTo(100L);
        assertThat(receivedFallbackId.get()).isEqualTo(99L);
    }

    @Test
    void testFallbackMapIsUnmodifiable() {
        Map<Long, Set<String>> fallback = new HashMap<>();
        fallback.put(99L, Set.of("Source#0"));
        RegionalCheckpointInfo info = new RegionalCheckpointInfo(fallback);

        assertThatThrownBy(() -> info.getFallbackCheckpointSubtasks().put(98L, Set.of("Map#1")))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void testMultipleFallbackCheckpoints() {
        Map<Long, Set<String>> fallback = new HashMap<>();
        fallback.put(99L, Set.of("Source#0", "Map#0"));
        fallback.put(98L, Set.of("Source#1"));
        RegionalCheckpointInfo info = new RegionalCheckpointInfo(fallback);

        assertThat(info.isGlobalCheckpoint()).isFalse();
        assertThat(info.getFallbackCheckpointIds()).containsExactlyInAnyOrder(99L, 98L);
    }
}
