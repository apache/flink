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

package org.apache.flink.datastream.impl.context;

import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link DefaultStateManager}. */
class DefaultStateManagerTest {
    @Test
    void testGetCurrentKey() {
        final String key = "key";
        DefaultStateManager stateManager = new DefaultStateManager(() -> key, ignore -> {});
        assertThat((String) stateManager.getCurrentKey()).isEqualTo(key);
    }

    @Test
    void testErrorInGetCurrentKey() {
        DefaultStateManager stateManager =
                new DefaultStateManager(
                        () -> {
                            throw new RuntimeException("Expected Error");
                        },
                        ignore -> {});
        assertThatThrownBy(stateManager::getCurrentKey)
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Expected Error");
    }

    @Test
    void testExecuteInKeyContext() {
        final int oldKey = 1;
        final int newKey = 2;
        // -1 as unset value
        AtomicInteger setKey = new AtomicInteger(-1);
        DefaultStateManager stateManager =
                new DefaultStateManager(() -> oldKey, k -> setKey.set((Integer) k));
        stateManager.executeInKeyContext(() -> assertThat(setKey).hasValue(newKey), newKey);
        assertThat(setKey).hasValue(oldKey);
    }
}
