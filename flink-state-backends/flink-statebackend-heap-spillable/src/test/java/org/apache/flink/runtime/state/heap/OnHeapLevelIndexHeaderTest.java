/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.flink.runtime.state.heap;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ThreadLocalRandom;

import static org.apache.flink.runtime.state.heap.SkipListUtils.DEFAULT_LEVEL;
import static org.apache.flink.runtime.state.heap.SkipListUtils.MAX_LEVEL;
import static org.apache.flink.runtime.state.heap.SkipListUtils.NIL_NODE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link OnHeapLevelIndexHeader}. */
class OnHeapLevelIndexHeaderTest {
    private static final ThreadLocalRandom random = ThreadLocalRandom.current();

    private OnHeapLevelIndexHeader heapHeadIndex;

    @BeforeEach
    void setUp() {
        heapHeadIndex = new OnHeapLevelIndexHeader();
    }

    @Test
    void testInitStatus() {
        assertThat(heapHeadIndex.getLevel()).isEqualTo(1);
        assertThat(heapHeadIndex.getLevelIndex()).hasSize(DEFAULT_LEVEL);
        for (long node : heapHeadIndex.getLevelIndex()) {
            assertThat(node).isEqualTo(NIL_NODE);
        }
        for (int level = 0; level <= heapHeadIndex.getLevel(); level++) {
            assertThat(heapHeadIndex.getNextNode(level)).isEqualTo(NIL_NODE);
        }
    }

    @Test
    void testNormallyUpdateLevel() {
        int level = heapHeadIndex.getLevel();
        // update level to no more than init max level
        for (; level <= DEFAULT_LEVEL; level++) {
            heapHeadIndex.updateLevel(level);
            assertThat(heapHeadIndex.getLevel()).isEqualTo(level);
            assertThat(heapHeadIndex.getLevelIndex()).hasSize(DEFAULT_LEVEL);
        }
        // update level to trigger scale up
        heapHeadIndex.updateLevel(level);
        assertThat(heapHeadIndex.getLevel()).isEqualTo(level);
        assertThat(heapHeadIndex.getLevelIndex()).hasSize(DEFAULT_LEVEL * 2);
    }

    /** Test update to current level is allowed. */
    @Test
    void testUpdateToCurrentLevel() {
        heapHeadIndex.updateLevel(heapHeadIndex.getLevel());
    }

    /** Test update to current level is allowed. */
    @Test
    void testUpdateLevelToLessThanCurrentLevel() {
        int level = heapHeadIndex.getLevel();
        // update level 10 times
        for (int i = 0; i < 10; i++) {
            heapHeadIndex.updateLevel(++level);
        }
        // check update level to values less than current top level
        for (int i = level - 1; i >= 0; i--) {
            heapHeadIndex.updateLevel(i);
            assertThat(heapHeadIndex.getLevel()).isEqualTo(level);
        }
    }

    /** Test once update more than one level is not allowed. */
    @Test
    void testOnceUpdateMoreThanOneLevel() {
        assertThatThrownBy(() -> heapHeadIndex.updateLevel(heapHeadIndex.getLevel() + 2))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /** Test update to negative level is not allowed. */
    @Test
    void testUpdateToNegativeLevel() {
        assertThatThrownBy(() -> heapHeadIndex.updateLevel(-1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /** Test update to more than max level is not allowed. */
    @Test
    void testUpdateToMoreThanMaximumAllowed() {
        assertThatThrownBy(() -> heapHeadIndex.updateLevel(MAX_LEVEL + 1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testUpdateNextNode() {
        // test update next node of level 0
        int level = 0;
        long node1 = random.nextLong(Long.MAX_VALUE);
        heapHeadIndex.updateNextNode(level, node1);
        assertThat(heapHeadIndex.getNextNode(level)).isEqualTo(node1);
        // Increase one level and make sure everything still works
        heapHeadIndex.updateLevel(++level);
        long node2 = random.nextLong(Long.MAX_VALUE);
        heapHeadIndex.updateNextNode(level, node2);
        assertThat(heapHeadIndex.getNextNode(level)).isEqualTo(node2);
        assertThat(heapHeadIndex.getNextNode(level - 1)).isEqualTo(node1);
    }

    @Test
    void testUpdateNextNodeAfterScale() {
        int level = 0;
        for (; level <= DEFAULT_LEVEL; level++) {
            heapHeadIndex.updateLevel(level);
        }
        heapHeadIndex.updateLevel(level);
        long node = random.nextLong(Long.MAX_VALUE);
        heapHeadIndex.updateNextNode(level, node);
        assertThat(heapHeadIndex.getNextNode(level)).isEqualTo(node);
        for (int i = 0; i < level; i++) {
            assertThat(heapHeadIndex.getNextNode(i)).isEqualTo(NIL_NODE);
        }
    }
}
