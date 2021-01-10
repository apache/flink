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

package org.apache.flink.runtime.io.network.api.writer;

import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

/** Tests {@link SubtaskStateMapper}. */
public class SubtaskStateMapperTest {
    @Test
    public void testDiscardTaskMappingOnScaleDown() {
        assertMappingEquals(
                new int[][] {{0}, {1}},
                SubtaskStateMapper.DISCARD_EXTRA_STATE.getNewToOldSubtasksMapping(3, 2));
    }

    @Test
    public void testDiscardTaskMappingOnNoScale() {
        // this may be a bit surprising, but the optimization should be done on call-site
        assertMappingEquals(
                new int[][] {{0}, {1}, {2}},
                SubtaskStateMapper.DISCARD_EXTRA_STATE.getNewToOldSubtasksMapping(3, 3));
    }

    @Test
    public void testDiscardTaskMappingOnScaleUp() {
        assertMappingEquals(
                new int[][] {{0}, {1}, {2}, {}},
                SubtaskStateMapper.DISCARD_EXTRA_STATE.getNewToOldSubtasksMapping(3, 4));
    }

    @Test
    public void testFirstTaskMappingOnScaleDown() {
        assertMappingEquals(
                new int[][] {{0, 1, 2}, {}},
                SubtaskStateMapper.FIRST.getNewToOldSubtasksMapping(3, 2));
    }

    @Test
    public void testFirstTaskMappingOnNoScale() {
        // this may be a bit surprising, but the optimization should be done on call-site
        assertMappingEquals(
                new int[][] {{0, 1, 2}, {}, {}},
                SubtaskStateMapper.FIRST.getNewToOldSubtasksMapping(3, 3));
    }

    @Test
    public void testFirstTaskMappingOnScaleUp() {
        assertMappingEquals(
                new int[][] {{0, 1, 2}, {}, {}, {}},
                SubtaskStateMapper.FIRST.getNewToOldSubtasksMapping(3, 4));
    }

    @Test
    public void testFullTaskMappingOnScaleDown() {
        assertMappingEquals(
                new int[][] {{0, 1, 2}, {0, 1, 2}},
                SubtaskStateMapper.FULL.getNewToOldSubtasksMapping(3, 2));
    }

    @Test
    public void testFullTaskMappingOnNoScale() {
        // this may be a bit surprising, but the optimization should be done on call-site
        assertMappingEquals(
                new int[][] {{0, 1, 2}, {0, 1, 2}, {0, 1, 2}},
                SubtaskStateMapper.FULL.getNewToOldSubtasksMapping(3, 3));
    }

    @Test
    public void testFullTaskMappingOnScaleUp() {
        assertMappingEquals(
                new int[][] {{0, 1, 2}, {0, 1, 2}, {0, 1, 2}, {0, 1, 2}},
                SubtaskStateMapper.FULL.getNewToOldSubtasksMapping(3, 4));
    }

    @Test
    public void testRangeSelectorTaskMappingOnScaleDown() {
        // 3 partitions: [0; 43) [43; 87) [87; 128)
        // 2 partitions: [0; 64) [64; 128)
        assertMappingEquals(
                new int[][] {{0, 1}, {1, 2}},
                SubtaskStateMapper.RANGE.getNewToOldSubtasksMapping(3, 2));

        assertMappingEquals(
                new int[][] {{0, 1, 2, 3, 4}, {5, 6, 7, 8, 9}},
                SubtaskStateMapper.RANGE.getNewToOldSubtasksMapping(10, 2));

        assertMappingEquals(
                new int[][] {{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}},
                SubtaskStateMapper.RANGE.getNewToOldSubtasksMapping(10, 1));
    }

    @Test
    public void testRangeSelectorTaskMappingOnNoScale() {
        assertMappingEquals(
                new int[][] {{0}, {1}, {2}},
                SubtaskStateMapper.RANGE.getNewToOldSubtasksMapping(3, 3));
    }

    @Test
    public void testRangeSelectorTaskMappingOnScaleUp() {
        assertMappingEquals(
                new int[][] {{0}, {0, 1}, {1, 2}, {2}},
                SubtaskStateMapper.RANGE.getNewToOldSubtasksMapping(3, 4));

        assertMappingEquals(
                new int[][] {{0}, {0}, {0, 1}, {1}, {1, 2}, {2}, {2}},
                SubtaskStateMapper.RANGE.getNewToOldSubtasksMapping(3, 7));
    }

    @Test
    public void testRoundRobinTaskMappingOnScaleDown() {
        assertMappingEquals(
                new int[][] {{0, 4, 8}, {1, 5, 9}, {2, 6}, {3, 7}},
                SubtaskStateMapper.ROUND_ROBIN.getNewToOldSubtasksMapping(10, 4));

        assertMappingEquals(
                new int[][] {{0, 4}, {1}, {2}, {3}},
                SubtaskStateMapper.ROUND_ROBIN.getNewToOldSubtasksMapping(5, 4));

        assertMappingEquals(
                new int[][] {{0, 2, 4}, {1, 3}},
                SubtaskStateMapper.ROUND_ROBIN.getNewToOldSubtasksMapping(5, 2));

        assertMappingEquals(
                new int[][] {{0, 1, 2, 3, 4}},
                SubtaskStateMapper.ROUND_ROBIN.getNewToOldSubtasksMapping(5, 1));
    }

    @Test
    public void testRoundRobinTaskMappingOnNoScale() {
        assertMappingEquals(
                new int[][] {{0}, {1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}, {9}},
                SubtaskStateMapper.ROUND_ROBIN.getNewToOldSubtasksMapping(10, 10));

        assertMappingEquals(
                new int[][] {{0}, {1}, {2}, {3}, {4}},
                SubtaskStateMapper.ROUND_ROBIN.getNewToOldSubtasksMapping(5, 5));

        assertMappingEquals(
                new int[][] {{0}}, SubtaskStateMapper.ROUND_ROBIN.getNewToOldSubtasksMapping(1, 1));
    }

    @Test
    public void testRoundRobinTaskMappingOnScaleUp() {
        assertMappingEquals(
                new int[][] {{0}, {1}, {2}, {3}, {}, {}, {}, {}, {}, {}},
                SubtaskStateMapper.ROUND_ROBIN.getNewToOldSubtasksMapping(4, 10));

        assertMappingEquals(
                new int[][] {{0}, {1}, {2}, {3}, {}},
                SubtaskStateMapper.ROUND_ROBIN.getNewToOldSubtasksMapping(4, 5));

        assertMappingEquals(
                new int[][] {{0}, {1}, {}, {}, {}},
                SubtaskStateMapper.ROUND_ROBIN.getNewToOldSubtasksMapping(2, 5));

        assertMappingEquals(
                new int[][] {{0}, {}, {}, {}, {}},
                SubtaskStateMapper.ROUND_ROBIN.getNewToOldSubtasksMapping(1, 5));
    }

    static void assertMappingEquals(int[][] expected, Map<Integer, Set<Integer>> actual) {
        Map<Integer, Set<Integer>> expectedMapping = new HashMap<>();
        for (int newTaskIndex = 0; newTaskIndex < expected.length; newTaskIndex++) {
            expectedMapping.put(
                    newTaskIndex,
                    Arrays.stream(expected[newTaskIndex]).boxed().collect(Collectors.toSet()));
        }
        assertEquals(expectedMapping, actual);
    }
}
