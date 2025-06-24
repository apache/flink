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

import org.junit.jupiter.api.Test;

import static org.apache.flink.runtime.checkpoint.InflightDataRescalingDescriptorUtil.mappings;
import static org.apache.flink.runtime.checkpoint.InflightDataRescalingDescriptorUtil.to;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests to(@link SubtaskStateMapper). */
class SubtaskStateMapperTest {

    @Test
    void testFirstTaskMappingOnScaleDown() {
        assertThat(SubtaskStateMapper.FIRST.getNewToOldSubtasksMapping(3, 2))
                .isEqualTo(mappings(to(0, 1, 2), to()));
    }

    @Test
    void testFirstTaskMappingOnNoScale() {
        // this may be a bit surprising, but the optimization should be done on call-site
        assertThat(SubtaskStateMapper.FIRST.getNewToOldSubtasksMapping(3, 3))
                .isEqualTo(mappings(to(0, 1, 2), to(), to()));
    }

    @Test
    void testFirstTaskMappingOnScaleUp() {
        assertThat(SubtaskStateMapper.FIRST.getNewToOldSubtasksMapping(3, 4))
                .isEqualTo(mappings(to(0, 1, 2), to(), to(), to()));
    }

    @Test
    void testFullTaskMappingOnScaleDown() {
        assertThat(SubtaskStateMapper.FULL.getNewToOldSubtasksMapping(3, 2))
                .isEqualTo(mappings(to(0, 1, 2), to(0, 1, 2)));
    }

    @Test
    void testFullTaskMappingOnNoScale() {
        // this may be a bit surprising, but the optimization should be done on call-site
        assertThat(SubtaskStateMapper.FULL.getNewToOldSubtasksMapping(3, 3))
                .isEqualTo(mappings(to(0, 1, 2), to(0, 1, 2), to(0, 1, 2)));
    }

    @Test
    void testFullTaskMappingOnScaleUp() {
        assertThat(SubtaskStateMapper.FULL.getNewToOldSubtasksMapping(3, 4))
                .isEqualTo(mappings(to(0, 1, 2), to(0, 1, 2), to(0, 1, 2), to(0, 1, 2)));
    }

    @Test
    void testRangeSelectorTaskMappingOnScaleDown() {
        // 3 partitions: [0; 43) [43; 87) [87; 128)
        // 2 partitions: [0; 64) [64; 128)
        assertThat(SubtaskStateMapper.RANGE.getNewToOldSubtasksMapping(3, 2))
                .isEqualTo(mappings(to(0, 1), to(1, 2)));

        assertThat(SubtaskStateMapper.RANGE.getNewToOldSubtasksMapping(10, 2))
                .isEqualTo(mappings(to(0, 1, 2, 3, 4), to(5, 6, 7, 8, 9)));

        assertThat(SubtaskStateMapper.RANGE.getNewToOldSubtasksMapping(10, 1))
                .isEqualTo(mappings(to(0, 1, 2, 3, 4, 5, 6, 7, 8, 9)));
    }

    @Test
    void testRangeSelectorTaskMappingOnNoScale() {
        assertThat(SubtaskStateMapper.RANGE.getNewToOldSubtasksMapping(3, 3))
                .isEqualTo(mappings(to(0), to(1), to(2)));
    }

    @Test
    void testRangeSelectorTaskMappingOnScaleUp() {
        assertThat(SubtaskStateMapper.RANGE.getNewToOldSubtasksMapping(3, 4))
                .isEqualTo(mappings(to(0), to(0, 1), to(1, 2), to(2)));

        assertThat(SubtaskStateMapper.RANGE.getNewToOldSubtasksMapping(3, 7))
                .isEqualTo(mappings(to(0), to(0), to(0, 1), to(1), to(1, 2), to(2), to(2)));
    }

    @Test
    void testRoundRobinTaskMappingOnScaleDown() {
        assertThat(SubtaskStateMapper.ROUND_ROBIN.getNewToOldSubtasksMapping(10, 4))
                .isEqualTo(mappings(to(0, 4, 8), to(1, 5, 9), to(2, 6), to(3, 7)));

        assertThat(SubtaskStateMapper.ROUND_ROBIN.getNewToOldSubtasksMapping(5, 4))
                .isEqualTo(mappings(to(0, 4), to(1), to(2), to(3)));

        assertThat(SubtaskStateMapper.ROUND_ROBIN.getNewToOldSubtasksMapping(5, 2))
                .isEqualTo(mappings(to(0, 2, 4), to(1, 3)));

        assertThat(SubtaskStateMapper.ROUND_ROBIN.getNewToOldSubtasksMapping(5, 1))
                .isEqualTo(mappings(to(0, 1, 2, 3, 4)));
    }

    @Test
    void testRoundRobinTaskMappingOnNoScale() {
        assertThat(SubtaskStateMapper.ROUND_ROBIN.getNewToOldSubtasksMapping(10, 10))
                .isEqualTo(
                        mappings(
                                to(0), to(1), to(2), to(3), to(4), to(5), to(6), to(7), to(8),
                                to(9)));

        assertThat(SubtaskStateMapper.ROUND_ROBIN.getNewToOldSubtasksMapping(5, 5))
                .isEqualTo(mappings(to(0), to(1), to(2), to(3), to(4)));

        assertThat(SubtaskStateMapper.ROUND_ROBIN.getNewToOldSubtasksMapping(1, 1))
                .isEqualTo(mappings(to(0)));
    }

    @Test
    void testRoundRobinTaskMappingOnScaleUp() {
        assertThat(SubtaskStateMapper.ROUND_ROBIN.getNewToOldSubtasksMapping(4, 10))
                .isEqualTo(
                        mappings(to(0), to(1), to(2), to(3), to(), to(), to(), to(), to(), to()));

        assertThat(SubtaskStateMapper.ROUND_ROBIN.getNewToOldSubtasksMapping(4, 5))
                .isEqualTo(mappings(to(0), to(1), to(2), to(3), to()));

        assertThat(SubtaskStateMapper.ROUND_ROBIN.getNewToOldSubtasksMapping(2, 5))
                .isEqualTo(mappings(to(0), to(1), to(), to(), to()));

        assertThat(SubtaskStateMapper.ROUND_ROBIN.getNewToOldSubtasksMapping(1, 5))
                .isEqualTo(mappings(to(0), to(), to(), to(), to()));
    }
}
