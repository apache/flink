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

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ErrorCollector;

import static org.apache.flink.runtime.checkpoint.InflightDataRescalingDescriptorUtil.mappings;
import static org.apache.flink.runtime.checkpoint.InflightDataRescalingDescriptorUtil.to;
import static org.junit.Assert.assertEquals;

/** Tests to(@link SubtaskStateMapper). */
public class SubtaskStateMapperTest {
    @Rule public ErrorCollector collector = new ErrorCollector();

    @Test
    public void testFirstTaskMappingOnScaleDown() {
        assertEquals(
                mappings(to(0, 1, 2), to()),
                SubtaskStateMapper.FIRST.getNewToOldSubtasksMapping(3, 2));
    }

    @Test
    public void testFirstTaskMappingOnNoScale() {
        // this may be a bit surprising, but the optimization should be done on call-site
        assertEquals(
                mappings(to(0, 1, 2), to(), to()),
                SubtaskStateMapper.FIRST.getNewToOldSubtasksMapping(3, 3));
    }

    @Test
    public void testFirstTaskMappingOnScaleUp() {
        assertEquals(
                mappings(to(0, 1, 2), to(), to(), to()),
                SubtaskStateMapper.FIRST.getNewToOldSubtasksMapping(3, 4));
    }

    @Test
    public void testFullTaskMappingOnScaleDown() {
        assertEquals(
                mappings(to(0, 1, 2), to(0, 1, 2)),
                SubtaskStateMapper.FULL.getNewToOldSubtasksMapping(3, 2));
    }

    @Test
    public void testFullTaskMappingOnNoScale() {
        // this may be a bit surprising, but the optimization should be done on call-site
        assertEquals(
                mappings(to(0, 1, 2), to(0, 1, 2), to(0, 1, 2)),
                SubtaskStateMapper.FULL.getNewToOldSubtasksMapping(3, 3));
    }

    @Test
    public void testFullTaskMappingOnScaleUp() {
        assertEquals(
                mappings(to(0, 1, 2), to(0, 1, 2), to(0, 1, 2), to(0, 1, 2)),
                SubtaskStateMapper.FULL.getNewToOldSubtasksMapping(3, 4));
    }

    @Test
    public void testRangeSelectorTaskMappingOnScaleDown() {
        // 3 partitions: [0; 43) [43; 87) [87; 128)
        // 2 partitions: [0; 64) [64; 128)
        assertEquals(
                mappings(to(0, 1), to(1, 2)),
                SubtaskStateMapper.RANGE.getNewToOldSubtasksMapping(3, 2));

        assertEquals(
                mappings(to(0, 1, 2, 3, 4), to(5, 6, 7, 8, 9)),
                SubtaskStateMapper.RANGE.getNewToOldSubtasksMapping(10, 2));

        assertEquals(
                mappings(to(0, 1, 2, 3, 4, 5, 6, 7, 8, 9)),
                SubtaskStateMapper.RANGE.getNewToOldSubtasksMapping(10, 1));
    }

    @Test
    public void testRangeSelectorTaskMappingOnNoScale() {
        assertEquals(
                mappings(to(0), to(1), to(2)),
                SubtaskStateMapper.RANGE.getNewToOldSubtasksMapping(3, 3));
    }

    @Test
    public void testRangeSelectorTaskMappingOnScaleUp() {
        assertEquals(
                mappings(to(0), to(0, 1), to(1, 2), to(2)),
                SubtaskStateMapper.RANGE.getNewToOldSubtasksMapping(3, 4));

        assertEquals(
                mappings(to(0), to(0), to(0, 1), to(1), to(1, 2), to(2), to(2)),
                SubtaskStateMapper.RANGE.getNewToOldSubtasksMapping(3, 7));
    }

    @Test
    public void testRoundRobinTaskMappingOnScaleDown() {
        assertEquals(
                mappings(to(0, 4, 8), to(1, 5, 9), to(2, 6), to(3, 7)),
                SubtaskStateMapper.ROUND_ROBIN.getNewToOldSubtasksMapping(10, 4));

        assertEquals(
                mappings(to(0, 4), to(1), to(2), to(3)),
                SubtaskStateMapper.ROUND_ROBIN.getNewToOldSubtasksMapping(5, 4));

        assertEquals(
                mappings(to(0, 2, 4), to(1, 3)),
                SubtaskStateMapper.ROUND_ROBIN.getNewToOldSubtasksMapping(5, 2));

        assertEquals(
                mappings(to(0, 1, 2, 3, 4)),
                SubtaskStateMapper.ROUND_ROBIN.getNewToOldSubtasksMapping(5, 1));
    }

    @Test
    public void testRoundRobinTaskMappingOnNoScale() {
        assertEquals(
                mappings(to(0), to(1), to(2), to(3), to(4), to(5), to(6), to(7), to(8), to(9)),
                SubtaskStateMapper.ROUND_ROBIN.getNewToOldSubtasksMapping(10, 10));

        assertEquals(
                mappings(to(0), to(1), to(2), to(3), to(4)),
                SubtaskStateMapper.ROUND_ROBIN.getNewToOldSubtasksMapping(5, 5));

        assertEquals(
                mappings(to(0)), SubtaskStateMapper.ROUND_ROBIN.getNewToOldSubtasksMapping(1, 1));
    }

    @Test
    public void testRoundRobinTaskMappingOnScaleUp() {
        assertEquals(
                mappings(to(0), to(1), to(2), to(3), to(), to(), to(), to(), to(), to()),
                SubtaskStateMapper.ROUND_ROBIN.getNewToOldSubtasksMapping(4, 10));

        assertEquals(
                mappings(to(0), to(1), to(2), to(3), to()),
                SubtaskStateMapper.ROUND_ROBIN.getNewToOldSubtasksMapping(4, 5));

        assertEquals(
                mappings(to(0), to(1), to(), to(), to()),
                SubtaskStateMapper.ROUND_ROBIN.getNewToOldSubtasksMapping(2, 5));

        assertEquals(
                mappings(to(0), to(), to(), to(), to()),
                SubtaskStateMapper.ROUND_ROBIN.getNewToOldSubtasksMapping(1, 5));
    }
}
