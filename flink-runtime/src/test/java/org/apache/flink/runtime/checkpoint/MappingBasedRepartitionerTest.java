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

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.runtime.io.network.api.writer.SubtaskStateMapper;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

/** Tests {@link MappingBasedRepartitioner}. */
public class MappingBasedRepartitionerTest {

    @Test
    public void testBroadcastRedistributeOnScaleDown() {
        List<List<String>> oldStates =
                Arrays.asList(
                        Arrays.asList("sub0state0", "sub0state1"),
                        Arrays.asList("sub1state0"),
                        Arrays.asList("sub2state0", "sub2state1"));

        assertEquals(
                Arrays.asList(
                        Arrays.asList(
                                "sub0state0",
                                "sub0state1",
                                "sub1state0",
                                "sub2state0",
                                "sub2state1"),
                        Arrays.asList(
                                "sub0state0",
                                "sub0state1",
                                "sub1state0",
                                "sub2state0",
                                "sub2state1")),
                repartition(oldStates, SubtaskStateMapper.FULL, 3, 2));
    }

    @Test
    public void testBroadcastRedistributeOnNoScale() {
        List<List<String>> oldStates =
                Arrays.asList(
                        Arrays.asList("sub0state0", "sub0state1"),
                        Arrays.asList("sub1state0"),
                        Arrays.asList("sub2state0", "sub2state1"));

        assertEquals(
                Arrays.asList(
                        Arrays.asList(
                                "sub0state0",
                                "sub0state1",
                                "sub1state0",
                                "sub2state0",
                                "sub2state1"),
                        Arrays.asList(
                                "sub0state0",
                                "sub0state1",
                                "sub1state0",
                                "sub2state0",
                                "sub2state1"),
                        Arrays.asList(
                                "sub0state0",
                                "sub0state1",
                                "sub1state0",
                                "sub2state0",
                                "sub2state1")),
                repartition(oldStates, SubtaskStateMapper.FULL, 3, 3));
    }

    @Test
    public void testBroadcastRedistributeOnScaleUp() {
        List<List<String>> oldStates =
                Arrays.asList(
                        Arrays.asList("sub0state0", "sub0state1"),
                        Arrays.asList("sub1state0"),
                        Arrays.asList("sub2state0", "sub2state1"));

        assertEquals(
                Arrays.asList(
                        Arrays.asList(
                                "sub0state0",
                                "sub0state1",
                                "sub1state0",
                                "sub2state0",
                                "sub2state1"),
                        Arrays.asList(
                                "sub0state0",
                                "sub0state1",
                                "sub1state0",
                                "sub2state0",
                                "sub2state1"),
                        Arrays.asList(
                                "sub0state0",
                                "sub0state1",
                                "sub1state0",
                                "sub2state0",
                                "sub2state1"),
                        Arrays.asList(
                                "sub0state0",
                                "sub0state1",
                                "sub1state0",
                                "sub2state0",
                                "sub2state1")),
                repartition(oldStates, SubtaskStateMapper.FULL, 3, 4));
    }

    @Test
    public void testRangeSelectorRedistributeOnScaleDown() {
        List<List<String>> oldStates =
                Arrays.asList(
                        Arrays.asList("sub0state0", "sub0state1"),
                        Arrays.asList("sub1state0"),
                        Arrays.asList("sub2state0", "sub2state1"));

        assertEquals(
                Arrays.asList(
                        Arrays.asList("sub0state0", "sub0state1", "sub1state0"),
                        Arrays.asList("sub1state0", "sub2state0", "sub2state1")),
                repartition(oldStates, SubtaskStateMapper.RANGE, 3, 2));
    }

    @Test
    public void testRangeSelectorRedistributeOnNoScale() {
        List<List<String>> oldStates =
                Arrays.asList(
                        Arrays.asList("sub0state0", "sub0state1"),
                        Arrays.asList("sub1state0"),
                        Arrays.asList("sub2state0", "sub2state1"));

        assertEquals(
                Arrays.asList(
                        Arrays.asList("sub0state0", "sub0state1"),
                        Arrays.asList("sub1state0"),
                        Arrays.asList("sub2state0", "sub2state1")),
                repartition(oldStates, SubtaskStateMapper.RANGE, 3, 3));
    }

    @Test
    public void testRangeSelectorRedistributeOnScaleUp() {
        List<List<String>> oldStates =
                Arrays.asList(
                        Arrays.asList("sub0state0", "sub0state1"),
                        Arrays.asList("sub1state0"),
                        Arrays.asList("sub2state0", "sub2state1"));

        assertEquals(
                Arrays.asList(
                        Arrays.asList("sub0state0", "sub0state1"),
                        Arrays.asList("sub0state0", "sub0state1", "sub1state0"),
                        Arrays.asList("sub1state0", "sub2state0", "sub2state1"),
                        Arrays.asList("sub2state0", "sub2state1")),
                repartition(oldStates, SubtaskStateMapper.RANGE, 3, 4));
    }

    @Test
    public void testRoundRobinRedistributeOnScaleDown() {
        List<List<String>> oldStates =
                Arrays.asList(
                        Arrays.asList("sub0state0", "sub0state1"),
                        Arrays.asList("sub1state0"),
                        Arrays.asList("sub2state0", "sub2state1"));

        assertEquals(
                Arrays.asList(
                        Arrays.asList("sub0state0", "sub0state1", "sub2state0", "sub2state1"),
                        Arrays.asList("sub1state0")),
                repartition(oldStates, SubtaskStateMapper.ROUND_ROBIN, 3, 2));

        assertEquals(
                Arrays.asList(
                        Arrays.asList(
                                "sub0state0",
                                "sub0state1",
                                "sub1state0",
                                "sub2state0",
                                "sub2state1")),
                repartition(oldStates, SubtaskStateMapper.ROUND_ROBIN, 3, 1));
    }

    @Test
    public void testRoundRobinRedistributeOnNoScale() {
        List<List<String>> oldStates =
                Arrays.asList(
                        Arrays.asList("sub0state0", "sub0state1"),
                        Arrays.asList("sub1state0"),
                        Arrays.asList("sub2state0", "sub2state1"));

        assertEquals(
                Arrays.asList(
                        Arrays.asList("sub0state0", "sub0state1"),
                        Arrays.asList("sub1state0"),
                        Arrays.asList("sub2state0", "sub2state1")),
                repartition(oldStates, SubtaskStateMapper.ROUND_ROBIN, 3, 3));
    }

    @Test
    public void testRoundRobinRedistributeOnScaleUp() {
        List<List<String>> oldStates =
                Arrays.asList(
                        Arrays.asList("sub0state0", "sub0state1"),
                        Arrays.asList("sub1state0"),
                        Arrays.asList("sub2state0", "sub2state1"));

        assertEquals(
                Arrays.asList(
                        Arrays.asList("sub0state0", "sub0state1"),
                        Arrays.asList("sub1state0"),
                        Arrays.asList("sub2state0", "sub2state1"),
                        Arrays.asList()),
                repartition(oldStates, SubtaskStateMapper.ROUND_ROBIN, 3, 4));

        assertEquals(
                Arrays.asList(
                        Arrays.asList("sub0state0", "sub0state1"),
                        Arrays.asList("sub1state0"),
                        Arrays.asList("sub2state0", "sub2state1"),
                        Arrays.asList(),
                        Arrays.asList()),
                repartition(oldStates, SubtaskStateMapper.ROUND_ROBIN, 3, 5));
    }

    private List<List<String>> repartition(
            List<List<String>> oldStates,
            SubtaskStateMapper mapper,
            int oldParalellism,
            int newParallelism) {
        return new MappingBasedRepartitioner<String>(
                        mapper.getNewToOldSubtasksMapping(oldParalellism, newParallelism))
                .repartitionState(oldStates, oldParalellism, newParallelism);
    }
}
