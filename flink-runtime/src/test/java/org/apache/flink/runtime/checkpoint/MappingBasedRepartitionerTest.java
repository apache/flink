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

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests {@link MappingBasedRepartitioner}. */
class MappingBasedRepartitionerTest {

    @Test
    void testBroadcastRedistributeOnScaleDown() {
        List<List<String>> oldStates =
                Arrays.asList(
                        Arrays.asList("sub0state0", "sub0state1"),
                        Collections.singletonList("sub1state0"),
                        Arrays.asList("sub2state0", "sub2state1"));

        assertThat(repartition(oldStates, SubtaskStateMapper.FULL, 3, 2))
                .isEqualTo(
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
                                        "sub2state1")));
    }

    @Test
    void testBroadcastRedistributeOnNoScale() {
        List<List<String>> oldStates =
                Arrays.asList(
                        Arrays.asList("sub0state0", "sub0state1"),
                        Collections.singletonList("sub1state0"),
                        Arrays.asList("sub2state0", "sub2state1"));

        assertThat(repartition(oldStates, SubtaskStateMapper.FULL, 3, 3))
                .isEqualTo(
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
                                        "sub2state1")));
    }

    @Test
    void testBroadcastRedistributeOnScaleUp() {
        List<List<String>> oldStates =
                Arrays.asList(
                        Arrays.asList("sub0state0", "sub0state1"),
                        Collections.singletonList("sub1state0"),
                        Arrays.asList("sub2state0", "sub2state1"));

        assertThat(repartition(oldStates, SubtaskStateMapper.FULL, 3, 4))
                .isEqualTo(
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
                                        "sub2state1")));
    }

    @Test
    void testRangeSelectorRedistributeOnScaleDown() {
        List<List<String>> oldStates =
                Arrays.asList(
                        Arrays.asList("sub0state0", "sub0state1"),
                        Collections.singletonList("sub1state0"),
                        Arrays.asList("sub2state0", "sub2state1"));

        assertThat(repartition(oldStates, SubtaskStateMapper.RANGE, 3, 2))
                .isEqualTo(
                        Arrays.asList(
                                Arrays.asList("sub0state0", "sub0state1", "sub1state0"),
                                Arrays.asList("sub1state0", "sub2state0", "sub2state1")));
    }

    @Test
    void testRangeSelectorRedistributeOnNoScale() {
        List<List<String>> oldStates =
                Arrays.asList(
                        Arrays.asList("sub0state0", "sub0state1"),
                        Collections.singletonList("sub1state0"),
                        Arrays.asList("sub2state0", "sub2state1"));

        assertThat(repartition(oldStates, SubtaskStateMapper.RANGE, 3, 3))
                .isEqualTo(
                        Arrays.asList(
                                Arrays.asList("sub0state0", "sub0state1"),
                                Collections.singletonList("sub1state0"),
                                Arrays.asList("sub2state0", "sub2state1")));
    }

    @Test
    void testRangeSelectorRedistributeOnScaleUp() {
        List<List<String>> oldStates =
                Arrays.asList(
                        Arrays.asList("sub0state0", "sub0state1"),
                        Collections.singletonList("sub1state0"),
                        Arrays.asList("sub2state0", "sub2state1"));

        assertThat(repartition(oldStates, SubtaskStateMapper.RANGE, 3, 4))
                .isEqualTo(
                        Arrays.asList(
                                Arrays.asList("sub0state0", "sub0state1"),
                                Arrays.asList("sub0state0", "sub0state1", "sub1state0"),
                                Arrays.asList("sub1state0", "sub2state0", "sub2state1"),
                                Arrays.asList("sub2state0", "sub2state1")));
    }

    @Test
    void testRoundRobinRedistributeOnScaleDown() {
        List<List<String>> oldStates =
                Arrays.asList(
                        Arrays.asList("sub0state0", "sub0state1"),
                        Collections.singletonList("sub1state0"),
                        Arrays.asList("sub2state0", "sub2state1"));

        assertThat(repartition(oldStates, SubtaskStateMapper.ROUND_ROBIN, 3, 2))
                .isEqualTo(
                        Arrays.asList(
                                Arrays.asList(
                                        "sub0state0", "sub0state1", "sub2state0", "sub2state1"),
                                Collections.singletonList("sub1state0")));

        assertThat(repartition(oldStates, SubtaskStateMapper.ROUND_ROBIN, 3, 1))
                .isEqualTo(
                        Collections.singletonList(
                                Arrays.asList(
                                        "sub0state0",
                                        "sub0state1",
                                        "sub1state0",
                                        "sub2state0",
                                        "sub2state1")));
    }

    @Test
    void testRoundRobinRedistributeOnNoScale() {
        List<List<String>> oldStates =
                Arrays.asList(
                        Arrays.asList("sub0state0", "sub0state1"),
                        Collections.singletonList("sub1state0"),
                        Arrays.asList("sub2state0", "sub2state1"));

        assertThat(repartition(oldStates, SubtaskStateMapper.ROUND_ROBIN, 3, 3))
                .isEqualTo(
                        Arrays.asList(
                                Arrays.asList("sub0state0", "sub0state1"),
                                Collections.singletonList("sub1state0"),
                                Arrays.asList("sub2state0", "sub2state1")));
    }

    @Test
    void testRoundRobinRedistributeOnScaleUp() {
        List<List<String>> oldStates =
                Arrays.asList(
                        Arrays.asList("sub0state0", "sub0state1"),
                        Collections.singletonList("sub1state0"),
                        Arrays.asList("sub2state0", "sub2state1"));

        assertThat(repartition(oldStates, SubtaskStateMapper.ROUND_ROBIN, 3, 4))
                .isEqualTo(
                        Arrays.asList(
                                Arrays.asList("sub0state0", "sub0state1"),
                                Collections.singletonList("sub1state0"),
                                Arrays.asList("sub2state0", "sub2state1"),
                                Collections.emptyList()));

        assertThat(repartition(oldStates, SubtaskStateMapper.ROUND_ROBIN, 3, 5))
                .isEqualTo(
                        Arrays.asList(
                                Arrays.asList("sub0state0", "sub0state1"),
                                Collections.singletonList("sub1state0"),
                                Arrays.asList("sub2state0", "sub2state1"),
                                Collections.emptyList(),
                                Collections.emptyList()));
    }

    private List<List<String>> repartition(
            List<List<String>> oldStates,
            SubtaskStateMapper mapper,
            int oldParallelism,
            int newParallelism) {
        return new MappingBasedRepartitioner<String>(
                        mapper.getNewToOldSubtasksMapping(oldParallelism, newParallelism))
                .repartitionState(oldStates, oldParallelism, newParallelism);
    }
}
