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

package org.apache.flink.table.planner.plan.abilities.source;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class PartitionPushDownSpecTest {

    private List<Map<String, String>> partitions;

    @BeforeEach
    void beforeEach() {
        partitions =
                List.of(
                        Map.of("part2", "A", "part1", "B", "part3", "C"),
                        Map.of("part1", "C", "part2", "D"));
    }

    @Test
    void testDigestsEmpty() {
        PartitionPushDownSpec spec = new PartitionPushDownSpec(Collections.emptyList());
        assertThat(spec.getDigests(null)).isEqualTo("partitions=[]");
    }

    @Test
    void testDigestsSorted() {
        PartitionPushDownSpec spec = new PartitionPushDownSpec(partitions);
        assertThat(spec.getDigests(null))
                .isEqualTo("partitions=[{part1=B, part2=A, part3=C}, {part1=C, part2=D}]");
    }
}
