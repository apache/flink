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

package org.apache.flink.table.planner.plan.nodes.exec.serde;

import org.apache.flink.table.planner.plan.nodes.exec.spec.IntervalJoinSpec;
import org.apache.flink.table.planner.plan.nodes.exec.spec.JoinSpec;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.module.SimpleModule;

import org.apache.calcite.rex.RexNode;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

/** Tests for {@link IntervalJoinSpec} serialization and deserialization. */
public class IntervalJoinSpecJsonSerdeTest {
    private ObjectMapper mapper;

    @Before
    public void setUp() {
        mapper = new ObjectMapper();
        SimpleModule module = new SimpleModule();
        module.addSerializer(new RexNodeJsonSerializer());
        module.addDeserializer(RexNode.class, new RexNodeJsonDeserializer());
        mapper.registerModule(module);
    }

    @Test
    public void testWindowBoundsSerde() throws IOException {
        IntervalJoinSpec.WindowBounds windowBounds =
                new IntervalJoinSpec.WindowBounds(true, 0L, 10L, 1, 2);
        assertEquals(
                windowBounds,
                mapper.readValue(
                        mapper.writeValueAsString(windowBounds),
                        IntervalJoinSpec.WindowBounds.class));
    }

    @Test
    public void testIntervalJoinSpecSerde() throws IOException {
        JoinSpec joinSpec =
                new JoinSpec(
                        FlinkJoinType.ANTI,
                        new int[] {1},
                        new int[] {1},
                        new boolean[] {true},
                        null);
        IntervalJoinSpec.WindowBounds windowBounds =
                new IntervalJoinSpec.WindowBounds(true, 0L, 10L, 1, 2);
        IntervalJoinSpec actual = new IntervalJoinSpec(joinSpec, windowBounds);
        assertEquals(
                actual,
                mapper.readValue(mapper.writeValueAsString(actual), IntervalJoinSpec.class));
    }
}
