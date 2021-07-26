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

import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

/** Tests for {@link InputProperty} serialization and deserialization. */
@RunWith(Parameterized.class)
public class InputPropertySerdeTest {
    @Parameterized.Parameter public InputProperty inputProperty;

    @Test
    public void testExecEdgeSerde() throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        StringWriter writer = new StringWriter(100);
        try (JsonGenerator gen = mapper.getFactory().createGenerator(writer)) {
            gen.writeObject(inputProperty);
        }
        String json = writer.toString();
        InputProperty actual = mapper.readValue(json, InputProperty.class);
        assertEquals(inputProperty, actual);
    }

    @Parameterized.Parameters(name = "{0}")
    public static List<InputProperty> testData() {
        return Arrays.asList(
                InputProperty.DEFAULT,
                InputProperty.builder()
                        .requiredDistribution(InputProperty.hashDistribution(new int[] {0, 1}))
                        .damBehavior(InputProperty.DamBehavior.BLOCKING)
                        .priority(0)
                        .build(),
                InputProperty.builder()
                        .requiredDistribution(InputProperty.BROADCAST_DISTRIBUTION)
                        .damBehavior(InputProperty.DamBehavior.END_INPUT)
                        .priority(0)
                        .build(),
                InputProperty.builder()
                        .requiredDistribution(InputProperty.SINGLETON_DISTRIBUTION)
                        .damBehavior(InputProperty.DamBehavior.END_INPUT)
                        .priority(1)
                        .build(),
                InputProperty.builder()
                        .requiredDistribution(InputProperty.ANY_DISTRIBUTION)
                        .damBehavior(InputProperty.DamBehavior.PIPELINED)
                        .priority(2)
                        .build(),
                InputProperty.builder()
                        .requiredDistribution(InputProperty.UNKNOWN_DISTRIBUTION)
                        .damBehavior(InputProperty.DamBehavior.PIPELINED)
                        .priority(0)
                        .build());
    }
}
