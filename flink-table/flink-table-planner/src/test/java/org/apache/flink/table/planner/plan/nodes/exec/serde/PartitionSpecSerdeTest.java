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

import org.apache.flink.table.planner.plan.nodes.exec.spec.PartitionSpec;
import org.apache.flink.util.jackson.JacksonMapperFactory;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Test PartitionSpec json ser/de. */
public class PartitionSpecSerdeTest {

    private static final ObjectMapper OBJECT_MAPPER = JacksonMapperFactory.createObjectMapper();

    @Test
    public void testPartitionSpec() throws JsonProcessingException {
        PartitionSpec spec = new PartitionSpec(new int[] {1, 2, 3});
        assertThat(
                        OBJECT_MAPPER.readValue(
                                OBJECT_MAPPER.writeValueAsString(spec), PartitionSpec.class))
                .isEqualTo(spec);
    }

    @Test
    public void testAllInOne() throws JsonProcessingException {
        assertThat(
                        OBJECT_MAPPER.readValue(
                                OBJECT_MAPPER.writeValueAsString(PartitionSpec.ALL_IN_ONE),
                                PartitionSpec.class))
                .isEqualTo(PartitionSpec.ALL_IN_ONE);
    }
}
