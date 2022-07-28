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

import org.apache.flink.table.planner.plan.nodes.exec.spec.SortSpec;
import org.apache.flink.util.jackson.JacksonMapperFactory;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Test SortSpec json ser/de. */
public class SortSpecSerdeTest {
    private static final ObjectMapper OBJECT_MAPPER = JacksonMapperFactory.createObjectMapper();

    @Test
    public void testSortSpec() throws JsonProcessingException {
        SortSpec sortSpec =
                SortSpec.builder()
                        .addField(1, true, true)
                        .addField(2, true, false)
                        .addField(3, false, true)
                        .addField(4, false, false)
                        .build();
        assertThat(
                        OBJECT_MAPPER.readValue(
                                OBJECT_MAPPER.writeValueAsString(sortSpec), SortSpec.class))
                .isEqualTo(sortSpec);
    }

    @Test
    public void testAny() throws JsonProcessingException {
        assertThat(
                        OBJECT_MAPPER.readValue(
                                OBJECT_MAPPER.writeValueAsString(SortSpec.ANY), SortSpec.class))
                .isEqualTo(SortSpec.ANY);
    }
}
