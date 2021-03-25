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

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/** Test SortSpec json ser/de. */
public class SortSpecSerdeTest {

    @Test
    public void testSortSpec() throws JsonProcessingException {
        SortSpec sortSpec =
                SortSpec.builder()
                        .addField(1, true, true)
                        .addField(2, true, false)
                        .addField(3, false, true)
                        .addField(4, false, false)
                        .build();
        ObjectMapper mapper = new ObjectMapper();
        assertEquals(
                sortSpec, mapper.readValue(mapper.writeValueAsString(sortSpec), SortSpec.class));
    }

    @Test
    public void testAny() throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        assertEquals(
                SortSpec.ANY,
                mapper.readValue(mapper.writeValueAsString(SortSpec.ANY), SortSpec.class));
    }
}
