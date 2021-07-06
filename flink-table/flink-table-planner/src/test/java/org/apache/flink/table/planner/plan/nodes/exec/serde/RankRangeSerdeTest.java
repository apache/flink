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

import org.apache.flink.table.runtime.operators.rank.ConstantRankRange;
import org.apache.flink.table.runtime.operators.rank.ConstantRankRangeWithoutEnd;
import org.apache.flink.table.runtime.operators.rank.RankRange;
import org.apache.flink.table.runtime.operators.rank.VariableRankRange;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/** Test RankRange json ser/de. */
public class RankRangeSerdeTest {

    @Test
    public void testRankRange() throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        RankRange[] ranges =
                new RankRange[] {
                    new ConstantRankRange(1, 2),
                    new VariableRankRange(3),
                    new ConstantRankRangeWithoutEnd(4)
                };
        for (RankRange range : ranges) {
            RankRange result = mapper.readValue(mapper.writeValueAsString(range), RankRange.class);
            assertEquals(range.toString(), result.toString());
        }
    }
}
