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

import org.apache.flink.table.planner.plan.utils.LookupJoinUtil;
import org.apache.flink.table.types.logical.BigIntType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectReader;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectWriter;

import org.apache.calcite.rex.RexBuilder;
import org.junit.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

/** Test LookupKey json ser/de. */
public class LookupKeySerdeTest {

    @Test
    public void testLookupKey() throws IOException {
        SerdeContext serdeCtx = JsonSerdeTestUtil.configuredSerdeContext();
        ObjectReader objectReader = JsonSerdeUtil.createObjectReader(serdeCtx);
        ObjectWriter objectWriter = JsonSerdeUtil.createObjectWriter(serdeCtx);

        LookupJoinUtil.LookupKey[] lookupKeys =
                new LookupJoinUtil.LookupKey[] {
                    new LookupJoinUtil.ConstantLookupKey(
                            new BigIntType(),
                            new RexBuilder(serdeCtx.getTypeFactory()).makeLiteral("a")),
                    new LookupJoinUtil.FieldRefLookupKey(3)
                };
        for (LookupJoinUtil.LookupKey lookupKey : lookupKeys) {
            LookupJoinUtil.LookupKey result =
                    objectReader.readValue(
                            objectWriter.writeValueAsString(lookupKey),
                            LookupJoinUtil.LookupKey.class);
            assertThat(result).isEqualTo(lookupKey);
        }
    }
}
