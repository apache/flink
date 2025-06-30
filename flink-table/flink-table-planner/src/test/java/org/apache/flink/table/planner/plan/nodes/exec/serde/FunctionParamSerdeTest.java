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

import org.apache.flink.table.planner.plan.utils.FunctionCallUtil;
import org.apache.flink.table.types.logical.BigIntType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectReader;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectWriter;

import org.apache.calcite.rex.RexBuilder;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

/** Test LookupKey json ser/de. */
class FunctionParamSerdeTest {

    @Test
    void testSerdeFunctionParams() throws IOException {
        SerdeContext serdeCtx = JsonSerdeTestUtil.configuredSerdeContext();
        ObjectReader objectReader = CompiledPlanSerdeUtil.createJsonObjectReader(serdeCtx);
        ObjectWriter objectWriter = CompiledPlanSerdeUtil.createJsonObjectWriter(serdeCtx);

        FunctionCallUtil.FunctionParam[] functionParams =
                new FunctionCallUtil.FunctionParam[] {
                    new FunctionCallUtil.Constant(
                            new BigIntType(),
                            new RexBuilder(serdeCtx.getTypeFactory()).makeLiteral("a")),
                    new FunctionCallUtil.FieldRef(3)
                };
        for (FunctionCallUtil.FunctionParam param : functionParams) {
            FunctionCallUtil.FunctionParam result =
                    objectReader.readValue(
                            objectWriter.writeValueAsString(param),
                            FunctionCallUtil.FunctionParam.class);
            assertThat(result).isEqualTo(param);
        }
    }
}
