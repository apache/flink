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

package org.apache.flink.table.planner.functions.casting.rules;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.planner.functions.casting.CastRulePredicate;
import org.apache.flink.table.planner.functions.casting.CodeGeneratorCastRule;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

/** Upcasting to long for smaller types. */
@Internal
public class UpcastToBigIntCastRule extends AbstractExpressionCodeGeneratorCastRule<Object, Long> {

    public static final UpcastToBigIntCastRule INSTANCE = new UpcastToBigIntCastRule();

    private UpcastToBigIntCastRule() {
        super(
                CastRulePredicate.builder()
                        .input(LogicalTypeRoot.TINYINT)
                        .input(LogicalTypeRoot.SMALLINT)
                        .input(LogicalTypeRoot.INTEGER)
                        .target(LogicalTypeRoot.BIGINT)
                        .build());
    }

    @Override
    public String generateExpression(
            CodeGeneratorCastRule.Context context,
            String inputTerm,
            LogicalType inputLogicalType,
            LogicalType targetLogicalType) {
        return "((long)(" + inputTerm + "))";
    }
}
