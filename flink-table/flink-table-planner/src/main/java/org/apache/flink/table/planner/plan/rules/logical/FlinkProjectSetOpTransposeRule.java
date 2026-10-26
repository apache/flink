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

package org.apache.flink.table.planner.plan.rules.logical;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.rules.ProjectSetOpTransposeRule;

/**
 * Extends calcite's ProjectSetOpTransposeRule, modification: only matches {@link LogicalUnion}.
 *
 * <p>Calcite's rule matches any set operation and only skips distinct ones. Pushing a projection
 * into the inputs of EXCEPT ALL or INTERSECT ALL changes the result, because rows are matched on
 * all of their fields: dropping a field the projection doesn't need can make rows match that
 * differed before.
 */
public class FlinkProjectSetOpTransposeRule extends ProjectSetOpTransposeRule {

    public static final RelOptRule INSTANCE =
            new FlinkProjectSetOpTransposeRule(
                    Config.DEFAULT
                            .withOperandSupplier(
                                    b0 ->
                                            b0.operand(LogicalProject.class)
                                                    .oneInput(
                                                            b1 ->
                                                                    b1.operand(LogicalUnion.class)
                                                                            .anyInputs()))
                            .as(Config.class));

    protected FlinkProjectSetOpTransposeRule(Config config) {
        super(config);
    }
}
