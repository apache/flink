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

import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalCalc;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalCorrelate;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalTableFunctionScan;

import org.apache.calcite.plan.RelOptRule;

/**
 * Rule will split the Python {@link FlinkLogicalTableFunctionScan} with Java calls or the Java
 * {@link FlinkLogicalTableFunctionScan} with Python calls into a {@link FlinkLogicalCalc} which
 * will be the left input of the new {@link FlinkLogicalCorrelate} and a new {@link
 * FlinkLogicalTableFunctionScan}.
 */
public class PythonCorrelateSplitRule {

    public static final RelOptRule INSTANCE =
            RemoteCorrelateSplitRule.Config.createDefault(new PythonRemoteCallFinder()).toRule();
}
