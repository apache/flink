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

import org.apache.flink.table.planner.calcite.FlinkContext;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalTableSourceScan;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalWatermarkAssigner;

import org.apache.calcite.plan.RelOptRuleCall;


/**
 * Rule to push the {@link FlinkLogicalWatermarkAssigner} into the {@link FlinkLogicalTableSourceScan}.
 */
public class PushWatermarkIntoTableSourceScanRule extends PushWatermarkIntoTableSourceScanRuleBase {
	public static final PushWatermarkIntoTableSourceScanRule INSTANCE = new PushWatermarkIntoTableSourceScanRule();

	public PushWatermarkIntoTableSourceScanRule() {
		super(operand(FlinkLogicalWatermarkAssigner.class,
				operand(FlinkLogicalTableSourceScan.class, none())),
				"PushWatermarkIntoTableSourceScanRule");
	}

	@Override
	public boolean matches(RelOptRuleCall call) {
		FlinkLogicalTableSourceScan scan = call.rel(1);
		return supportsWatermarkPushDown(scan);
	}

	@Override
	public void onMatch(RelOptRuleCall call) {
		FlinkLogicalWatermarkAssigner watermarkAssigner = call.rel(0);
		FlinkLogicalTableSourceScan scan = call.rel(1);
		FlinkContext context = (FlinkContext) call.getPlanner().getContext();

		FlinkLogicalTableSourceScan newScan =
				getNewScan(watermarkAssigner, watermarkAssigner.watermarkExpr(), scan, context.getTableConfig(), true);

		call.transformTo(newScan);
	}
}
