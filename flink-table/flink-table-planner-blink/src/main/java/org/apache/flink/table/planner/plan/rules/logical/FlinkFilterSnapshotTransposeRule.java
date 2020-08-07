/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.planner.plan.rules.logical;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Snapshot;
import org.apache.calcite.rel.logical.LogicalSnapshot;

/**
 * Planner rule that pushes Filter{@link org.apache.calcite.rel.core.Filter} past a LogicalSnapshot{@link org.apache.calcite.rel.logical.LogicalSnapshot}.
 * */
public class FlinkFilterSnapshotTransposeRule extends RelOptRule {

	public static final FlinkFilterSnapshotTransposeRule INSTNACE = new FlinkFilterSnapshotTransposeRule();

	public FlinkFilterSnapshotTransposeRule() {
		super(
				operand(Filter.class,
						operand(LogicalSnapshot.class, any())),
				"FlinkFilterSnapshotTransposeRule"
		);
	}

	@Override
	public void onMatch(RelOptRuleCall call) {
		Filter filter = call.rel(0);
		LogicalSnapshot snapshot = call.rel(1);

		Filter newFilter = filter.copy(filter.getTraitSet(), snapshot.getInput(), filter.getCondition());
		Snapshot newSnapshot = snapshot.copy(snapshot.getTraitSet(), newFilter, snapshot.getPeriod());
		call.transformTo(newSnapshot);
	}
}
