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
 *
 */

package org.apache.flink.optimizer.plandump;

import org.apache.flink.api.common.Plan;
import org.apache.flink.api.java.ExecutionPlanUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.optimizer.DataStatistics;
import org.apache.flink.optimizer.Optimizer;
import org.apache.flink.optimizer.costs.DefaultCostEstimator;
import org.apache.flink.optimizer.plan.OptimizedPlan;

/**
 * Utility for extracting an execution plan (as JSON) from a given {@link Plan}.
 *
 * <p>We need this util here in the optimizer because it is the only module that has {@link
 * Optimizer}, {@link OptimizedPlan}, and {@link PlanJSONDumpGenerator} available. We use this
 * reflectively from the batch execution environments to generate the plan, which we cannot do
 * there. It is used from {@link ExecutionPlanUtil}.
 */
@SuppressWarnings("unused")
public class ExecutionPlanJSONGenerator implements ExecutionPlanUtil.ExecutionPlanJSONGenerator {

	@Override
	public String getExecutionPlan(Plan plan) {
		Optimizer opt = new Optimizer(
				new DataStatistics(),
				new DefaultCostEstimator(),
				new Configuration());
		OptimizedPlan optPlan = opt.compile(plan);
		return new PlanJSONDumpGenerator().getOptimizerPlanAsJSON(optPlan);
	}
}
