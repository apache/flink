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

package org.apache.flink.api.java;

import org.apache.flink.api.common.Plan;
import org.apache.flink.api.common.PlanExecutor;
import org.apache.flink.configuration.Configuration;

/**
 * A utility for extracting an execution plan (as JSON) from a {@link Plan}.
 */
class ExecutionPlanUtil {

	/**
	 * Extracts the execution plan (as JSON) from the given {@link Plan}.
	 */
	static String getExecutionPlanAsJSON(Plan plan) {
		// make sure that we do not start an executor in any case here.
		// if one runs, fine, of not, we only create the class but disregard immediately afterwards
		PlanExecutor tempExecutor = PlanExecutor.createLocalExecutor(new Configuration());
		return tempExecutor.getOptimizerPlanAsJSON(plan);
	}
}
