/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.api;

import org.apache.flink.configuration.ConfigOption;

import static org.apache.flink.configuration.ConfigOptions.key;

/**
 * This class holds configuration constants used by Flink's table planner module.
 */
public class PlannerConfigOptions {

	// ------------------------------------------------------------------------
	//  Optimizer Options
	// ------------------------------------------------------------------------

	public static final ConfigOption<Integer> SQL_OPTIMIZER_CNF_NODES_LIMIT =
			key("sql.optimizer.cnf.nodes.limit")
					.defaultValue(-1)
					.withDescription("When converting to conjunctive normal form (CNF), fail if the expression" +
							" exceeds this threshold; the threshold is expressed in terms of number of nodes " +
							"(only count RexCall node, including leaves and interior nodes). Negative number to" +
							" use the default threshold: double of number of nodes.");
}
