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

	public static final ConfigOption<String> SQL_OPTIMIZER_AGG_PHASE_ENFORCER =
			key("sql.optimizer.agg.phase.enforcer")
					.defaultValue("NONE")
					.withDescription("Strategy for agg phase. Only NONE, TWO_PHASE or ONE_PHASE can be set.\n" +
							"NONE: No special enforcer for aggregate stage. Whether to choose two stage aggregate or one" +
							" stage aggregate depends on cost. \n" +
							"TWO_PHASE: Enforce to use two stage aggregate which has localAggregate and globalAggregate. " +
							"NOTE: If aggregate call does not support split into two phase, still use one stage aggregate.\n" +
							"ONE_PHASE: Enforce to use one stage aggregate which only has CompleteGlobalAggregate.");

	public static final ConfigOption<Boolean> SQL_OPTIMIZER_SHUFFLE_PARTIAL_KEY_ENABLED =
			key("sql.optimizer.shuffle.partial-key.enabled")
					.defaultValue(false)
					.withDescription("Enables shuffle by partial partition keys. " +
							"For example, A join with join condition: L.c1 = R.c1 and L.c2 = R.c2. " +
							"If this flag is enabled, there are 3 shuffle strategy:\n " +
							"1. L and R shuffle by c1 \n" +
							"2. L and R shuffle by c2\n" +
							"3. L and R shuffle by c1 and c2\n" +
							"It can reduce some shuffle cost someTimes.");

	public static final ConfigOption<Boolean> SQL_OPTIMIZER_SMJ_REMOVE_SORT_ENABLED =
			key("sql.optimizer.smj.remove-sort.enabled")
					.defaultValue(false)
					.withDescription("When true, the optimizer will try to remove redundant sort for SortMergeJoin. " +
							"However that will increase optimization time. Default value is false.");

	public static final ConfigOption<Long> SQL_OPTIMIZER_HASH_JOIN_BROADCAST_THRESHOLD =
			key("sql.optimizer.hash-join.broadcast.threshold")
					.defaultValue(1024 * 1024L)
					.withDescription("Maximum size in bytes for data that could be broadcast to each parallel " +
							"instance that holds a partition of all data when performing a hash join. " +
							"Broadcast will be disabled if the value is -1.");

	public static final ConfigOption<Double> SQL_OPTIMIZER_SEMI_JOIN_BUILD_DISTINCT_NDV_RATIO =
			key("sql.optimizer.semi-anti-join.build-distinct.ndv-ratio")
					.defaultValue(0.8)
					.withDescription("When the semi-side of semi/anti join can distinct a lot of data in advance," +
							" we will add distinct node before semi/anti join.");

	public static final ConfigOption<Long> SQL_OPTIMIZER_JOIN_NULL_FILTER_THRESHOLD =
			key("sql.optimizer.join.null.filter.threshold")
					.defaultValue(2000000L)
					.withDescription("If the source of InnerJoin has nullCount more than this value, " +
							"it will add a null filter (possibly be pushDowned) before the join, filter" +
							" null values to avoid the impact of null values on the single join node.");

	public static final ConfigOption<Boolean> SQL_OPTIMIZER_DATA_SKEW_DISTINCT_AGG_ENABLED =
			key("sql.optimizer.data-skew.distinct-agg.enabled")
					.defaultValue(false)
					.withDescription("Tell the optimizer whether there exists data skew in distinct aggregation\n"
							+ "so as to enable the aggregation split optimization.");

	public static final ConfigOption<Integer> SQL_OPTIMIZER_DATA_SKEW_DISTINCT_AGG_BUCKET =
			key("sql.optimizer.data-skew.distinct-agg.bucket")
					.defaultValue(1024)
					.withDescription("Configure the number of buckets when splitting distinct aggregation.");

	public static final ConfigOption<Boolean> SQL_OPTIMIZER_INCREMENTAL_AGG_ENABLED =
			key("sql.optimizer.incremental-agg.enabled")
					.defaultValue(true)
					.withDescription("Whether to enable incremental aggregate.");


	public static final ConfigOption<Boolean> SQL_OPTIMIZER_REUSE_SUB_PLAN_ENABLED =
			key("sql.optimizer.reuse.sub-plan.enabled")
					.defaultValue(true)
					.withDescription("When true, the optimizer will try to find out duplicated " +
							"sub-plan and reuse them.");

	public static final ConfigOption<Boolean> SQL_OPTIMIZER_REUSE_TABLE_SOURCE_ENABLED =
			key("sql.optimizer.reuse.table-source.enabled")
					.defaultValue(true)
					.withDescription("When true, the optimizer will try to find out duplicated table-source and " +
							"reuse them. This works only when " + SQL_OPTIMIZER_REUSE_SUB_PLAN_ENABLED + " is true.");

	public static final ConfigOption<Boolean> SQL_OPTIMIZER_REUSE_OPTIMIZE_BLOCK_WITH_DIGEST_ENABLED =
			key("sql.optimizer.reuse.optimize-block.with-digest.enabled")
					.defaultValue(false)
					.withDescription("When true, the optimizer will try to find out duplicated sub-plan by digest " +
							"to build optimize block. Each optimize block will be optimized independently.");

	public static final ConfigOption<Boolean> SQL_OPTIMIZER_UNIONALL_AS_BREAKPOINT_DISABLED =
			key("sql.optimizer.unionall-as-breakpoint.disabled")
					.defaultValue(false)
					.withDescription("Disable union all as breakpoint when constructing RelNodeBlock");

	public static final ConfigOption<Long> SQL_OPTIMIZER_ROWS_PER_LOCALAGG =
			key("sql.optimizer.rows-per-localAgg")
					.defaultValue(1000000L)
					.withDescription("Sets how many rows one localAgg processes. We will infer agg degree to decide whether " +
							"to use localAgg according to it.");

}
