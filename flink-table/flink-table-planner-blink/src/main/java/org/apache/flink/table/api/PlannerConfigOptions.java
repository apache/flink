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

import org.apache.flink.annotation.docs.Documentation;
import org.apache.flink.configuration.ConfigOption;

import static org.apache.flink.configuration.ConfigOptions.key;

/**
 * This class holds configuration constants used by Flink's table planner module.
 */
public class PlannerConfigOptions {

	// ------------------------------------------------------------------------
	//  Optimizer Options
	// ------------------------------------------------------------------------
	@Documentation.ExcludeFromDocumentation(value = "Just for corner cases that sql is converted to hundreds of thousands of CNF nodes.")
	@Documentation.TableMeta(execMode = Documentation.ExecMode.BOTH)
	public static final ConfigOption<Integer> SQL_OPTIMIZER_CNF_NODES_LIMIT =
			key("sql.optimizer.cnf.nodes.limit")
					.defaultValue(-1)
					.withDescription("When converting to conjunctive normal form (CNF, like '(a AND b) OR c' will be " +
							"converted to '(a OR c) AND (b OR c)'), fail if the expression  exceeds this threshold; " +
							"(e.g. predicate in TPC-DS q41.sql will be converted to hundreds of thousands of CNF nodes.) " +
							"the threshold is expressed in terms of number of nodes  (only count RexCall node, " +
							"including leaves and interior nodes). Negative number to use the default threshold: double of number of nodes.");

	@Documentation.TableMeta(execMode = Documentation.ExecMode.BOTH)
	public static final ConfigOption<String> SQL_OPTIMIZER_AGG_PHASE_ENFORCER =
			key("sql.optimizer.agg-phase.strategy")
					.defaultValue("NONE")
					.withDescription("Strategy for aggregate phase. Only NONE, TWO_PHASE or ONE_PHASE can be set.\n" +
							"NONE: No special enforcer for aggregate stage. Whether to choose two stage aggregate or one" +
							" stage aggregate depends on cost. \n" +
							"TWO_PHASE: Enforce to use two stage aggregate which has localAggregate and globalAggregate. " +
							"NOTE: If aggregate call does not support split into two phase, still use one stage aggregate.\n" +
							"ONE_PHASE: Enforce to use one stage aggregate which only has CompleteGlobalAggregate.");

	@Documentation.TableMeta(execMode = Documentation.ExecMode.BATCH)
	public static final ConfigOption<Boolean> SQL_OPTIMIZER_SHUFFLE_PARTIAL_KEY_ENABLED =
			key("sql.optimizer.shuffle.partial-key.enabled")
					.defaultValue(false)
					.withDescription("Enables shuffling by partial partition keys. " +
							"For example, A join with join condition: L.c1 = R.c1 and L.c2 = R.c2. " +
							"If this flag is enabled, there are 3 shuffle strategy:\n " +
							"1. L and R shuffle by c1 \n" +
							"2. L and R shuffle by c2\n" +
							"3. L and R shuffle by c1 and c2\n" +
							"It can reduce some shuffle cost someTimes.");

	@Documentation.ExcludeFromDocumentation(value = "Temporary solution to enable optimization that removes redundant sort for SortMergeJoin, " +
			"and this config option will be removed later.")
	@Documentation.TableMeta(execMode = Documentation.ExecMode.BATCH)
	public static final ConfigOption<Boolean> SQL_OPTIMIZER_SMJ_REMOVE_SORT_ENABLED =
			key("sql.optimizer.sortmergejoin.remove-sort.enabled")
					.defaultValue(false)
					.withDescription("When true, the optimizer will try to remove redundant sort for SortMergeJoin. " +
							"However that will increase optimization time. Default value is false.");

	@Documentation.TableMeta(execMode = Documentation.ExecMode.BATCH)
	public static final ConfigOption<Long> SQL_OPTIMIZER_BROADCAST_JOIN_THRESHOLD =
			key("sql.optimizer.broadcast.join.threshold")
					.defaultValue(1024 * 1024L)
					.withDescription("Configures the maximum size in bytes for a table that will be broadcast to all worker " +
							"nodes when performing a join.  By setting this value to -1 broadcasting can be disabled. ");

	@Documentation.ExcludeFromDocumentation(value = "Just for corner cases, when the cbo is totally wrong, " +
			"we can adjust this value to reduce useless computation.")
	@Documentation.TableMeta(execMode = Documentation.ExecMode.BATCH)
	public static final ConfigOption<Double> SQL_OPTIMIZER_SEMI_JOIN_BUILD_DISTINCT_NDV_RATIO =
			key("sql.optimizer.semi-anti-join.build-distinct.ndv-ratio")
					.defaultValue(0.8)
					.withDescription("In order to reduce the amount of data on semi/anti join's" +
							" build side, we will add distinct node before semi/anti join when" +
							"  the semi-side or semi/anti join can distinct a lot of data in advance." +
							" We add this configuration to help the optimizer to decide whether to" +
							" add the distinct.");

	@Documentation.ExcludeFromDocumentation(value = "Just for corner cases, when the cbo is totally wrong, " +
			"we can adjust this value to reduce useless filter.")
	@Documentation.TableMeta(execMode = Documentation.ExecMode.BATCH)
	public static final ConfigOption<Long> SQL_OPTIMIZER_JOIN_NULL_FILTER_THRESHOLD =
			key("sql.optimizer.join.null.filter.threshold")
					.defaultValue(2000000L)
					.withDescription("To avoid the impact of null values on the single join node, " +
							"We will add a null filter (possibly be pushed down) before the join to filter" +
							" null values when the source of InnerJoin has nullCount more than this value.");

	@Documentation.TableMeta(execMode = Documentation.ExecMode.STREAMING)
	public static final ConfigOption<Boolean> SQL_OPTIMIZER_DATA_SKEW_DISTINCT_AGG_ENABLED =
			key("sql.optimizer.data-skew.distinct-agg.enabled")
					.defaultValue(false)
					.withDescription("Tell the optimizer whether there is data skew in distinct aggregation. " +
							"For example: COUNT(DISTINCT col), SUM(DISTINCT col). " +
							"If true, this will enable the optimizer to split distinct aggregation into two level. " +
							"This will increase some overhead, e.g. network shuffle, " +
							"but gives the ability to scale-up the job. Default is false.");

	@Documentation.TableMeta(execMode = Documentation.ExecMode.STREAMING)
	public static final ConfigOption<Integer> SQL_OPTIMIZER_DATA_SKEW_DISTINCT_AGG_BUCKET =
			key("sql.optimizer.data-skew.distinct-agg.bucket")
					.defaultValue(1024)
					.withDescription("Configure the number of buckets when splitting distinct aggregation. " +
							"The number is used in the first level aggregation to calculate a bucket key " +
							"'hash_code(distinct_key) % BUCKET_NUM' which is used as a group by key after splitting.");

	@Documentation.ExcludeFromDocumentation(value = "We do not find a bad case yet that need to change this configuration value. " +
			"So we don't want to expose this configuration to users currently.")
	@Documentation.TableMeta(execMode = Documentation.ExecMode.STREAMING)
	public static final ConfigOption<Boolean> SQL_OPTIMIZER_INCREMENTAL_AGG_ENABLED =
			key("sql.optimizer.incremental-agg.enabled")
					.defaultValue(true)
					.withDescription("When both local aggregation and distinct aggregation splitting are enabled, " +
							"a distinct aggregation will be optimized into four aggregations, " +
							"i.e., local-agg1, global-agg1, local-agg2 and global-Agg2. " +
							"We can combine global-agg1 and local-agg2 into a single operator " +
							"(we call it incremental agg because it receives incremental accumulators and output incremental results). " +
							"In this way, we can reduce some state overhead and resources. Default is enabled.");

	@Documentation.TableMeta(execMode = Documentation.ExecMode.BOTH)
	public static final ConfigOption<Boolean> SQL_OPTIMIZER_REUSE_SUB_PLAN_ENABLED =
			key("sql.optimizer.reuse.sub-plan.enabled")
					.defaultValue(true)
					.withDescription("When it is true, optimizer will try to find out duplicated " +
							"sub-plan and reuse them.");

	@Documentation.TableMeta(execMode = Documentation.ExecMode.BOTH)
	public static final ConfigOption<Boolean> SQL_OPTIMIZER_REUSE_TABLE_SOURCE_ENABLED =
			key("sql.optimizer.reuse.table-source.enabled")
					.defaultValue(true)
					.withDescription("When it is true, optimizer will try to find out duplicated table-source and " +
							"reuse them. This works only when " + SQL_OPTIMIZER_REUSE_SUB_PLAN_ENABLED.key() + " is true.");

	@Documentation.ExcludeFromDocumentation(value = "The optimizer algorithm is unstable, and will be improved later. " +
			"This config option is just for corner case")
	@Documentation.TableMeta(execMode = Documentation.ExecMode.BOTH)
	public static final ConfigOption<Boolean> SQL_OPTIMIZER_REUSE_OPTIMIZE_BLOCK_WITH_DIGEST_ENABLED =
			key("sql.optimizer.reuse.optimize-block.with-digest.enabled")
					.defaultValue(false)
					.withDescription("When true, the optimizer will try to find out duplicated sub-plan by digest " +
							"to build optimize block(a.k.a. common sub-graph). Each optimize block will be optimized independently.");

	@Documentation.ExcludeFromDocumentation(value = "Experimental control parameters.")
	@Documentation.TableMeta(execMode = Documentation.ExecMode.BOTH)
	public static final ConfigOption<Boolean> SQL_OPTIMIZER_UNIONALL_AS_BREAKPOINT_DISABLED =
			key("sql.optimizer.unionall-as-breakpoint.disabled")
					.defaultValue(false)
					.withDescription("Disable union-all node as breakpoint when constructing common sub-graph.");

	@Documentation.ExcludeFromDocumentation(value = "Most users do not need to care about this configuration and this is a temporary solution.")
	@Documentation.TableMeta(execMode = Documentation.ExecMode.BATCH)
	public static final ConfigOption<Long> SQL_OPTIMIZER_ROWS_PER_LOCALAGG =
			key("sql.optimizer.rows-per-local-agg")
					.defaultValue(1000000L)
					.withDescription("Sets estimated number of records that one local-agg processes. Optimizer will infer whether to use local/global aggregate according to it.");

	@Documentation.TableMeta(execMode = Documentation.ExecMode.BOTH)
	public static final ConfigOption<Boolean> SQL_OPTIMIZER_PREDICATE_PUSHDOWN_ENABLED =
			key("sql.optimizer.predicate-pushdown.enabled")
					.defaultValue(true)
					.withDescription("If it is true, enable predicate pushdown to the FilterableTableSource. " +
			"Default value is true.");

	@Documentation.TableMeta(execMode = Documentation.ExecMode.BOTH)
	public static final ConfigOption<Boolean> SQL_OPTIMIZER_JOIN_REORDER_ENABLED =
			key("sql.optimizer.join-reorder.enabled")
					.defaultValue(false)
					.withDescription("Enables join reorder in optimizer cbo. Default is disabled.");
}
