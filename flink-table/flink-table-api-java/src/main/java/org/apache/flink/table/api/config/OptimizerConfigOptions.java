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

package org.apache.flink.table.api.config;

import org.apache.flink.annotation.docs.Documentation;
import org.apache.flink.configuration.ConfigOption;

import static org.apache.flink.configuration.ConfigOptions.key;

/**
 * This class holds configuration constants used by Flink's table planner module.
 *
 * <p>This is only used for the Blink planner.
 *
 * <p>NOTE: All option keys in this class must start with "table.optimizer".
 */
public class OptimizerConfigOptions {

	// ------------------------------------------------------------------------
	//  Optimizer Options
	// ------------------------------------------------------------------------
	@Documentation.TableOption(execMode = Documentation.ExecMode.BATCH_STREAMING)
	public static final ConfigOption<String> TABLE_OPTIMIZER_AGG_PHASE_STRATEGY =
		key("table.optimizer.agg-phase-strategy")
			.defaultValue("AUTO")
			.withDescription("Strategy for aggregate phase. Only AUTO, TWO_PHASE or ONE_PHASE can be set.\n" +
				"AUTO: No special enforcer for aggregate stage. Whether to choose two stage aggregate or one" +
				" stage aggregate depends on cost. \n" +
				"TWO_PHASE: Enforce to use two stage aggregate which has localAggregate and globalAggregate. " +
				"Note that if aggregate call does not support optimize into two phase, we will still use one stage aggregate.\n" +
				"ONE_PHASE: Enforce to use one stage aggregate which only has CompleteGlobalAggregate.");

	@Documentation.TableOption(execMode = Documentation.ExecMode.BATCH)
	public static final ConfigOption<Long> TABLE_OPTIMIZER_BROADCAST_JOIN_THRESHOLD =
		key("table.optimizer.join.broadcast-threshold")
			.defaultValue(1024 * 1024L)
			.withDescription("Configures the maximum size in bytes for a table that will be broadcast to all worker " +
				"nodes when performing a join. By setting this value to -1 to disable broadcasting.");

	@Documentation.TableOption(execMode = Documentation.ExecMode.STREAMING)
	public static final ConfigOption<Boolean> TABLE_OPTIMIZER_DISTINCT_AGG_SPLIT_ENABLED =
		key("table.optimizer.distinct-agg.split.enabled")
			.defaultValue(false)
			.withDescription("Tells the optimizer whether to split distinct aggregation " +
				"(e.g. COUNT(DISTINCT col), SUM(DISTINCT col)) into two level. " +
				"The first aggregation is shuffled by an additional key which is calculated using " +
				"the hashcode of distinct_key and number of buckets. This optimization is very useful " +
				"when there is data skew in distinct aggregation and gives the ability to scale-up the job. " +
				"Default is false.");

	@Documentation.TableOption(execMode = Documentation.ExecMode.STREAMING)
	public static final ConfigOption<Integer> TABLE_OPTIMIZER_DISTINCT_AGG_SPLIT_BUCKET_NUM =
		key("table.optimizer.distinct-agg.split.bucket-num")
			.defaultValue(1024)
			.withDescription("Configure the number of buckets when splitting distinct aggregation. " +
				"The number is used in the first level aggregation to calculate a bucket key " +
				"'hash_code(distinct_key) % BUCKET_NUM' which is used as an additional group key after splitting.");

	@Documentation.TableOption(execMode = Documentation.ExecMode.BATCH_STREAMING)
	public static final ConfigOption<Boolean> TABLE_OPTIMIZER_REUSE_SUB_PLAN_ENABLED =
		key("table.optimizer.reuse-sub-plan-enabled")
			.defaultValue(true)
			.withDescription("When it is true, the optimizer will try to find out duplicated sub-plans and reuse them.");

	@Documentation.TableOption(execMode = Documentation.ExecMode.BATCH_STREAMING)
	public static final ConfigOption<Boolean> TABLE_OPTIMIZER_REUSE_SOURCE_ENABLED =
		key("table.optimizer.reuse-source-enabled")
			.defaultValue(true)
			.withDescription("When it is true, the optimizer will try to find out duplicated table sources and " +
				"reuse them. This works only when " + TABLE_OPTIMIZER_REUSE_SUB_PLAN_ENABLED.key() + " is true.");

	@Documentation.TableOption(execMode = Documentation.ExecMode.BATCH_STREAMING)
	public static final ConfigOption<Boolean> TABLE_OPTIMIZER_SOURCE_PREDICATE_PUSHDOWN_ENABLED =
		key("table.optimizer.source.predicate-pushdown-enabled")
			.defaultValue(true)
			.withDescription("When it is true, the optimizer will push down predicates into the FilterableTableSource. " +
				"Default value is true.");

	@Documentation.TableOption(execMode = Documentation.ExecMode.BATCH_STREAMING)
	public static final ConfigOption<Boolean> TABLE_OPTIMIZER_JOIN_REORDER_ENABLED =
		key("table.optimizer.join-reorder-enabled")
			.defaultValue(false)
			.withDescription("Enables join reorder in optimizer. Default is disabled.");
}
