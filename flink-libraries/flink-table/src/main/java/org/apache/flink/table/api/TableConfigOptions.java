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
 * This class holds configuration constants used by Flink's table module.
 */
public class TableConfigOptions {

	// ------------------------------------------------------------------------
	//  Optimizer Options
	// ------------------------------------------------------------------------

	public static final ConfigOption<Boolean> SQL_OPTIMIZER_JOIN_REORDER_ENABLED =
			key("sql.optimizer.join-reorder.enabled")
			.defaultValue(false)
			.withDescription("Enables join reorder in optimizer cbo. Default is disabled.");

	public static final ConfigOption<String> SQL_OPTIMIZER_AGG_PHASE_ENFORCER =
			key("sql.optimizer.agg.phase.enforcer")
			.defaultValue("NONE")
			.withDescription("Strategy for agg phase. Only NONE, TWO_PHASE or ONE_PHASE can be set.\n" +
				"NONE: No special enforcer for aggregate stage. Whether to choose two stage aggregate or one" +
				"   stage aggregate depends on cost. \n" +
				"TWO_PHASE: Enforce to use two stage aggregate which has localAggregate and globalAggregate. " +
					"NOTE: If aggregate call does not support split into two phase, still use one stage aggregate.\n" +
				"ONE_PHASE: Enforce to use one stage aggregate which only has CompleteGlobalAggregate.");

	public static final ConfigOption<Integer> SQL_OPTIMIZER_CNF_NODES_LIMIT =
			key("sql.optimizer.cnf.nodes.limit")
			.defaultValue(-1)
			.withDescription("When converting to conjunctive normal form (CNF), fail if the expression" +
				" exceeds this threshold; the threshold is expressed in terms of number of nodes " +
				"(only count RexCall node, including leaves and interior nodes). Negative number to" +
				" use the default threshold: double of number of nodes.");

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

	public static final ConfigOption<Long> SQL_OPTIMIZER_JOIN_NULL_FILTER_THRESHOLD =
			key("sql.optimizer.join.null.filter.threshold")
			.defaultValue(2000000L)
			.withDescription("If the source of InnerJoin has nullCount more than this value, " +
					"it will add a null filter (possibly be pushDowned) before the join, filter" +
					" null values to avoid the impact of null values on the single join node.");

	public static final ConfigOption<Double> SQL_OPTIMIZER_SEMI_JOIN_BUILD_DISTINCT_NDV_RATIO =
			key("sql.optimizer.semi-join.build-distinct.ndv-ratio")
			.defaultValue(0.8)
			.withDescription("When the semi-side of semiJoin can distinct a lot of data in advance," +
					" we will add distinct node before semiJoin.");

	public static final ConfigOption<Boolean> SQL_OPTIMIZER_SUBSECTION_UNIONALL_AS_BREAKPOINT_DISABLED =
			key("sql.optimizer.subsection.unionall-as-breakpoint.disabled")
			.defaultValue(false)
			.withDescription("Disable union all as breakpoint in subsection optimization");

	public static final ConfigOption<Boolean> SQL_OPTIMIZER_PLAN_DUMP_ENABLED =
			key("sql.optimizer.plan.dump.enabled")
			.defaultValue(false)
			.withDescription("If dump optimized plan is enabled, the optimized plan need to be dump.");

	public static final ConfigOption<String> SQL_OPTIMIZER_PLAN_DUMP_PATH =
			key("sql.optimizer.plan.dump.path")
			.noDefaultValue()
			.withDescription("Sets the file path to dump optimized plan.");

	public static final ConfigOption<Boolean> SQL_OPTIMIZER_REUSE_SUB_PLAN_ENABLED =
			key("sql.optimizer.reuse.sub-plan.enabled")
			.defaultValue(false)
			.withDescription("When true, the optimizer will try to find out duplicated " +
				"sub-plan and reuse them.");

	public static final ConfigOption<Boolean> SQL_OPTIMIZER_REUSE_TABLE_SOURCE_ENABLED =
			key("sql.optimizer.reuse.table-source.enabled")
			.defaultValue(false)
			.withDescription("When true, the optimizer will try to find out duplicated table-source and reuse them. " +
				"This works only when " + SQL_OPTIMIZER_REUSE_SUB_PLAN_ENABLED + " is true.");

	public static final ConfigOption<Boolean> SQL_OPTIMIZER_REUSE_NONDETERMINISTIC_OPERATOR_ENABLED =
			key("sql.optimizer.reuse.nondeterministic-operator.enabled")
			.defaultValue(false)
			.withDescription("When true, the optimizer will try to find out duplicated nondeterministic-operator and" +
				" reuse them. This works only when " + SQL_OPTIMIZER_REUSE_SUB_PLAN_ENABLED + " is true. \n" +
				"Nondeterministic-operator contains :\n" +
				"1. nondeterministic [[ScalarFunction]] (UDF, e.g. now)\n" +
				"2. nondeterministic [[AggregateFunction]](UDAF)\n" +
				"3. nondeterministic [[TableFunction]] (UDTF)");


	public static final ConfigOption<Boolean> SQL_OPTIMIZER_DATA_SKEW_DISTINCT_AGG =
			key("sql.optimizer.data-skew.distinct-agg")
			.defaultValue(false)
			.withDescription("Tell the optimizer whether there exists data skew in distinct aggregation\n"
				+ "so as to enable the aggregation split optimization.");

	public static final ConfigOption<Integer> SQL_OPTIMIZER_DATA_SKEW_DISTINCT_AGG_BUCKET =
			key("sql.optimizer.data-skew.distinct-agg.bucket")
			.defaultValue(1024)
			.withDescription("Configure the number of buckets when splitting distinct aggregation.");

	public static final ConfigOption<Boolean> SQL_OPTIMIZER_SMJ_REMOVE_SORT_ENABLE =
			key("sql.optimizer.smj.remove-sort.enable")
			.defaultValue(false)
			.withDescription("When true, the optimizer will try to remove redundant sort for SortMergeJoin. " +
					"However that will increase optimization time. Default value is false.");

	// ------------------------------------------------------------------------
	//  CodGen Options
	// ------------------------------------------------------------------------

	public static final ConfigOption<Integer> SQL_CODEGEN_LENGTH_MAX =
			key("sql.codegen.length.max")
			.defaultValue(48 * 1024)
			.withDescription("Generated code will be split if code length exceeds this limitation.");

	// ------------------------------------------------------------------------
	//  Sort Options
	// ------------------------------------------------------------------------

	public static final ConfigOption<Boolean> SQL_EXEC_SORT_RANGE_ENABLED =
			key("sql.exec.sort.range.enabled")
			.defaultValue(false)
			.withDescription("Sets whether to enable range sort, use range sort to sort all data in several partitions." +
				"When it is false, sorting in only one partition");

	public static final ConfigOption<Integer> SQL_EXEC_SORT_DEFAULT_LIMIT =
			key("sql.exec.sort.default.limit")
			.defaultValue(200)
			.withDescription("Default limit when user don't set a limit after order by. " +
				"This default value will be invalidated if " + SQL_EXEC_SORT_RANGE_ENABLED + " is set to be true.");

	public static final ConfigOption<Integer> SQL_EXEC_SORT_FILE_HANDLES_MAX_NUM =
			key("sql.exec.sort.file-handles.max.num")
			.defaultValue(128)
			.withDescription("Sort merge's maximum number of roads, too many roads, may cause too many files to be" +
				" read at the same time, resulting in excessive use of memory.");

	public static final ConfigOption<Boolean> SQL_EXEC_SORT_ASYNC_MERGE_ENABLED =
			key("sql.exec.sort.async-merge.enabled")
			.defaultValue(true)
			.withDescription("Whether to asynchronously merge sort spill files.");

	public static final ConfigOption<Boolean> SQL_EXEC_SORT_NON_TEMPORAL_ENABLED =
			key("sql.exec.sort.non-temporal.enabled")
			.defaultValue(false)
			.withDescription("Switch on/off stream sort without temporal or limit." +
				"Set whether to enable universal sort for stream. When it is false, " +
				"universal sort can't use for stream, default false. Just for testing.");

	// ------------------------------------------------------------------------
	//  Spill Options
	// ------------------------------------------------------------------------

	public static final ConfigOption<Boolean> SQL_EXEC_SPILL_COMPRESSION_ENABLED =
			key("sql.exec.spill.compression.enabled")
			.defaultValue(true)
			.withDescription("Whether to compress spilled data. (Now include sort and hash agg and hash join)");

	public static final ConfigOption<String> SQL_EXEC_SPILL_COMPRESSION_CODEC =
			key("sql.exec.spill.compression.codec")
			.defaultValue("lz4")
			.withDescription("Use that compression codec to compress spilled file. Now we support lz4, gzip, bzip2.");

	public static final ConfigOption<Integer> SQL_EXEC_SPILL_COMPRESSION_BLOCK_SIZE =
			key("sql.exec.spill.compression.block-size")
			.defaultValue(64 * 1024)
			.withDescription("The buffer is to compress. The larger the buffer," +
					" the better the compression ratio, but the more memory consumption.");

	// ------------------------------------------------------------------------
	//  Join Options
	// ------------------------------------------------------------------------

	public static final ConfigOption<Long> SQL_EXEC_HASH_JOIN_BROADCAST_THRESHOLD =
			key("sql.exec.hash-join.broadcast.threshold")
			.defaultValue(1024 * 1024L)
			.withDescription("Maximum size in bytes for data that could be broadcast to each parallel instance " +
				"that holds a partition of all data when performing a hash join. " +
				"Broadcast will be disabled if the value is -1.");

	// ------------------------------------------------------------------------
	//  Agg Options
	// ------------------------------------------------------------------------

	public static final ConfigOption<Integer> SQL_EXEC_WINDOW_AGG_BUFFER_SIZE_LIMIT =
			key("sql.exec.window-agg.buffer-size.limit")
			.defaultValue(100 * 1000)
			.withDescription("Sets the window elements buffer limit in size used in group window agg operator.");

	public static final ConfigOption<Boolean> SQL_EXEC_INCREMENTAL_AGG_ENABLED =
			key("sql.exec.incremental-agg.enabled")
			.defaultValue(true)
			.withDescription("Whether to enable incremental aggregate.");

	// ------------------------------------------------------------------------
	//  topN Options
	// ------------------------------------------------------------------------

	public static final ConfigOption<Long> SQL_EXEC_TOPN_CACHE_SIZE =
			key("sql.exec.topn.cache.size")
			.defaultValue(10000L)
			.withDescription("Cache size of every topn task.");

	// ------------------------------------------------------------------------
	//  Runtime Filter Options
	// ------------------------------------------------------------------------

	public static final ConfigOption<Boolean> SQL_EXEC_RUNTIME_FILTER_ENABLED =
			key("sql.exec.runtime-filter.enabled")
			.defaultValue(false)
			.withDescription("Runtime filter for hash join. The Build side of HashJoin will " +
					"build a bloomFilter in advance to filter the data on the probe side.");

	public static final ConfigOption<Boolean> SQL_EXEC_RUNTIME_FILTER_WAIT =
			key("sql.exec.runtime-filter.wait")
			.defaultValue(false)
			.withDescription("Weather to let probe side to wait bloom filter.");

	public static final ConfigOption<Integer> SQL_EXEC_RUNTIME_FILTER_SIZE_MAX =
			key("sql.exec.runtime-filter.size.max.mb")
			.defaultValue(10)
			.withDescription("The max size of MB to BloomFilter. A too large BloomFilter will cause " +
				"the JobMaster bandwidth to fill up and affect scheduling.");

	public static final ConfigOption<Double> SQL_EXEC_RUNTIME_FILTER_PROBE_FILTER_DEGREE_MIN =
			key("sql.exec.runtime-filter.probe.filter-degree.min")
			.defaultValue(0.5)
			.withDescription("The minimum filtering degree of the probe side to enable runtime filter." +
				"(1 - buildNdv / probeNdv) * (1 - minFpp) >= minProbeFilter.");

	public static final ConfigOption<Long> SQL_EXEC_RUNTIME_FILTER_PROBE_ROW_COUNT_MIN =
			key("sql.exec.runtime-filter.probe.row-count.min")
			.defaultValue(100000000L)
			.withDescription("The minimum row count of probe side to enable runtime filter." +
				"Probe.rowCount >= minProbeRowCount.");

	public static final ConfigOption<Double> SQL_EXEC_RUNTIME_FILTER_BUILD_PROBE_ROW_COUNT_RATIO_MAX =
			key("sql.exec.runtime-filter.build-probe.row-count-ratio.max")
			.defaultValue(0.5)
			.withDescription("The rowCount of the build side and the rowCount of the probe should " +
				"have a certain ratio before using the RuntimeFilter. " +
				"Builder.rowCount / probe.rowCount <= maxRowCountRatio.");

	public static final ConfigOption<Integer> SQL_EXEC_RUNTIME_FILTER_ROW_COUNT_NUM_BITS_RATIO =
			key("sql.exec.runtime-filter.row-count.num-bits.ratio")
			.defaultValue(40)
			.withDescription("A ratio between the probe row count and the BloomFilter size. If the " +
				"probe row count is too small, we should not use too large BloomFilter. maxBfBits = " +
				"Math.min(probeRowCount / ratioOfRowAndBits, " + SQL_EXEC_RUNTIME_FILTER_SIZE_MAX + ")");

	public static final ConfigOption<Double> SQL_EXEC_RUNTIME_FILTER_BUILDER_PUSH_DOWN_RATIO_MAX =
			key("sql.exec.runtime-filter.builder.push-down.ratio.max")
			.defaultValue(1.2)
			.withDescription("If the join key is the same, we can push down the BloomFilter" +
				" builder to the input node build, but we need to ensure that the NDV " +
				"and row count doesn't change much. " +
				"PushDownNdv / ndv <= maxRatio && pushDownRowCount / rowCount <= maxRatio.");

	// ------------------------------------------------------------------------
	//  MiniBatch Options
	// ------------------------------------------------------------------------

	public static final ConfigOption<Long> SQL_EXEC_MINIBATCH_ALLOW_LATENCY =
			key("sql.exec.mini-batch.allow-latency.ms")
			.defaultValue(Long.MIN_VALUE)
			.withDescription("MiniBatch allow latency(ms). Value > 0 means MiniBatch enabled.");

	public static final ConfigOption<Long> SQL_EXEC_MINIBATCH_SIZE =
			key("sql.exec.mini-batch.size")
			.defaultValue(Long.MIN_VALUE)
			.withDescription("The maximum number of inputs that MiniBatch buffer can accommodate.");

	public static final ConfigOption<Boolean> SQL_EXEC_MINIBATCH_JOIN_ENABLED =
			key("sql.exec.mini-batch.join.enabled")
			.defaultValue(true)
			.withDescription("Whether to enable miniBatch join.");

	public static final ConfigOption<Boolean> SQL_EXEC_MINI_BATCH_FLUSH_BEFORE_SNAPSHOT =
			key("sql.exec.mini-batch.flush-before-snapshot")
			.defaultValue(true)
			.withDescription("Whether to enable flushing buffered data before snapshot.");

	// ------------------------------------------------------------------------
	//  Source Options
	// ------------------------------------------------------------------------

	public static final ConfigOption<Boolean> SQL_EXEC_SOURCE_VALUES_INPUT_ENABLED =
			key("sql.exec.source.values-input.enabled")
			.defaultValue(false)
			.withDescription("Whether support values source input. The reason for disabling this " +
				"feature is that checkpoint will not work properly when source finished.");

	// ------------------------------------------------------------------------
	//  State Options
	// ------------------------------------------------------------------------

	public static final ConfigOption<Long> SQL_EXEC_STATE_TTL_MS =
			key("sql.exec.state.ttl.ms")
			.defaultValue(Long.MIN_VALUE)
			.withDescription("The minimum time until state that was not updated will be retained. State" +
				" might be cleared and removed if it was not updated for the defined period of time.");

	public static final ConfigOption<Long> SQL_EXEC_STATE_TTL_MAX_MS =
			key("sql.exec.state.ttl.max.ms")
			.defaultValue(Long.MIN_VALUE)
			.withDescription("The maximum time until state which was not updated will be retained." +
				"State will be cleared and removed if it was not updated for the defined " +
				"period of time.");

	// ------------------------------------------------------------------------
	//  Other Exec Options
	// ------------------------------------------------------------------------

	public static final ConfigOption<String> SQL_EXEC_DISABLED_OPERATORS =
			key("sql.exec.disabled-operators")
			.defaultValue("")
			.withDescription("Mainly for testing. A comma-separated list of name of the OperatorType, each name " +
				"means a kind of disabled operator. Its default value is empty that means no operators are disabled. " +
				"If the configure's value is \"NestedLoopJoin, ShuffleHashJoin\", NestedLoopJoin and ShuffleHashJoin " +
				"are disabled. If the configure's value is \"HashJoin\", " +
				"ShuffleHashJoin and BroadcastHashJoin are disabled.");

	public static final ConfigOption<Boolean> SQL_EXEC_DATA_EXCHANGE_MODE_ALL_BATCH =
			key("sql.exec.data-exchange-mode.all-batch")
			.defaultValue(true)
			.withDescription("Sets Whether all data-exchange-mode is batch.");

	public static final ConfigOption<Boolean> SQL_EXEC_OPERATOR_METRIC_DUMP_ENABLED =
			key("sql.exec.operator-metric.dump.enabled")
			.defaultValue(false)
			.withDescription("If dump operator metric is enabled, the operator metrics need to " +
				"be dump during runtime phase.");

	public static final ConfigOption<String> SQL_EXEC_OPERATOR_METRIC_DUMP_PATH =
			key("sql.exec.operator-metric.dump.path")
			.noDefaultValue()
			.withDescription("Sets the file path to dump stream graph plan with operator metrics." +
				"Default is null.");

	// ------------------------------------------------------------------------
	//  resource Options
	// ------------------------------------------------------------------------

	public static final ConfigOption<String> SQL_RESOURCE_INFER_MODE =
			key("sql.resource.infer.mode")
			.defaultValue("NONE")
			.withDescription("Sets infer resource mode according to statics. Only NONE, ONLY_SOURCE or ALL can be set.\n" +
				"If set NONE, parallelism and memory of all node are set by config.\n" +
				"If set ONLY_SOURCE, only source parallelism is inferred according to statics.\n" +
				"If set ALL, parallelism and memory of all node are inferred according to statics.\n");

	public static final ConfigOption<Integer> SQL_RESOURCE_HASH_AGG_TABLE_MEM =
			key("sql.resource.hash-agg.table.memory.mb")
			.defaultValue(32)
			.withDescription("Sets the table reserved memory size of hashAgg operator. It defines the lower limit.");

	public static final ConfigOption<Integer> SQL_RESOURCE_HASH_JOIN_TABLE_MEM =
			key("sql.resource.hash-join.table.memory.mb")
			.defaultValue(32)
			.withDescription("Sets the HashTable reserved memory for hashJoin operator. It defines the lower limit for.");

	public static final ConfigOption<Integer> SQL_RESOURCE_SORT_BUFFER_MEM =
			key("sql.resource.sort.buffer.memory.mb")
			.defaultValue(32)
			.withDescription("Sets the buffer reserved memory size for sort. It defines the lower limit for the sort.");

	public static final ConfigOption<Integer> SQL_RESOURCE_EXTERNAL_BUFFER_MEM =
			key("sql.resource.external-buffer.memory.mb")
			.defaultValue(10)
			.withDescription("Sets the externalBuffer memory size that is used in sortMergeJoin and overWindow.");

	// ------------------------------------------------------------------------
	//  prefer and max memory resource Options
	// ------------------------------------------------------------------------

	public static final ConfigOption<Integer> SQL_RESOURCE_SORT_BUFFER_PREFER_MEM =
			key("sql.resource.sort.buffer-prefer-memory-mb")
			.defaultValue(128)
			.withDescription("Sets the preferred buffer memory size for sort. " +
				"It defines the applied memory for the sort.");

	public static final ConfigOption<Integer> SQL_RESOURCE_SORT_BUFFER_MAX_MEM =
			key("sql.resource.sort.buffer-max-memory-mb")
			.defaultValue(512)
			.withDescription("Sets the max buffer memory size for sort. It defines the upper memory for the sort.");

	public static final ConfigOption<Integer> SQL_RESOURCE_HASH_JOIN_TABLE_PREFER_MEM =
			key("sql.resource.hash-join.table-prefer-memory-mb")
			.defaultValue(128)
			.withDescription("Sets the HashTable preferred memory for hashJoin operator. It defines the upper limit.");

	public static final ConfigOption<Integer> SQL_RESOURCE_HASH_JOIN_TABLE_MAX_MEM =
			key("sql.resource.hash-join.table-max-memory-mb")
			.defaultValue(512)
			.withDescription("Sets the HashTable max memory for hashJoin operator. It defines the upper limit.");

	public static final ConfigOption<Integer> SQL_RESOURCE_HASH_AGG_TABLE_PREFER_MEM =
			key("sql.resource.hash-agg.table-prefer-memory-mb")
			.defaultValue(128)
			.withDescription("Sets the table preferred memory size of hashAgg operator. It defines the upper limit.");

	public static final ConfigOption<Integer> SQL_RESOURCE_HASH_AGG_TABLE_MAX_MEM =
			key("sql.resource.hash-agg.table-max-memory-mb")
			.defaultValue(512)
			.withDescription("Sets the table max memory size of hashAgg operator. It defines the upper limit.");

	public static final ConfigOption<Integer> SQL_RESOURCE_SINK_DEFAULT_MEM =
			key("sql.resource.sink.default.memory.mb")
			.defaultValue(16)
			.withDescription("Sets the heap memory size of sink operator.");

	public static final ConfigOption<Integer> SQL_RESOURCE_SINK_DIRECT_MEM =
			key("sql.resource.sink.default.direct.memory.mb")
			.defaultValue(0)
			.withDescription("Sets the default direct memory size of sink operator.");

	public static final ConfigOption<Integer> SQL_RESOURCE_SINK_PARALLELISM =
			key("sql.resource.sink.parallelism")
			.defaultValue(-1)
			.withDescription("Sets sink parallelism if it is set. If it is not set, " +
				"sink nodes will chain with ahead nodes as far as possible.");

	public static final ConfigOption<Integer> SQL_RESOURCE_SOURCE_DEFAULT_MEM =
			key("sql.resource.source.default.memory.mb")
			.defaultValue(16)
			.withDescription("Sets the heap memory size of source operator.");

	public static final ConfigOption<Integer> SQL_RESOURCE_SOURCE_DIRECT_MEM =
			key("sql.resource.source.default.direct.memory.mb")
			.defaultValue(0)
			.withDescription("Sets the default direct memory size of source operator.");

	public static final ConfigOption<Integer> SQL_RESOURCE_DEFAULT_PARALLELISM =
			key("sql.resource.default.parallelism")
			.defaultValue(-1)
			.withDescription("Default parallelism of the job. If any node do not have special parallelism, use it." +
				"Its default value is the num of cpu cores in the client host.");

	public static final ConfigOption<Integer> SQL_RESOURCE_SOURCE_PARALLELISM =
			key("sql.resource.source.parallelism")
			.defaultValue(-1)
			.withDescription("Sets source parallelism if " + SQL_RESOURCE_INFER_MODE + " is NONE." +
				"If it is not set, use " + SQL_RESOURCE_DEFAULT_PARALLELISM + " to set source parallelism.");

	public static final ConfigOption<Double> SQL_RESOURCE_DEFAULT_CPU =
			key("sql.resource.default.cpu")
			.defaultValue(0.3)
			.withDescription("Default cpu for each operator.");

	public static final ConfigOption<Integer> SQL_RESOURCE_DEFAULT_MEM =
			key("sql.resource.default.memory.mb")
			.defaultValue(16)
			.withDescription("Default heap memory size for each operator.");

	public static final ConfigOption<Integer> SQL_RESOURCE_DEFAULT_DIRECT_MEM =
			key("sql.resource.default.direct.memory.mb")
			.defaultValue(0)
			.withDescription("Default direct memory size for each operator.");

	public static final ConfigOption<Long> SQL_RESOURCE_INFER_ROWS_PER_PARTITION =
			key("sql.resource.infer.rows-per-partition")
			.defaultValue(1000000L)
			.withDescription("Sets how many rows one partition processes. We will infer parallelism according " +
				"to input row count.");

	public static final ConfigOption<Integer> SQL_RESOURCE_INFER_SOURCE_PARALLELISM_MAX =
			key("sql.resource.infer.source.parallelism.max")
			.defaultValue(1000)
			.withDescription("Sets max parallelism for source operator.");

	public static final ConfigOption<Integer> SQL_RESOURCE_INFER_SOURCE_MB_PER_PARTITION =
			key("sql.resource.infer.source.mb-per-partition")
			.defaultValue(100)
			.withDescription("Sets how many data size in MB one partition processes. We will infer the source parallelism" +
				" according to source data size.");

	public static final ConfigOption<Integer> SQL_RESOURCE_INFER_OPERATOR_PARALLELISM_MAX =
			key("sql.resource.infer.operator.parallelism.max")
			.defaultValue(800)
			.withDescription("Sets max parallelism for all operators.");

	public static final ConfigOption<Integer> SQL_RESOURCE_INFER_OPERATOR_MEM_MAX =
			key("sql.resource.infer.operator.memory.max.mb")
			.defaultValue(1024)
			.withDescription("Maybe inferred operator mem is too large, so this setting is upper limit for the" +
				" inferred operator mem.");

	public static final ConfigOption<Double> SQL_RESOURCE_RUNNING_UNIT_CPU_TOTAL =
			key("sql.resource.running-unit.cpu.total")
			.defaultValue(0d)
			.withDescription("total cpu limit of a runningUnit. 0 means no limit.");

	// ------------------------------------------------------------------------
	//  schedule Options
	// ------------------------------------------------------------------------
	public static final ConfigOption<Boolean> SQL_SCHEDULE_RUNNING_UNIT_ENABLED =
			key("sql.schedule.running-unit.enabled")
			.defaultValue(true)
			.withDescription("Whether to schedule according to runningUnits.");

	// ------------------------------------------------------------------------
	//  Parquet Options
	// ------------------------------------------------------------------------

	public static final ConfigOption<Integer> SQL_PARQUET_SINK_BLOCK_SIZE =
			key("sql.parquet.sink.block.size")
			.defaultValue(128 * 1024 * 1024)
			.withDescription("Parquet block size in bytes, this value would be set as " +
				"parquet.block.size and dfs.blocksize to improve alignment between " +
				"row group and hdfs block.");

	public static final ConfigOption<Boolean> SQL_PARQUET_SINK_DICTIONARY_ENABLED =
			key("sql.parquet.sink.dictionary.enabled")
			.defaultValue(true)
			.withDescription("Enable Parquet dictionary encoding.");

	public static final ConfigOption<Boolean> SQL_PARQUET_PREDICATE_PUSHDOWN_ENABLED =
			key("sql.parquet.predicate-pushdown.enabled")
			.defaultValue(true)
			.withDescription("Allow trying to push filter down to a parquet [[TableSource]]. the default value is true, " +
				"means allow the attempt.");

	// ------------------------------------------------------------------------
	//  Orc Options
	// ------------------------------------------------------------------------

	public static final ConfigOption<Boolean> SQL_ORC_PREDICATE_PUSHDOWN_ENABLED =
			key("sql.orc.predicate-pushdown.enabled")
			.defaultValue(true)
			.withDescription("Allow trying to push filter down to a orc [[TableSource]]. The default value is true, " +
				"means allow the attempt.");

}
