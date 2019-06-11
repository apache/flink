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

package org.apache.flink.table.api.config;

import org.apache.flink.configuration.ConfigOption;

import static org.apache.flink.configuration.ConfigOptions.key;

/**
 * This class holds configuration constants used by Flink's table module.
 *
 * <p>This is only used for the Blink planner.
 */
public class ExecutionConfigOptions {

	// ------------------------------------------------------------------------
	//  Source Options
	// ------------------------------------------------------------------------
	public static final ConfigOption<String> TABLE_EXEC_SOURCE_IDLE_TIMEOUT =
		key("table.exec.source.idle-timeout")
			.defaultValue("-1 ms")
			.withDescription("When a source do not receive any elements for the timeout time, " +
				"it will be marked as temporarily idle. This allows downstream " +
				"tasks to advance their watermarks without the need to wait for " +
				"watermarks from this source while it is idle.");

	// ------------------------------------------------------------------------
	//  Sort Options
	// ------------------------------------------------------------------------
	public static final ConfigOption<Integer> TABLE_EXEC_SORT_DEFAULT_LIMIT =
		key("table.exec.sort.default-limit")
			.defaultValue(200)
			.withDescription("Default limit when user don't set a limit after order by.");

	public static final ConfigOption<Integer> TABLE_EXEC_SORT_MAX_NUM_FILE_HANDLES =
		key("table.exec.sort.max-num-file-handles")
			.defaultValue(128)
			.withDescription("The maximal fan-in for external merge sort. It limits the number of file handles per operator. " +
				"If it is too small, may cause intermediate merging. But if it is too large, " +
				"it will cause too many files opened at the same time, consume memory and lead to random reading.");

	public static final ConfigOption<Boolean> TABLE_EXEC_SORT_ASYNC_MERGE_ENABLED =
		key("table.exec.sort.async-merge-enabled")
			.defaultValue(true)
			.withDescription("Whether to asynchronously merge sorted spill files.");

	// ------------------------------------------------------------------------
	//  Spill Options
	// ------------------------------------------------------------------------
	public static final ConfigOption<Boolean> TABLE_EXEC_SPILL_COMPRESSION_ENABLED =
		key("table.exec.spill-compression.enabled")
			.defaultValue(true)
			.withDescription("Whether to compress spilled data. " +
				"Currently we only support compress spilled data for sort and hash-agg and hash-join operators.");

	public static final ConfigOption<String> TABLE_EXEC_SPILL_COMPRESSION_BLOCK_SIZE =
		key("table.exec.spill-compression.block-size")
			.defaultValue("64 kb")
			.withDescription("The memory size used to do compress when spilling data. " +
				"The larger the memory, the higher the compression ratio, " +
				"but more memory resource will be consumed by the job.");

	// ------------------------------------------------------------------------
	//  Resource Options
	// ------------------------------------------------------------------------

	public static final ConfigOption<Integer> TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM =
		key("table.exec.resource.default-parallelism")
			.defaultValue(-1)
			.withDescription("Default parallelism of job operators. If it is <= 0, use parallelism of StreamExecutionEnvironment(" +
				"its default value is the num of cpu cores in the client host).");

	public static final ConfigOption<Integer> TABLE_EXEC_RESOURCE_SOURCE_PARALLELISM =
		key("table.exec.resource.source.parallelism")
			.defaultValue(-1)
			.withDescription("Sets source parallelism, if it is <= 0, use " + TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM.key() + " to set source parallelism.");

	public static final ConfigOption<Integer> TABLE_EXEC_RESOURCE_SINK_PARALLELISM =
		key("table.exec.resource.sink.parallelism")
			.defaultValue(-1)
			.withDescription("Sets sink parallelism, if it is <= 0, use " + TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM.key() + " to set sink parallelism.");

	public static final ConfigOption<String> TABLE_EXEC_RESOURCE_EXTERNAL_BUFFER_MEMORY =
		key("table.exec.resource.external-buffer-memory")
			.defaultValue("10 mb")
			.withDescription("Sets the external buffer memory size that is used in sort merge join and nested join and over window.");

	public static final ConfigOption<String> TABLE_EXEC_RESOURCE_HASH_AGG_MEMORY =
		key("table.exec.resource.hash-agg.memory")
			.defaultValue("128 mb")
			.withDescription("Sets the managed memory size of hash aggregate operator.");

	public static final ConfigOption<String> TABLE_EXEC_RESOURCE_HASH_JOIN_MEMORY =
		key("table.exec.resource.hash-join.memory")
			.defaultValue("128 mb")
			.withDescription("Sets the managed memory for hash join operator. It defines the lower limit.");

	public static final ConfigOption<String> TABLE_EXEC_RESOURCE_SORT_MEMORY =
		key("table.exec.resource.sort.memory")
			.defaultValue("128 mb")
			.withDescription("Sets the managed buffer memory size for sort operator.");

	// ------------------------------------------------------------------------
	//  Agg Options
	// ------------------------------------------------------------------------

	/**
	 * See {@code org.apache.flink.table.runtime.operators.window.grouping.HeapWindowsGrouping}.
	 */
	public static final ConfigOption<Integer> TABLE_EXEC_WINDOW_AGG_BUFFER_SIZE_LIMIT =
		key("table.exec.window-agg.buffer-size-limit")
			.defaultValue(100 * 1000)
			.withDescription("Sets the window elements buffer size limit used in group window agg operator.");

	// ------------------------------------------------------------------------
	//  Async Lookup Options
	// ------------------------------------------------------------------------
	public static final ConfigOption<Integer> TABLE_EXEC_ASYNC_LOOKUP_BUFFER_CAPACITY =
		key("table.exec.async-lookup.buffer-capacity")
			.defaultValue(100)
			.withDescription("The max number of async i/o operation that the async lookup join can trigger.");

	public static final ConfigOption<String> TABLE_EXEC_ASYNC_LOOKUP_TIMEOUT =
		key("table.exec.async-lookup.timeout")
			.defaultValue("3 min")
			.withDescription("The async timeout for the asynchronous operation to complete.");

	// ------------------------------------------------------------------------
	//  MiniBatch Options
	// ------------------------------------------------------------------------

	public static final ConfigOption<Boolean> TABLE_EXEC_MINIBATCH_ENABLED =
		key("table.exec.mini-batch.enabled")
			.defaultValue(false)
			.withDescription("Specifies whether to enable MiniBatch optimization. " +
				"MiniBatch is an optimization to buffer input records to reduce state access. " +
				"This is disabled by default. To enable this, users should set this config to true. " +
				"NOTE: If mini-batch is enabled, 'table.exec.mini-batch.allow-latency' and " +
				"'table.exec.mini-batch.size' must be set.");

	public static final ConfigOption<String> TABLE_EXEC_MINIBATCH_ALLOW_LATENCY =
		key("table.exec.mini-batch.allow-latency")
			.defaultValue("-1 ms")
			.withDescription("The maximum latency can be used for MiniBatch to buffer input records. " +
				"MiniBatch is an optimization to buffer input records to reduce state access. " +
				"MiniBatch is triggered with the allowed latency interval and when the maximum number of buffered records reached. " +
				"NOTE: If " + TABLE_EXEC_MINIBATCH_ENABLED.key() + " is set true, its value must be greater than zero.");

	public static final ConfigOption<Long> TABLE_EXEC_MINIBATCH_SIZE =
		key("table.exec.mini-batch.size")
			.defaultValue(-1L)
			.withDescription("The maximum number of input records can be buffered for MiniBatch. " +
				"MiniBatch is an optimization to buffer input records to reduce state access. " +
				"MiniBatch is triggered with the allowed latency interval and when the maximum number of buffered records reached. " +
				"NOTE: MiniBatch only works for non-windowed aggregations currently. If " + TABLE_EXEC_MINIBATCH_ENABLED.key() +
				" is set true, its value must be positive.");

	// ------------------------------------------------------------------------
	//  Other Exec Options
	// ------------------------------------------------------------------------
	public static final ConfigOption<String> TABLE_EXEC_DISABLED_OPERATORS =
		key("table.exec.disabled-operators")
			.noDefaultValue()
			.withDescription("Mainly for testing. A comma-separated list of operator names, each name " +
				"represents a kind of disabled operator.\n" +
				"Operators that can be disabled include \"NestedLoopJoin\", \"ShuffleHashJoin\", \"BroadcastHashJoin\", " +
				"\"SortMergeJoin\", \"HashAgg\", \"SortAgg\".\n" +
				"By default no operator is disabled.");

	public static final ConfigOption<String> TABLE_EXEC_SHUFFLE_MODE =
		key("table.exec.shuffle-mode")
			.defaultValue("batch")
			.withDescription("Sets exec shuffle mode. Only batch or pipeline can be set.\n" +
				"batch: the job will run stage by stage. \n" +
				"pipeline: the job will run in streaming mode, but it may cause resource deadlock that receiver waits for resource to start when " +
				"the sender holds resource to wait to send data to the receiver.");
}
