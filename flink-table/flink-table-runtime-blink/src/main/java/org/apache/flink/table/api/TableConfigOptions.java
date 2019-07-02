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

package org.apache.flink.table.api;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.runtime.window.grouping.HeapWindowsGrouping;

import static org.apache.flink.configuration.ConfigOptions.key;

/**
 * This class holds configuration constants used by Flink's table module.
 */
public class TableConfigOptions {

	// ------------------------------------------------------------------------
	//  Source Options
	// ------------------------------------------------------------------------

	public static final ConfigOption<Boolean> SQL_EXEC_SOURCE_VALUES_INPUT_ENABLED =
			key("sql.exec.source.values-input.enabled")
					.defaultValue(false)
					.withDescription("Whether support values source input. The reason for disabling this " +
									"feature is that checkpoint will not work properly when source finished.");

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
					.withDescription("Sort merge's maximum number of roads, too many roads, " +
							"may cause too many files to be read at the same time, resulting in " +
							"excessive use of memory.");

	public static final ConfigOption<Boolean> SQL_EXEC_SORT_ASYNC_MERGE_ENABLED =
			key("sql.exec.sort.async-merge.enabled")
					.defaultValue(true)
					.withDescription("Whether to asynchronously merge sort spill files.");

	public static final ConfigOption<Integer> SQL_EXEC_PER_REQUEST_MEM =
			key("sql.exec.per-request.mem.mb")
					.defaultValue(32)
					.withDescription("Sets the number of per-requested buffers when the operator " +
							"allocates much more segments from the floating memory pool.");

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
					.withDescription("Whether to compress spilled data. " +
							"(Now include sort and hash agg and hash join)");

	public static final ConfigOption<String> SQL_EXEC_SPILL_COMPRESSION_CODEC =
			key("sql.exec.spill.compression.codec")
					.defaultValue("lz4")
					.withDescription("Use that compression codec to compress spilled file. " +
							"Now we support lz4.");

	public static final ConfigOption<Integer> SQL_EXEC_SPILL_COMPRESSION_BLOCK_SIZE =
			key("sql.exec.spill.compression.block-size")
					.defaultValue(64 * 1024)
					.withDescription("The buffer is to compress. The larger the buffer," +
							" the better the compression ratio, but the more memory consumption.");

	// ------------------------------------------------------------------------
	//  Resource Options
	// ------------------------------------------------------------------------

	public static final ConfigOption<String> SQL_RESOURCE_INFER_MODE =
			key("sql.resource.infer.mode")
					.defaultValue("NONE")
					.withDescription("Sets infer resource mode according to statics. Only NONE, or ONLY_SOURCE can be set.\n" +
							"If set NONE, parallelism and memory of all node are set by config.\n" +
							"If set ONLY_SOURCE, only source parallelism is inferred according to statics.\n");

	public static final ConfigOption<Long> SQL_RESOURCE_INFER_ROWS_PER_PARTITION =
			key("sql.resource.infer.rows-per-partition")
					.defaultValue(1000000L)
					.withDescription("Sets how many rows one partition processes. We will infer parallelism according " +
							"to input row count.");

	public static final ConfigOption<Integer> SQL_RESOURCE_INFER_SOURCE_PARALLELISM_MAX =
			key("sql.resource.infer.source.parallelism.max")
					.defaultValue(1000)
					.withDescription("Sets max infer parallelism for source operator.");

	public static final ConfigOption<Integer> SQL_RESOURCE_DEFAULT_PARALLELISM =
			key("sql.resource.default.parallelism")
					.defaultValue(-1)
					.withDescription("Default parallelism of the job. If any node do not have special parallelism, use it." +
							"Its default value is the num of cpu cores in the client host.");

	public static final ConfigOption<Integer> SQL_RESOURCE_SOURCE_PARALLELISM =
			key("sql.resource.source.parallelism")
					.defaultValue(-1)
					.withDescription("Sets source parallelism if " + SQL_RESOURCE_INFER_MODE + " is NONE, " +
							"use " + SQL_RESOURCE_DEFAULT_PARALLELISM + " to set source parallelism.");

	public static final ConfigOption<Integer> SQL_RESOURCE_SINK_PARALLELISM =
			key("sql.resource.sink.parallelism")
					.defaultValue(-1)
					.withDescription("Sets sink parallelism if it is set. If it is not set, " +
							"sink nodes will chain with ahead nodes as far as possible.");

	public static final ConfigOption<Integer> SQL_RESOURCE_EXTERNAL_BUFFER_MEM =
			key("sql.resource.external-buffer.memory.mb")
					.defaultValue(10)
					.withDescription("Sets the externalBuffer memory size that is used in sortMergeJoin and overWindow.");

	public static final ConfigOption<Integer> SQL_RESOURCE_HASH_AGG_TABLE_MEM =
			key("sql.resource.hash-agg.table.memory.mb")
					.defaultValue(32)
					.withDescription("Sets the table reserved memory size of hashAgg operator. It defines the lower limit.");

	public static final ConfigOption<Integer> SQL_RESOURCE_SORT_BUFFER_MEM =
			key("sql.resource.sort.buffer.memory.mb")
					.defaultValue(32)
					.withDescription("Sets the buffer reserved memory size for sort. It defines the lower limit for the sort.");

	// ------------------------------------------------------------------------
	//  Agg Options
	// ------------------------------------------------------------------------

	/**
	 * See {@link HeapWindowsGrouping}.
	 */
	public static final ConfigOption<Integer> SQL_EXEC_WINDOW_AGG_BUFFER_SIZE_LIMIT =
			key("sql.exec.window-agg.buffer-size.limit")
					.defaultValue(100 * 1000)
					.withDescription("Sets the window elements buffer limit in size used in group window agg operator.");

	// ------------------------------------------------------------------------
	//  topN Options
	// ------------------------------------------------------------------------

	public static final ConfigOption<Long> SQL_EXEC_TOPN_CACHE_SIZE =
			key("sql.exec.topn.cache.size")
					.defaultValue(10000L)
					.withDescription("Cache size of every topn task.");

	// ------------------------------------------------------------------------
	//  Async Lookup Options
	// ------------------------------------------------------------------------

	public static final ConfigOption<Integer> SQL_EXEC_LOOKUP_ASYNC_BUFFER_CAPACITY =
		key("sql.exec.lookup.async.buffer-capacity")
		.defaultValue(100)
		.withDescription("The max number of async i/o operation that the async lookup join can trigger.");

	public static final ConfigOption<Long> SQL_EXEC_LOOKUP_ASYNC_TIMEOUT_MS =
		key("sql.exec.lookup.async.timeout-ms")
		.defaultValue(180_000L)
		.withDescription("The async timeout millisecond for the asynchronous operation to complete.");

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

	// ------------------------------------------------------------------------
	//  STATE BACKEND Options
	// ------------------------------------------------------------------------

	public static final ConfigOption<Boolean> SQL_EXEC_STATE_BACKEND_ON_HEAP =
			key("sql.exec.statebackend.onheap")
					.defaultValue(false)
					.withDescription("Whether the statebackend is on heap.");

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
}
