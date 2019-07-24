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
	public static final ConfigOption<String> SQL_EXEC_SOURCE_IDLE_TIMEOUT =
			key("sql.exec.source.idle.timeout")
					.defaultValue("-1 ms")
					.withDescription("When a source do not receive any elements for the timeout time, " +
							"it will be marked as temporarily idle. This allows downstream " +
							"tasks to advance their watermarks without the need to wait for " +
							"watermarks from this source while it is idle.");

	// ------------------------------------------------------------------------
	//  Sort Options
	// ------------------------------------------------------------------------
	public static final ConfigOption<Integer> SQL_EXEC_SORT_DEFAULT_LIMIT =
			key("sql.exec.sort.default.limit")
					.defaultValue(200)
					.withDescription("Default limit when user don't set a limit after order by. ");

	public static final ConfigOption<Integer> SQL_EXEC_SORT_FILE_HANDLES_MAX_NUM =
			key("sql.exec.sort.max-num-file-handles")
					.defaultValue(128)
					.withDescription("The maximal fan-in for external merge sort. It limits the number of file handles per operator. " +
							"If it is too small, may cause intermediate merging. But if it is too large, " +
							"it will cause too many files opened at the same time, consume memory and lead to random reading.");

	public static final ConfigOption<Boolean> SQL_EXEC_SORT_ASYNC_MERGE_ENABLED =
			key("sql.exec.sort.async-merge.enabled")
					.defaultValue(true)
					.withDescription("Whether to asynchronously merge sorted spill files.");

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
							"Now we only support lz4.");

	public static final ConfigOption<Integer> SQL_EXEC_SPILL_COMPRESSION_BLOCK_SIZE =
			key("sql.exec.spill.compression.block-size")
					.defaultValue(64 * 1024)
					.withDescription("The buffer is to compress. The larger the buffer," +
							" the better the compression ratio, but the more memory consumption.");

	// ------------------------------------------------------------------------
	//  Resource Options
	// ------------------------------------------------------------------------

	public static final ConfigOption<Integer> SQL_RESOURCE_DEFAULT_PARALLELISM =
			key("sql.resource.default.parallelism")
					.defaultValue(-1)
					.withDescription("Default parallelism of job operators. If it is <= 0, use parallelism of StreamExecutionEnvironment(" +
							"its default value is the num of cpu cores in the client host).");

	public static final ConfigOption<Integer> SQL_RESOURCE_SOURCE_PARALLELISM =
			key("sql.resource.source.parallelism")
					.defaultValue(-1)
					.withDescription("Sets source parallelism, if it is <= 0, use " + SQL_RESOURCE_DEFAULT_PARALLELISM.key() + " to set source parallelism.");

	public static final ConfigOption<Integer> SQL_RESOURCE_SINK_PARALLELISM =
			key("sql.resource.sink.parallelism")
					.defaultValue(-1)
					.withDescription("Sets sink parallelism, if it is <= 0, use " + SQL_RESOURCE_DEFAULT_PARALLELISM.key() + " to set sink parallelism.");

	public static final ConfigOption<Integer> SQL_RESOURCE_EXTERNAL_BUFFER_MEM =
			key("sql.resource.external-buffer.memory.mb")
					.defaultValue(10)
					.withDescription("Sets the externalBuffer memory size that is used in sortMergeJoin and overWindow.");

	public static final ConfigOption<Integer> SQL_RESOURCE_HASH_AGG_TABLE_MEM =
			key("sql.resource.hash-agg.table.memory.mb")
					.defaultValue(128)
					.withDescription("Sets the table memory size of hashAgg operator.");

	public static final ConfigOption<Integer> SQL_RESOURCE_HASH_JOIN_TABLE_MEM =
			key("sql.resource.hash-join.table.memory.mb")
					.defaultValue(128)
					.withDescription("Sets the HashTable reserved memory for hashJoin operator. It defines the lower limit.");

	public static final ConfigOption<Integer> SQL_RESOURCE_SORT_BUFFER_MEM =
			key("sql.resource.sort.buffer.memory.mb")
					.defaultValue(128)
					.withDescription("Sets the buffer memory size for sort.");

	// ------------------------------------------------------------------------
	//  Agg Options
	// ------------------------------------------------------------------------

	/**
	 * See {@code org.apache.flink.table.runtime.operators.window.grouping.HeapWindowsGrouping}.
	 */
	public static final ConfigOption<Integer> SQL_EXEC_WINDOW_AGG_BUFFER_SIZE_LIMIT =
			key("sql.exec.window-agg.buffer-size-limit")
					.defaultValue(100 * 1000)
					.withDescription("Sets the window elements buffer size limit used in group window agg operator.");

	// ------------------------------------------------------------------------
	//  Async Lookup Options
	// ------------------------------------------------------------------------
	public static final ConfigOption<Integer> SQL_EXEC_LOOKUP_ASYNC_BUFFER_CAPACITY =
			key("sql.exec.lookup.async.buffer-capacity")
					.defaultValue(100)
					.withDescription("The max number of async i/o operation that the async lookup join can trigger.");

	public static final ConfigOption<String> SQL_EXEC_LOOKUP_ASYNC_TIMEOUT =
			key("sql.exec.lookup.async.timeout")
					.defaultValue("3 min")
					.withDescription("The async timeout for the asynchronous operation to complete.");

	// ------------------------------------------------------------------------
	//  MiniBatch Options
	// ------------------------------------------------------------------------

	public static final ConfigOption<Boolean> SQL_EXEC_MINIBATCH_ENABLED =
			key("sql.exec.mini-batch.enabled")
					.defaultValue(false)
					.withDescription("Specifies whether to enable MiniBatch optimization. " +
							"MiniBatch is an optimization to buffer input records to reduce state access. " +
							"This is disabled by default. To enable this, users should set this config to true.");

	public static final ConfigOption<String> SQL_EXEC_MINIBATCH_ALLOW_LATENCY =
			key("sql.exec.mini-batch.allow-latency")
					.defaultValue("-1 ms")
					.withDescription("The maximum latency can be used for MiniBatch to buffer input records. " +
							"MiniBatch is an optimization to buffer input records to reduce state access. " +
							"MiniBatch is triggered with the allowed latency interval and when the maximum number of buffered records reached. " +
							"NOTE: If " + SQL_EXEC_MINIBATCH_ENABLED.key() + " is set true, its value must be greater than zero.");

	public static final ConfigOption<Long> SQL_EXEC_MINIBATCH_SIZE =
			key("sql.exec.mini-batch.size")
					.defaultValue(-1L)
					.withDescription("The maximum number of input records can be buffered for MiniBatch. " +
							"MiniBatch is an optimization to buffer input records to reduce state access. " +
							"MiniBatch is triggered with the allowed latency interval and when the maximum number of buffered records reached. " +
							"NOTE: MiniBatch only works for non-windowed aggregations currently. If " + SQL_EXEC_MINIBATCH_ENABLED.key() +
							" is set true, its value must be positive.");

	// ------------------------------------------------------------------------
	//  State Options
	// ------------------------------------------------------------------------
	public static final ConfigOption<String> SQL_EXEC_STATE_TTL =
			key("sql.exec.state.ttl")
					.defaultValue("-1 ms")
					.withDescription("Specifies a minimum time interval for how long idle state " +
							"(i.e. state which was not updated), will be retained. State will never be " +
							"cleared until it was idle for less than the minimum time, and will be cleared " +
							"at some time after it was idle. Default is never clean-up the state.\n" +
							"NOTE: Cleaning up state requires additional overhead for bookkeeping.");

	// ------------------------------------------------------------------------
	//  Other Exec Options
	// ------------------------------------------------------------------------
	public static final ConfigOption<String> SQL_EXEC_DISABLED_OPERATORS =
			key("sql.exec.disabled-operators")
					.defaultValue("")
					.withDescription("Mainly for testing. A comma-separated list of name of the OperatorType, each name " +
							"means a kind of disabled operator. Its default value is empty that means no operators are disabled. " +
							"If the configure's value is \"NestedLoopJoin, ShuffleHashJoin\", NestedLoopJoin and ShuffleHashJoin " +
							"are disabled. If configure's value is \"HashJoin\", ShuffleHashJoin and BroadcastHashJoin are disabled.");

	public static final ConfigOption<String> SQL_EXEC_SHUFFLE_MODE =
			key("sql.exec.shuffle-mode")
					.defaultValue("batch")
					.withDescription("Sets exec shuffle mode. Only batch or pipeline can be set.\n" +
							"batch: the job will run stage by stage. \n" +
							"pipeline: the job will run in streaming mode, but it may cause resource deadlock that receiver waits for resource to start when " +
							"the sender holds resource to wait to send data to the receiver.");
}
