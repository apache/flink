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

	public static final ConfigOption<Integer> SQL_RESOURCE_DEFAULT_PARALLELISM =
			key("sql.resource.default.parallelism")
					.defaultValue(-1)
					.withDescription("Default parallelism of the job. If any node do not have special parallelism, use it." +
							"Its default value is the num of cpu cores in the client host.");

	// ------------------------------------------------------------------------
	//  MiniBatch Options
	// ------------------------------------------------------------------------

	public static final ConfigOption<Long> SQL_EXEC_MINIBATCH_ALLOW_LATENCY =
			key("sql.exec.mini-batch.allow-latency.ms")
					.defaultValue(Long.MIN_VALUE)
					.withDescription("MiniBatch allow latency(ms). Value > 0 means MiniBatch enabled.");

	// ------------------------------------------------------------------------
	//  STATE BACKEND Options
	// ------------------------------------------------------------------------

	public static final ConfigOption<Boolean> SQL_EXEC_STATE_BACKEND_ON_HEAP =
			key("sql.exec.statebackend.onheap")
					.defaultValue(false)
					.withDescription("Whether the statebackend is on heap.");

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
