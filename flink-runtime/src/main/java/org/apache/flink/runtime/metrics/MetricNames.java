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

package org.apache.flink.runtime.metrics;

import org.apache.flink.runtime.io.network.metrics.NettyShuffleMetricFactory;
import org.apache.flink.runtime.metrics.scope.TaskManagerScopeFormat;

import java.util.StringJoiner;

/**
 * Collection of metric names.
 */
public class MetricNames {
	private MetricNames() {
	}

	public static final String SUFFIX_RATE = "PerSecond";

	public static final String IO_NUM_RECORDS_IN = "numRecordsIn";
	public static final String IO_NUM_RECORDS_OUT = "numRecordsOut";
	public static final String IO_NUM_RECORDS_IN_RATE = IO_NUM_RECORDS_IN + SUFFIX_RATE;
	public static final String IO_NUM_RECORDS_OUT_RATE = IO_NUM_RECORDS_OUT + SUFFIX_RATE;

	public static final String IO_NUM_BYTES_IN = "numBytesIn";
	public static final String IO_NUM_BYTES_OUT = "numBytesOut";
	public static final String IO_NUM_BYTES_IN_RATE = IO_NUM_BYTES_IN + SUFFIX_RATE;
	public static final String IO_NUM_BYTES_OUT_RATE = IO_NUM_BYTES_OUT + SUFFIX_RATE;

	public static final String IO_NUM_BUFFERS_IN = "numBuffersIn";
	public static final String IO_NUM_BUFFERS_OUT = "numBuffersOut";
	public static final String IO_NUM_BUFFERS_OUT_RATE = IO_NUM_BUFFERS_OUT + SUFFIX_RATE;

	public static final String IO_CURRENT_INPUT_WATERMARK = "currentInputWatermark";
	public static final String IO_CURRENT_INPUT_1_WATERMARK = "currentInput1Watermark";
	public static final String IO_CURRENT_INPUT_2_WATERMARK = "currentInput2Watermark";
	public static final String IO_CURRENT_OUTPUT_WATERMARK = "currentOutputWatermark";

	private static final String  SHUFFLE_NETTY_GROUP = new StringJoiner(TaskManagerScopeFormat.SCOPE_SEPARATOR)
		.add(NettyShuffleMetricFactory.METRIC_GROUP_SHUFFLE)
		.add(NettyShuffleMetricFactory.METRIC_GROUP_NETTY).toString();
	private static final String SHUFFLE_NETTY_INPUT_GROUP = new StringJoiner(TaskManagerScopeFormat.SCOPE_SEPARATOR,
		SHUFFLE_NETTY_GROUP, NettyShuffleMetricFactory.METRIC_GROUP_BUFFERS)
		.add(NettyShuffleMetricFactory.METRIC_GROUP_INPUT).toString();
	private static final String SHUFFLE_NETTY_OUPUT_GROUP = new StringJoiner(TaskManagerScopeFormat.SCOPE_SEPARATOR,
		SHUFFLE_NETTY_GROUP, NettyShuffleMetricFactory.METRIC_GROUP_BUFFERS)
		.add(NettyShuffleMetricFactory.METRIC_GROUP_OUTPUT).toString();
	public static final String USAGE_SHUFFLE_NETTY_INPUT_FLOATING_BUFFERS = new StringJoiner(
		TaskManagerScopeFormat.SCOPE_SEPARATOR,
		SHUFFLE_NETTY_INPUT_GROUP,
		NettyShuffleMetricFactory.METRIC_INPUT_FLOATING_BUFFERS_USAGE).toString();
	public static final String USAGE_SHUFFLE_NETTY_INPUT_EXCLUSIVE_BUFFERS = new StringJoiner(
		TaskManagerScopeFormat.SCOPE_SEPARATOR,
		SHUFFLE_NETTY_INPUT_GROUP,
		NettyShuffleMetricFactory.METRIC_INPUT_EXCLUSIVE_BUFFERS_USAGE).toString();
	public static final String USAGE_SHUFFLE_NETTY_OUTPUT_POOL_USAGE = new StringJoiner(
		TaskManagerScopeFormat.SCOPE_SEPARATOR,
		SHUFFLE_NETTY_OUPUT_GROUP,
		NettyShuffleMetricFactory.METRIC_OUTPUT_POOL_USAGE).toString();

	public static final String NUM_RUNNING_JOBS = "numRunningJobs";
	public static final String TASK_SLOTS_AVAILABLE = "taskSlotsAvailable";
	public static final String TASK_SLOTS_TOTAL = "taskSlotsTotal";
	public static final String NUM_REGISTERED_TASK_MANAGERS = "numRegisteredTaskManagers";

	public static final String NUM_RESTARTS = "numRestarts";

	@Deprecated
	public static final String FULL_RESTARTS = "fullRestarts";

	public static final String MEMORY_USED = "Used";
	public static final String MEMORY_COMMITTED = "Committed";
	public static final String MEMORY_MAX = "Max";

	public static final String IS_BACKPRESSURED = "isBackPressured";
}
