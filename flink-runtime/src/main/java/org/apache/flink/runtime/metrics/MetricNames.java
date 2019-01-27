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

/**
 * Collection of metric names.
 */
public class MetricNames {
	private MetricNames() {
	}

	private static final String SUFFIX_RATE = "PerSecond";
	private static final String SUFFIX_OPERATOR = "Operator";

	public static final String IO_NUM_TPS = "tps";
	public static final String IO_NUM_DELAY = "delay";
	public static final String IO_NUM_RECORDS_IN = "numRecordsIn";
	public static final String IO_NUM_RECORDS_OUT = "numRecordsOut";
	public static final String IO_NUM_RECORDS_IN_RATE = IO_NUM_RECORDS_IN + SUFFIX_RATE;
	public static final String IO_NUM_RECORDS_OUT_RATE = IO_NUM_RECORDS_OUT + SUFFIX_RATE;

	public static final String IO_NUM_OPERATOR_RECORDS_IN = "numRecordsIn" + SUFFIX_OPERATOR;
	public static final String IO_NUM_OPERATOR_RECORDS_OUT = "numRecordsOut" + SUFFIX_OPERATOR;
	public static final String IO_NUM_OPERATOR_RECORDS_IN_RATE = IO_NUM_RECORDS_IN + SUFFIX_OPERATOR + SUFFIX_RATE;
	public static final String IO_NUM_OPERATOR_RECORDS_OUT_RATE = IO_NUM_RECORDS_OUT + SUFFIX_OPERATOR + SUFFIX_RATE;

	public static final String IO_NUM_BYTES_IN = "numBytesIn";
	public static final String IO_NUM_BYTES_IN_LOCAL = IO_NUM_BYTES_IN + "Local";
	public static final String IO_NUM_BYTES_IN_REMOTE = IO_NUM_BYTES_IN + "Remote";
	public static final String IO_NUM_BYTES_OUT = "numBytesOut";
	public static final String IO_NUM_BUFFERS_OUT = "numBuffersOut";
	public static final String IO_NUM_BYTES_IN_LOCAL_RATE = IO_NUM_BYTES_IN_LOCAL + SUFFIX_RATE;
	public static final String IO_NUM_BYTES_IN_REMOTE_RATE = IO_NUM_BYTES_IN_REMOTE + SUFFIX_RATE;
	public static final String IO_NUM_BYTES_OUT_RATE = IO_NUM_BYTES_OUT + SUFFIX_RATE;

	public static final String IO_CURRENT_INPUT_WATERMARK = "currentInputWatermark";
	public static final String IO_CURRENT_INPUT_1_WATERMARK = "currentInput1Watermark";
	public static final String IO_CURRENT_INPUT_2_WATERMARK = "currentInput2Watermark";
	public static final String IO_CURRENT_OUTPUT_WATERMARK = "currentOutputWatermark";

	public static final String BUFFERS = "buffers";
	public static final String BUFFERS_INPUT_QUEUE_LENGTH = "inputQueueLength";
	public static final String BUFFERS_INPUT_QUEUE_LENGTH_NAME = BUFFERS + "." + BUFFERS_INPUT_QUEUE_LENGTH;
	public static final String BUFFERS_OUT_QUEUE_LENGTH = "outputQueueLength";
	public static final String BUFFERS_OUT_QUEUE_LENGTH_NAME = BUFFERS + "." + BUFFERS_OUT_QUEUE_LENGTH;
	public static final String BUFFERS_IN_POOL_USAGE = "inPoolUsage";
	public static final String BUFFERS_IN_POOL_USAGE_NAME = BUFFERS + "." + BUFFERS_IN_POOL_USAGE;
	public static final String BUFFERS_OUT_POOL_USAGE = "outPoolUsage";
	public static final String BUFFERS_OUT_POOL_USAGE_NAME = BUFFERS + "." + BUFFERS_OUT_POOL_USAGE;

	public static final String IO_NUM_LATENCY = "latency";
	public static final String IO_WAIT_BUFFER_TIME = "waitOutput";
	public static final String IO_WAIT_INPUT = "waitInput";
	public static final String TASK_LATENCY = "taskLatency";
	public static final String SOURCE_LATENCY = "sourceLatency";
}
