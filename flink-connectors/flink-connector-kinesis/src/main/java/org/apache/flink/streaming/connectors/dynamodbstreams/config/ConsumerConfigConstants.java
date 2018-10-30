/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.dynamodbstreams.config;

import org.apache.flink.streaming.connectors.kinesis.config.AWSConfigConstants;

/**
 * Optional consumer specific configuration keys and default values for {@link org.apache.flink.streaming.connectors.dynamodbstreams.FlinkDynamodbStreamsConsumer}.
 */
public class ConsumerConfigConstants extends AWSConfigConstants {
	/**
	 * The base backoff time between each describeStream attempt.
	 * Different tag name to distinguish from "flink.stream.describe.backoff.base"
	 * since the latter is deprecated.
	 */
	public static final String STREAM_DESCRIBE_BACKOFF_BASE =
			"flink.dynamodbstreams.describe.backoff.base";

	/**
	 * The maximum backoff time between each describeStream attempt.
	 * Different tag name to distinguish from "flink.stream.describe.backoff.max"
	 * since the latter is deprecated.
	 */
	public static final String STREAM_DESCRIBE_BACKOFF_MAX =
			"flink.dynamodbstreams.describe.backoff.max";

	/**
	 * The power constant for exponential backoff between each describeStream attempt.
	 * Different tag name to distinguish from "flink.stream.describe.backoff.expcost"
	 * since the latter is deprecated.
	 */
	public static final String STREAM_DESCRIBE_BACKOFF_EXPONENTIAL_CONSTANT =
			"flink.dynamodbstreams.describe.backoff.expconst";

	/**
	 * Boolean to indicate whether to compare/enforce shardId format based on the one defined in
	 * DynamodbStreamsShardHandle.
	 */
	public static final String STREAM_SHARDID_FORMAT_CHECK =
			"flink.dynamodbstreams.shardid.format.check";

	public static final long DEFAULT_STREAM_DESCRIBE_BACKOFF_BASE = 1000L;

	public static final long DEFAULT_STREAM_DESCRIBE_BACKOFF_MAX = 5000L;

	public static final double DEFAULT_STREAM_DESCRIBE_BACKOFF_EXPONENTIAL_CONSTANT = 1.5;

	// By default disable shardId format check.
	public static final String DEFAULT_STREAM_SHARDID_FORMAT_CHECK = "false";
}
