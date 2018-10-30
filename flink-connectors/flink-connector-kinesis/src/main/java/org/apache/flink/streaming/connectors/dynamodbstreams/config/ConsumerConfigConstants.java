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
			"flink.dynamodbstream.describe.backoff.base";

	/**
	 * The maximum backoff time between each describeStream attempt.
	 * Different tag name to distinguish from "flink.stream.describe.backoff.max"
	 * since the latter is deprecated.
	 */
	public static final String STREAM_DESCRIBE_BACKOFF_MAX =
			"flink.dynamodbstream.describe.backoff.max";

	/**
	 * The power constant for exponential backoff between each describeStream attempt.
	 * Different tag name to distinguish from "flink.stream.describe.backoff.expcost"
	 * since the latter is deprecated.
	 */
	public static final String STREAM_DESCRIBE_BACKOFF_EXPONENTIAL_CONSTANT =
			"flink.dynamodbstream.describe.backoff.expconst";

	/** Boolean to imply whether to compare shards based on the Shard Handle format. */
	public static final String STREAM_SHARDID_FORMAT_CHECK =
			"flink.dynamodbstream.shardid.format.check";

	public static final long DEFAULT_STREAM_DESCRIBE_BACKOFF_BASE = 1000L;

	public static final long DEFAULT_STREAM_DESCRIBE_BACKOFF_MAX = 5000L;

	public static final double DEFAULT_STREAM_DESCRIBE_BACKOFF_EXPONENTIAL_CONSTANT = 1.5;

	public static final String DEFAULT_STREAM_SHARDID_FORMAT_CHECK = "false";
}
