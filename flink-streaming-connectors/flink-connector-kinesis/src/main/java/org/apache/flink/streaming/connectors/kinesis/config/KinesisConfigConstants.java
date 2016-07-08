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

package org.apache.flink.streaming.connectors.kinesis.config;

/**
 * Keys and default values used to configure the Kinesis consumer.
 */
public class KinesisConfigConstants {

	// ------------------------------------------------------------------------
	//  Configuration Keys
	// ------------------------------------------------------------------------

	/** The base backoff time between each describeStream attempt */
	public static final String CONFIG_STREAM_DESCRIBE_BACKOFF_BASE = "flink.stream.describe.backoff.base";

	/** The maximum backoff time between each describeStream attempt */
	public static final String CONFIG_STREAM_DESCRIBE_BACKOFF_MAX = "flink.stream.describe.backoff.max";

	/** The power constant for exponential backoff between each describeStream attempt */
	public static final String CONFIG_STREAM_DESCRIBE_BACKOFF_EXPONENTIAL_CONSTANT = "flink.stream.describe.backoff.expconst";

	/** The maximum number of records to try to get each time we fetch records from a AWS Kinesis shard */
	public static final String CONFIG_SHARD_GETRECORDS_MAX = "flink.shard.getrecords.maxrecordcount";

	/** The maximum number of getRecords attempts if we get ProvisionedThroughputExceededException */
	public static final String CONFIG_SHARD_GETRECORDS_RETRIES = "flink.shard.getrecords.maxretries";

	/** The base backoff time between getRecords attempts if we get a ProvisionedThroughputExceededException */
	public static final String CONFIG_SHARD_GETRECORDS_BACKOFF_BASE = "flink.shard.getrecords.backoff.base";

	/** The maximum backoff time between getRecords attempts if we get a ProvisionedThroughputExceededException */
	public static final String CONFIG_SHARD_GETRECORDS_BACKOFF_MAX = "flink.shard.getrecords.backoff.max";

	/** The power constant for exponential backoff between each getRecords attempt */
	public static final String CONFIG_SHARD_GETRECORDS_BACKOFF_EXPONENTIAL_CONSTANT = "flink.shard.getrecords.backoff.expconst";

	/** The maximum number of getShardIterator attempts if we get ProvisionedThroughputExceededException */
	public static final String CONFIG_SHARD_GETITERATOR_RETRIES = "flink.shard.getiterator.maxretries";

	/** The base backoff time between getShardIterator attempts if we get a ProvisionedThroughputExceededException */
	public static final String CONFIG_SHARD_GETITERATOR_BACKOFF_BASE = "flink.shard.getiterator.backoff.base";

	/** The maximum backoff time between getShardIterator attempts if we get a ProvisionedThroughputExceededException */
	public static final String CONFIG_SHARD_GETITERATOR_BACKOFF_MAX = "flink.shard.getiterator.backoff.max";

	/** The power constant for exponential backoff between each getShardIterator attempt */
	public static final String CONFIG_SHARD_GETITERATOR_BACKOFF_EXPONENTIAL_CONSTANT = "flink.shard.getiterator.backoff.expconst";

	/** The interval between each attempt to discover new shards */
	public static final String CONFIG_SHARD_DISCOVERY_INTERVAL_MILLIS = "flink.shard.discovery.intervalmillis";

	/** The initial position to start reading Kinesis streams from (LATEST is used if not set) */
	public static final String CONFIG_STREAM_INIT_POSITION_TYPE = "flink.stream.initpos.type";

	/** The credential provider type to use when AWS credentials are required (BASIC is used if not set)*/
	public static final String CONFIG_AWS_CREDENTIALS_PROVIDER_TYPE = "aws.credentials.provider";

	/** The AWS access key ID to use when setting credentials provider type to BASIC */
	public static final String CONFIG_AWS_CREDENTIALS_PROVIDER_BASIC_ACCESSKEYID = "aws.credentials.provider.basic.accesskeyid";

	/** The AWS secret key to use when setting credentials provider type to BASIC */
	public static final String CONFIG_AWS_CREDENTIALS_PROVIDER_BASIC_SECRETKEY = "aws.credentials.provider.basic.secretkey";

	/** Optional configuration for profile path if credential provider type is set to be PROFILE */
	public static final String CONFIG_AWS_CREDENTIALS_PROVIDER_PROFILE_PATH = "aws.credentials.provider.profile.path";

	/** Optional configuration for profile name if credential provider type is set to be PROFILE */
	public static final String CONFIG_AWS_CREDENTIALS_PROVIDER_PROFILE_NAME = "aws.credentials.provider.profile.name";

	/** The AWS region of the Kinesis streams to be pulled ("us-east-1" is used if not set) */
	public static final String CONFIG_AWS_REGION = "aws.region";

	/** Maximum number of items to pack into an PutRecords request. **/
	public static final String CONFIG_PRODUCER_COLLECTION_MAX_COUNT = "aws.producer.collectionMaxCount";

	/** Maximum number of items to pack into an aggregated record. **/
	public static final String CONFIG_PRODUCER_AGGREGATION_MAX_COUNT = "aws.producer.aggregationMaxCount";


	// ------------------------------------------------------------------------
	//  Default configuration values
	// ------------------------------------------------------------------------

	public static final long DEFAULT_STREAM_DESCRIBE_BACKOFF_BASE = 1000L;

	public static final long DEFAULT_STREAM_DESCRIBE_BACKOFF_MAX = 5000L;

	public static final double DEFAULT_STREAM_DESCRIBE_BACKOFF_EXPONENTIAL_CONSTANT = 1.5;

	public static final int DEFAULT_SHARD_GETRECORDS_MAX = 100;

	public static final int DEFAULT_SHARD_GETRECORDS_RETRIES = 3;

	public static final long DEFAULT_SHARD_GETRECORDS_BACKOFF_BASE = 300L;

	public static final long DEFAULT_SHARD_GETRECORDS_BACKOFF_MAX = 1000L;

	public static final double DEFAULT_SHARD_GETRECORDS_BACKOFF_EXPONENTIAL_CONSTANT = 1.5;

	public static final int DEFAULT_SHARD_GETITERATOR_RETRIES = 3;

	public static final long DEFAULT_SHARD_GETITERATOR_BACKOFF_BASE = 300L;

	public static final long DEFAULT_SHARD_GETITERATOR_BACKOFF_MAX = 1000L;

	public static final double DEFAULT_SHARD_GETITERATOR_BACKOFF_EXPONENTIAL_CONSTANT = 1.5;

	public static final long DEFAULT_SHARD_DISCOVERY_INTERVAL_MILLIS = 10000L;

}
