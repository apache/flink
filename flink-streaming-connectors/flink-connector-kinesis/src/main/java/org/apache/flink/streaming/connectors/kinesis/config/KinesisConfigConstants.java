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

	/** The max retries to retrieve metadata from a Kinesis stream using describeStream API
	 * (Note: describeStream attempts may be temporarily blocked due to AWS capping 5 attempts per sec)  */
	public static final String CONFIG_STREAM_DESCRIBE_RETRIES = "flink.stream.describe.retry";

	/** The backoff time between each describeStream attempt */
	public static final String CONFIG_STREAM_DESCRIBE_BACKOFF = "flink.stream.describe.backoff";

	/** The maximum number of records to try to get each time we fetch records from a AWS Kinesis shard */
	public static final String CONFIG_SHARD_RECORDS_PER_GET = "flink.shard.getrecords.maxcount";

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

	public static final int DEFAULT_STREAM_DESCRIBE_RETRY_TIMES = 3;

	public static final long DEFAULT_STREAM_DESCRIBE_BACKOFF = 1000L;

	public static final int DEFAULT_SHARD_RECORDS_PER_GET = 100;

}
