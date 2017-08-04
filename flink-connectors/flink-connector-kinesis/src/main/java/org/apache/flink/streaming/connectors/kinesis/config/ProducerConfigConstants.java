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

import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisProducer;

/**
 * Optional producer specific configuration keys for {@link FlinkKinesisProducer}.
 */
public class ProducerConfigConstants extends AWSConfigConstants {

	/** Maximum number of KPL user records to store in a single Kinesis Streams record (an aggregated record). */
	public static final String AGGREGATION_MAX_COUNT = "aws.producer.aggregationMaxCount";

	/** Maximum number of Kinesis Streams records to pack into an PutRecords request. */
	public static final String COLLECTION_MAX_COUNT = "aws.producer.collectionMaxCount";

	/** Maximum number of connections to open to the backend. HTTP requests are
	 * sent in parallel over multiple connections */
	public static final String MAX_CONNECTIONS = "aws.producer.maxConnections";

	/** Limits the maximum allowed put rate for a shard, as a percentage of the backend limits. */
	public static final String RATE_LIMIT = "aws.producer.rateLimit";

	/** Maximum amount of itme (milliseconds) a record may spend being buffered
	 * before it gets sent. Records may be sent sooner than this depending on the
	 * other buffering limits. */
	public static final String RECORD_MAX_BUFFERED_TIME = "aws.producer.recordMaxBufferedTime";

	/** Set a time-to-live on records (milliseconds). Records that do not get
	 * successfully put within the limit are failed. */
	public static final String RECORD_TIME_TO_LIVE = "aws.producer.recordTtl";

	/** The maximum total time (milliseconds) elapsed between when we begin a HTTP
	 * request and receiving all of the response. If it goes over, the request
	 * will be timed-out. */
	public static final String REQUEST_TIMEOUT = "aws.producer.requestTimeout";
}
