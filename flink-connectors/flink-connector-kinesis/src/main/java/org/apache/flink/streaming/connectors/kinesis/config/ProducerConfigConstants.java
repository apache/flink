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
 *
 * @deprecated This class is deprecated in favor of the official AWS Kinesis producer configuration keys.
 *             See <a href="https://github.com/awslabs/amazon-kinesis-producer/blob/master/java/amazon-kinesis-producer-sample/default_config.properties">
 *             here</a> for the full list of available configs.
 *             For configuring the region and credentials, please use the keys in {@link AWSConfigConstants}.
 */
@Deprecated
public class ProducerConfigConstants extends AWSConfigConstants {

	/**
	 * Deprecated key.
	 *
	 * @deprecated This is deprecated in favor of the official AWS Kinesis producer configuration keys.
	 *             Please use {@code CollectionMaxCount} instead.
	 **/
	@Deprecated
	public static final String COLLECTION_MAX_COUNT = "aws.producer.collectionMaxCount";

 	/**
	 * Deprecated key.
	 *
	 * @deprecated This is deprecated in favor of the official AWS Kinesis producer configuration keys.
	 *             Please use {@code AggregationMaxCount} instead.
	 **/
	@Deprecated
	public static final String AGGREGATION_MAX_COUNT = "aws.producer.aggregationMaxCount";
}
