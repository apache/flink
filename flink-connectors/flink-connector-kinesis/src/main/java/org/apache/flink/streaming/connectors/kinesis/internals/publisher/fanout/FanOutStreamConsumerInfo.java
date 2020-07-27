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

package org.apache.flink.streaming.connectors.kinesis.internals.publisher.fanout;

/**
 * This is a data class which describe all related information when de-/registering streams.
 */
public class FanOutStreamConsumerInfo {
	/**
	 * Public constructor for fan out stream info.
	 * @param stream the kinesis' stream name.
	 * @param streamArn the kinesis' stream arn.
	 * @param consumerName the kinesis' enhanced fan-out consumer name.
	 * @param consumerArn the kinesis' enhanced fan-out consumer arn.
	 */
	public FanOutStreamConsumerInfo(String stream, String streamArn, String consumerName, String consumerArn) {
		this.stream = stream;
		this.streamArn = streamArn;
		this.consumerName = consumerName;
		this.consumerArn = consumerArn;
	}

	/** Kinesis stream name. */
	private final String stream;

	/** Kinesis stream arn. */
	private final String streamArn;

	/** Registered consumer name for the related stream. */
	private final String consumerName;

	/** Registered consumer arn for the related stream. */
	private final String consumerArn;

	/**
	 * Return the Kinesis stream name.
	 */
	public String getStream() {
		return stream;
	}

	/**
	 * Return the Kinesis stream arn.
	 */
	public String getStreamArn() {
		return streamArn;
	}

	/**
	 * Return the Kinesis consumer name for an enhanced fan-out consumer.
	 */
	public String getConsumerName() {
		return consumerName;
	}

	/**
	 * Return the Kinesis consumer arn for an enhanced fan-out consumer.
	 */
	public String getConsumerArn() {
		return consumerArn;
	}
}
