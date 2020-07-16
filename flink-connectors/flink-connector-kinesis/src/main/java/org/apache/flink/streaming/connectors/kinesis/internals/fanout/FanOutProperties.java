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

package org.apache.flink.streaming.connectors.kinesis.internals.fanout;

import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants;
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.EFORegistrationType;
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.RecordPublisherType;
import org.apache.flink.streaming.connectors.kinesis.util.KinesisConfigUtil;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;


/**
 * This is a configuration class for enhanced fan-out components.
 */
public class FanOutProperties {

	/**
	 * The efo registration type for de-/registration of streams.
	 */
	private final EFORegistrationType efoRegistrationType;

	/**
	 * The efo stream consumer name. Should not be Null if the efoRegistrationType is either LAZY or EAGER.
	 */
	@Nullable
	private String consumerName;

	/**
	 * The manual set efo consumer arns for each stream. Should not be Null if the efoRegistrationType is NONE
	 */
	@Nullable
	private Map<String, String> streamConsumerArns;

	/**
	 * Base backoff millis for the deregister stream operation.
	 */
	private final int subscribeToShardMaxRetries;

	/**
	 * Maximum backoff millis for the subscribe to shard operation.
	 */
	private final long subscribeToShardMaxBackoffMillis;

	/**
	 * Base backoff millis for the subscribe to shard operation.
	 */
	private final long subscribeToShardBaseBackoffMillis;

	/**
	 * Exponential backoff power constant for the subscribe to shard operation.
	 */
	private final double subscribeToShardExpConstant;

	/**
	 * Base backoff millis for the register stream operation.
	 */
	private final long registerStreamBaseBackoffMillis;

	/**
	 * Maximum backoff millis for the register stream operation.
	 */
	private final long registerStreamMaxBackoffMillis;

	/**
	 * Exponential backoff power constant for the register stream operation.
	 */
	private final double registerStreamExpConstant;

	/**
	 * Maximum retry attempts for the register stream operation.
	 */
	private final int registerStreamMaxRetries;

	/**
	 * Base backoff millis for the deregister stream operation.
	 */
	private final long deregisterStreamBaseBackoffMillis;

	/**
	 * Maximum backoff millis for the deregister stream operation.
	 */
	private final long deregisterStreamMaxBackoffMillis;

	/**
	 * Exponential backoff power constant for the deregister stream operation.
	 */
	private final double deregisterStreamExpConstant;

	/**
	 * Maximum retry attempts for the deregister stream operation.
	 */
	private final int deregisterStreamMaxRetries;

	/**
	 * Base backoff millis for the list stream operation.
	 */
	private final long listStreamConsumersBaseBackoffMillis;

	/**
	 * Maximum backoff millis for the list stream operation.
	 */
	private final long listStreamConsumersMaxBackoffMillis;

	/**
	 * Exponential backoff power constant for the list stream operation.
	 */
	private final double listStreamConsumersExpConstant;

	/**
	 * Maximum retry attempts for the list stream operation.
	 */
	private final int listStreamConsumersMaxRetries;

	// ------------------------------------------------------------------------
	//  registerStream() related performance settings
	// ------------------------------------------------------------------------
	/**
	 * Creates a FanOutProperties.
	 *
	 * @param configProps the configuration properties from config file.
	 * @param streams     the streams which is sent to match the EFO consumer arn if the EFO registration mode is set to `NONE`.
	 */
	public FanOutProperties(Properties configProps, List<String> streams) {
		Preconditions.checkArgument(configProps.getProperty(ConsumerConfigConstants.RECORD_PUBLISHER_TYPE).equals(RecordPublisherType.EFO.toString()), "Only efo record publisher can register a FanOutProperties.");
		KinesisConfigUtil.validateEfoConfiguration(configProps, streams);

		efoRegistrationType = EFORegistrationType.valueOf(configProps.getProperty(ConsumerConfigConstants.EFO_REGISTRATION_TYPE, EFORegistrationType.EAGER.toString()));
		//if efo registration type is EAGER|LAZY, then user should explicitly provide a consumer name for each stream.
		if (efoRegistrationType == EFORegistrationType.EAGER || efoRegistrationType == EFORegistrationType.LAZY) {
			consumerName = configProps.getProperty(ConsumerConfigConstants.EFO_CONSUMER_NAME);
		} else {
			//else users should explicitly provide consumer arns.
			streamConsumerArns = new HashMap<>();
			for (String stream : streams) {
				String key = ConsumerConfigConstants.EFO_CONSUMER_ARN_PREFIX + "." + stream;
				streamConsumerArns.put(stream, configProps.getProperty(key));
			}
		}

		this.subscribeToShardMaxRetries = Integer.parseInt(
			configProps.getProperty(
				ConsumerConfigConstants.SUBSCRIBE_TO_SHARD_RETRIES,
				Long.toString(ConsumerConfigConstants.DEFAULT_SUBSCRIBE_TO_SHARD_RETRIES)));
		this.subscribeToShardBaseBackoffMillis = Long.parseLong(
			configProps.getProperty(ConsumerConfigConstants.SUBSCRIBE_TO_SHARD_BACKOFF_BASE,
				Long.toString(ConsumerConfigConstants.DEFAULT_SUBSCRIBE_TO_SHARD_BACKOFF_BASE)));
		this.subscribeToShardMaxBackoffMillis = Long.parseLong(
			configProps.getProperty(ConsumerConfigConstants.SUBSCRIBE_TO_SHARD_BACKOFF_MAX,
				Long.toString(ConsumerConfigConstants.DEFAULT_SUBSCRIBE_TO_SHARD_BACKOFF_MAX)));
		this.subscribeToShardExpConstant = Double.parseDouble(
			configProps.getProperty(ConsumerConfigConstants.SUBSCRIBE_TO_SHARD_BACKOFF_EXPONENTIAL_CONSTANT,
				Double.toString(ConsumerConfigConstants.DEFAULT_SUBSCRIBE_TO_SHARD_BACKOFF_EXPONENTIAL_CONSTANT)));

		this.registerStreamBaseBackoffMillis = Long.parseLong(
			configProps.getProperty(
				ConsumerConfigConstants.REGISTER_STREAM_BACKOFF_BASE,
				Long.toString(ConsumerConfigConstants.DEFAULT_REGISTER_STREAM_BACKOFF_BASE)));
		this.registerStreamMaxBackoffMillis = Long.parseLong(
			configProps.getProperty(
				ConsumerConfigConstants.REGISTER_STREAM_BACKOFF_MAX,
				Long.toString(ConsumerConfigConstants.DEFAULT_REGISTER_STREAM_BACKOFF_MAX)));
		this.registerStreamExpConstant = Double.parseDouble(
			configProps.getProperty(
				ConsumerConfigConstants.REGISTER_STREAM_BACKOFF_EXPONENTIAL_CONSTANT,
				Double.toString(ConsumerConfigConstants.DEFAULT_REGISTER_STREAM_BACKOFF_EXPONENTIAL_CONSTANT)));
		this.registerStreamMaxRetries = Integer.parseInt(
			configProps.getProperty(
				ConsumerConfigConstants.REGISTER_STREAM_RETRIES,
				Long.toString(ConsumerConfigConstants.DEFAULT_REGISTER_STREAM_RETRIES)));

		this.deregisterStreamBaseBackoffMillis = Long.parseLong(
			configProps.getProperty(
				ConsumerConfigConstants.DEREGISTER_STREAM_BACKOFF_BASE,
				Long.toString(ConsumerConfigConstants.DEFAULT_DEREGISTER_STREAM_BACKOFF_BASE)));
		this.deregisterStreamMaxBackoffMillis = Long.parseLong(
			configProps.getProperty(
				ConsumerConfigConstants.DEREGISTER_STREAM_BACKOFF_MAX,
				Long.toString(ConsumerConfigConstants.DEFAULT_DEREGISTER_STREAM_BACKOFF_MAX)));
		this.deregisterStreamExpConstant = Double.parseDouble(
			configProps.getProperty(
				ConsumerConfigConstants.DEREGISTER_STREAM_BACKOFF_EXPONENTIAL_CONSTANT,
				Double.toString(ConsumerConfigConstants.DEFAULT_DEREGISTER_STREAM_BACKOFF_EXPONENTIAL_CONSTANT)));
		this.deregisterStreamMaxRetries = Integer.parseInt(
			configProps.getProperty(
				ConsumerConfigConstants.DEREGISTER_STREAM_RETRIES,
				Long.toString(ConsumerConfigConstants.DEFAULT_DEREGISTER_STREAM_RETRIES)));

		this.listStreamConsumersBaseBackoffMillis = Long.parseLong(
			configProps.getProperty(
				ConsumerConfigConstants.LIST_STREAM_CONSUMERS_BACKOFF_BASE,
				Long.toString(ConsumerConfigConstants.DEFAULT_LIST_STREAM_CONSUMERS_BACKOFF_BASE)));
		this.listStreamConsumersMaxBackoffMillis = Long.parseLong(
			configProps.getProperty(
				ConsumerConfigConstants.LIST_STREAM_CONSUMERS_BACKOFF_MAX,
				Long.toString(ConsumerConfigConstants.DEFAULT_LIST_STREAM_CONSUMERS_BACKOFF_MAX)));
		this.listStreamConsumersExpConstant = Double.parseDouble(
			configProps.getProperty(
				ConsumerConfigConstants.LIST_STREAM_CONSUMERS_BACKOFF_EXPONENTIAL_CONSTANT,
				Double.toString(ConsumerConfigConstants.DEFAULT_LIST_STREAM_CONSUMERS_BACKOFF_EXPONENTIAL_CONSTANT)));
		this.listStreamConsumersMaxRetries = Integer.parseInt(
			configProps.getProperty(
				ConsumerConfigConstants.LIST_STREAM_CONSUMERS_RETRIES,
				Long.toString(ConsumerConfigConstants.DEFAULT_LIST_STREAM_CONSUMERS_RETRIES)));
	}

	// ------------------------------------------------------------------------
	//  subscribeToShard() related performance settings
	// ------------------------------------------------------------------------
	/**
	 * Get maximum retry attempts for the subscribe to shard operation.
	 */
	public int getSubscribeToShardMaxRetries() {
		return subscribeToShardMaxRetries;
	}

	/**
	 * Get maximum backoff millis for the subscribe to shard operation.
	 */
	public long getSubscribeToShardMaxBackoffMillis() {
		return subscribeToShardMaxBackoffMillis;
	}

	/**
	 * Get base backoff millis for the subscribe to shard operation.
	 */
	public long getSubscribeToShardBaseBackoffMillis() {
		return subscribeToShardBaseBackoffMillis;
	}

	/**
	 * Get exponential backoff power constant for the subscribe to shard operation.
	 */
	public double getSubscribeToShardExpConstant() {
		return subscribeToShardExpConstant;
	}
	// ------------------------------------------------------------------------
	//  registerStream() related performance settings
	// ------------------------------------------------------------------------

	/**
	 * Get base backoff millis for the register stream operation.
	 */
	public long getRegisterStreamBaseBackoffMillis() {
		return registerStreamBaseBackoffMillis;
	}

	/**
	 * Get maximum backoff millis for the register stream operation.
	 */
	public long getRegisterStreamMaxBackoffMillis() {
		return registerStreamMaxBackoffMillis;
	}

	/**
	 * Get exponential backoff power constant for the register stream operation.
	 */
	public double getRegisterStreamExpConstant() {
		return registerStreamExpConstant;
	}

	/**
	 * Get maximum retry attempts for the register stream operation.
	 */
	public int getRegisterStreamMaxRetries() {
		return registerStreamMaxRetries;
	}

	// ------------------------------------------------------------------------
	//  deregisterStream() related performance settings
	// ------------------------------------------------------------------------

	/**
	 * Get base backoff millis for the deregister stream operation.
	 */
	public long getDeregisterStreamBaseBackoffMillis() {
		return deregisterStreamBaseBackoffMillis;
	}

	/**
	 * Get maximum backoff millis for the deregister stream operation.
	 */
	public long getDeregisterStreamMaxBackoffMillis() {
		return deregisterStreamMaxBackoffMillis;
	}

	/**
	 * Get exponential backoff power constant for the deregister stream operation.
	 */
	public double getDeregisterStreamExpConstant() {
		return deregisterStreamExpConstant;
	}

	/**
	 * Get maximum retry attempts for the register stream operation.
	 */
	public int getDeregisterStreamMaxRetries() {
		return deregisterStreamMaxRetries;
	}
	// ------------------------------------------------------------------------
	//  listStream() related performance settings
	// ------------------------------------------------------------------------

	/**
	 * Get base backoff millis for the list stream consumers operation.
	 */
	public long getListStreamConsumersBaseBackoffMillis() {
		return listStreamConsumersBaseBackoffMillis;
	}

	/**
	 * Get maximum backoff millis for the list stream consumers operation.
	 */
	public long getListStreamConsumersMaxBackoffMillis() {
		return listStreamConsumersMaxBackoffMillis;
	}

	/**
	 * Get exponential backoff power constant for the list stream consumers operation.
	 */
	public double getListStreamConsumersExpConstant() {
		return listStreamConsumersExpConstant;
	}

	/**
	 * Get maximum retry attempts for the list stream consumers operation.
	 */
	public int getListStreamConsumersMaxRetries() {
		return listStreamConsumersMaxRetries;
	}

	/**
	 * Get efo registration type.
	 */
	public EFORegistrationType getEfoRegistrationType() {
		return efoRegistrationType;
	}

	/**
	 * Get consumer name, will be null if efo registration type is 'NONE'.
	 */
	@Nullable
	public String getConsumerName() {
		return consumerName;
	}

	/**
	 * Get stream consumer arns, will be null if efo registration type is 'LAZY' or 'EAGER'.
	 */
	@Nullable
	public Map<String, String> getStreamConsumerArns() {
		return streamConsumerArns;
	}

	/**
	 * Get the according consumer arn to the stream, will be null if efo registration type is 'LAZY' or 'EAGER'.
	 */
	@Nullable
	public String getStreamConsumerArn(String stream) {
		if (this.streamConsumerArns == null) {
			return null;
		}
		return streamConsumerArns.get(stream);
	}
}
