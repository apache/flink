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

package org.apache.flink.streaming.connectors.rocketmq;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.rocketmq.common.ProducerConfig;
import org.apache.flink.streaming.connectors.rocketmq.common.RocketMQMessage;

import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.client.producer.SendStatus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A flink Data Sink for RocketMQ {@link RocketMQSink}.
 */
public class RocketMQSink extends RichSinkFunction<RocketMQMessage> {

	private static final Logger LOG = LoggerFactory.getLogger(RocketMQSink.class);

	/**
	 * define the RocketMQ producer client here to send flink message to RocketMQ cluster.
	 */
	private transient DefaultMQProducer producer = null;

	/**
	 * define the ProducerConfig for producer init.
	 */
	private ProducerConfig producerConfig = null;

	/**
	 * Constructor for RocketMQSink,need the ProducerConfig.
	 *
	 * @param producerConfig
	 * @throws MQClientException
	 */
	public RocketMQSink(ProducerConfig producerConfig) throws MQClientException {

		if (null == this.producerConfig) {
			LOG.error("error producerConfig,exit");
			return;
		} else {
			this.producerConfig = producerConfig;
			if (null == this.producer) {
				producer = new DefaultMQProducer(producerConfig.getProducerGroupName());
			}
		}
	}

	/**
	 * Flink sink life cycle open().
	 *
	 * @param parameters
	 * @throws Exception
	 */
	@Override
	public void open(Configuration parameters) throws Exception {

		LOG.info("RocketMQSink open called");

		if (null != producer) {
			producer.setNamesrvAddr(producerConfig.getNameServerAddress());
			producer.start();
			LOG.info("RocketMQSink started,{},{}", producer.getProducerGroup(), producer.getNamesrvAddr());
		}
	}

	/**
	 * Flink sink life cycle invoke(),all the logical processing are here.
	 *
	 * @param value   The input record.
	 * @param context Additional context about the input record.
	 * @throws Exception
	 */
	@Override
	public void invoke(RocketMQMessage value, Context context) throws Exception {

		SendResult sendResult = producer.send(value);
		if (null != sendResult && !sendResult.getSendStatus().equals(SendStatus.SEND_OK)) {
			LOG.error("RocketMQSink send message failed, error = {}", sendResult.toString());
		} else {
			LOG.debug("RocketMQSink send message {}", sendResult.toString());
		}
	}

	/**
	 * Flink sink life cycle close().
	 *
	 * @throws Exception
	 */
	@Override
	public void close() throws Exception {

		if (producer != null) {
			producer.shutdown();
		}
		LOG.info("RocketMQSink shutdown");
	}
}
