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
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.connectors.rocketmq.common.ConsumerConfig;
import org.apache.flink.streaming.connectors.rocketmq.common.RocketMQMessage;

import com.alibaba.rocketmq.client.consumer.DefaultMQPullConsumer;
import com.alibaba.rocketmq.client.consumer.PullResult;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.message.MessageQueue;
import com.alibaba.rocketmq.common.protocol.heartbeat.MessageModel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A flink Data Source for RocketMQ {@link RocketMQSource}.
 */
public class RocketMQSource extends RichSourceFunction<RocketMQMessage> {

	private static final Logger LOG = LoggerFactory.getLogger(RocketMQSource.class);

	/**
	 * define the ConsumerConfig for consumer init.
	 */
	private ConsumerConfig consumerConfig = null;

	/**
	 * the status for running thread,if equal 'false' then stop the message consumering.
	 */
	private transient volatile boolean running = false;

	/**
	 * the RocketMQ consumer client to get messasge from message cluster.
	 */
	private transient DefaultMQPullConsumer consumer = null;

	/**
	 * defien the offset_table to maintain the status for local message queue.
	 */
	private static final Map<MessageQueue, Long> OFFSE_TABLE = new HashMap<MessageQueue, Long>();

	/**
	 * Constructor for RocketMQSource,need the ConsumerConfig.
	 *
	 * @param consumerConfig
	 */
	public RocketMQSource(ConsumerConfig consumerConfig) {

		if (null == consumerConfig) {
			LOG.error("error consumerConfig,exit");
			return;
		} else {
			this.consumerConfig = consumerConfig;
			if (null == this.consumer) {
				this.consumer = new DefaultMQPullConsumer(this.consumerConfig.getConsumerGroupName());
			}
		}
	}

	/**
	 * Flink source life cycle open().
	 *
	 * @param parameters
	 * @throws Exception
	 */
	@Override
	public void open(Configuration parameters) throws Exception {

		LOG.info("RocketMQSource open called");

		if (null != consumer) {

			consumer.setNamesrvAddr(this.consumerConfig.getNameServerAddress());

			if (consumerConfig.getMessageModel().toUpperCase().indexOf("BROADCASTING") >= 0) {
				consumer.setMessageModel(MessageModel.BROADCASTING);
			} else {
				consumer.setMessageModel(MessageModel.CLUSTERING);
			}

			consumer.start();
			running = true;

			LOG.info("RocketMQSource started,{},{}", consumer.getConsumerGroup(), consumer.getNamesrvAddr());
		}
	}

	/**
	 * Flink source life cycle run().
	 *
	 * @param ctx The context to emit elements to and for accessing locks.
	 * @throws Exception
	 */
	@Override
	public void run(SourceContext<RocketMQMessage> ctx) throws Exception {

		while (running) {

			Set<MessageQueue> mqs = consumer.fetchSubscribeMessageQueues(this.consumerConfig.getSubscribeTopics());
			for (MessageQueue mq : mqs) {

				long offset = consumer.fetchConsumeOffset(mq, true);
				LOG.debug("consumer from the queue:" + mq + ":" + offset);

				PullResult pullResult = consumer.pullBlockIfNotFound(mq, null, getMessageQueueOffset(mq), 32);
				putMessageQueueOffset(mq, pullResult.getNextBeginOffset());

				switch (pullResult.getPullStatus()) {
					case FOUND:
						List<MessageExt> messageExtList = pullResult.getMsgFoundList();
						for (MessageExt message : messageExtList) {

							LOG.debug("fecthed messgae: {}", new String(message.getBody()));
							ctx.collect((RocketMQMessage) message);
						}
						break;
					case NO_MATCHED_MSG:
						break;
					case NO_NEW_MSG:
						break;
					case OFFSET_ILLEGAL:
						break;
				}
			}
		}
	}

	/**
	 * Flink source life cycle cancel().
	 */
	@Override
	public void cancel() {
		running = false;
	}

	/**
	 * Flink source life cycle close().
	 *
	 * @throws Exception
	 */
	@Override
	public void close() throws Exception {

		if (null != consumer) {
			consumer.shutdown();
		}
		LOG.info("RocketMQSource shutdown");
	}

	/**
	 * get Message Queue Offset like the function name.
	 *
	 * @param mq
	 * @return
	 */
	private long getMessageQueueOffset(MessageQueue mq) {

		Long offset = OFFSE_TABLE.get(mq);
		if (offset != null) {
			return offset;
		}

		return 0;
	}

	/**
	 * update the Message Queue Offset like the function name.
	 *
	 * @param mq
	 * @param offset
	 */
	private void putMessageQueueOffset(MessageQueue mq, long offset) {
		OFFSE_TABLE.put(mq, offset);
	}
}
