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

package org.apache.flink.streaming.connectors.kafka.api.simple.offset;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.TopicAndPartition;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.consumer.SimpleConsumer;

/**
 * Superclass for various kinds of KafkaOffsets.
 */
public abstract class KafkaOffset implements Serializable {

	private static final Logger LOG = LoggerFactory.getLogger(KafkaOffset.class);

	private static final long serialVersionUID = 1L;

	public abstract long getOffset(SimpleConsumer consumer, String topic, int partition,
			String clientName);

	/**
	 *
	 * @param consumer
	 * @param topic
	 * @param partition
	 * @param whichTime Type of offset request (latest time / earliest time)
	 * @param clientName
	 * @return
	 */
	protected long getLastOffset(SimpleConsumer consumer, String topic, int partition,
			long whichTime, String clientName) {
		TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
		Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
		requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(whichTime, 1));

		kafka.javaapi.OffsetRequest request = new kafka.javaapi.OffsetRequest(requestInfo,
				kafka.api.OffsetRequest.CurrentVersion(), clientName);
		OffsetResponse response = consumer.getOffsetsBefore(request);

		while (response.hasError()) {
			int errorCode = response.errorCode(topic, partition);
			LOG.warn("Response has error. Error code "+errorCode);
			switch (errorCode) {
				case 6:
				case 3:
					LOG.warn("Kafka broker trying to fetch from a non-leader broker.");
					break;
				default:
					throw new RuntimeException("Error fetching data from Kafka broker. Error code " + errorCode);
			}

			request = new kafka.javaapi.OffsetRequest(requestInfo, kafka.api.OffsetRequest.CurrentVersion(), clientName);
			response = consumer.getOffsetsBefore(request);
		}

		long[] offsets = response.offsets(topic, partition);
		if(offsets.length > 1) {
			LOG.warn("The offset response unexpectedly contained more than one offset: "+ Arrays.toString(offsets) + " Using only first one");
		}
		return offsets[0];
	}

}
