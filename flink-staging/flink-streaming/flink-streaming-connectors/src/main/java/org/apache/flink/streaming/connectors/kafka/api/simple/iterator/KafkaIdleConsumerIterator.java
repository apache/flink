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

package org.apache.flink.streaming.connectors.kafka.api.simple.iterator;

import org.apache.flink.streaming.connectors.kafka.api.simple.MessageWithMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * No-op iterator. Used when more source tasks are available than Kafka partitions
 */
public class KafkaIdleConsumerIterator implements KafkaConsumerIterator {

	private static final Logger LOG = LoggerFactory.getLogger(KafkaIdleConsumerIterator.class);

	public KafkaIdleConsumerIterator() {
		if (LOG.isWarnEnabled()) {
			LOG.warn("Idle Kafka consumer created. The subtask does nothing.");
		}
	}


	@Override
	public void initialize() throws InterruptedException {

	}

	@Override
	public boolean hasNext() {
		return false;
	}

	@Override
	public byte[] next() {
		throw new RuntimeException("Idle consumer has no input.");
	}

	@Override
	public MessageWithMetadata nextWithOffset() {
		throw new RuntimeException("Idle consumer has no input.");
	}
}
