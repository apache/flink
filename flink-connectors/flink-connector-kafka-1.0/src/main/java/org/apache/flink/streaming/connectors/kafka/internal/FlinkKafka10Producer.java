/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.kafka.internal;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.util.Preconditions;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Inner flink kafka producer.
 */
@PublicEvolving
public class FlinkKafka10Producer<K, V> extends FlinkKafkaProducer<K, V> {
	private static final Logger LOG = LoggerFactory.getLogger(FlinkKafka10Producer.class);

	public FlinkKafka10Producer(Properties properties) {
		super(properties);
	}

	/**
	 * Instead of obtaining producerId and epoch from the transaction coordinator, re-use previously obtained ones,
	 * so that we can resume transaction after a restart. Implementation of this method is based on
	 * {@link KafkaProducer#initTransactions}.
	 * https://github.com/apache/kafka/commit/5d2422258cb975a137a42a4e08f03573c49a387e#diff-f4ef1afd8792cd2a2e9069cd7ddea630
	 */
	public void resumeTransaction(long producerId, short epoch) {
		Preconditions.checkState(producerId >= 0 && epoch >= 0, "Incorrect values for producerId {} and epoch {}", producerId, epoch);
		LOG.info("Attempting to resume transaction {} with producerId {} and epoch {}", transactionalId, producerId, epoch);

		Object transactionManager = getValue(kafkaProducer, "transactionManager");
		synchronized (transactionManager) {
			Object nextSequence = getValue(transactionManager, "nextSequence");

			invoke(transactionManager, "transitionTo", getEnum("org.apache.kafka.clients.producer.internals.TransactionManager$State.INITIALIZING"));
			invoke(nextSequence, "clear");

			Object producerIdAndEpoch = getValue(transactionManager, "producerIdAndEpoch");
			setValue(producerIdAndEpoch, "producerId", producerId);
			setValue(producerIdAndEpoch, "epoch", epoch);

			invoke(transactionManager, "transitionTo", getEnum("org.apache.kafka.clients.producer.internals.TransactionManager$State.READY"));

			invoke(transactionManager, "transitionTo", getEnum("org.apache.kafka.clients.producer.internals.TransactionManager$State.IN_TRANSACTION"));
			setValue(transactionManager, "transactionStarted", true);
		}
	}

}
