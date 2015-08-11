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

package org.apache.flink.streaming.connectors.internals;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.apache.flink.kafka_backport.clients.consumer.CommitType;
import org.apache.flink.kafka_backport.clients.consumer.ConsumerRecord;
import org.apache.flink.kafka_backport.clients.consumer.ConsumerRecords;
import org.apache.flink.kafka_backport.clients.consumer.KafkaConsumer;
import org.apache.flink.kafka_backport.common.TopicPartition;
import org.apache.flink.kafka_backport.common.serialization.ByteArrayDeserializer;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * A fetcher that uses the new Kafka consumer API to fetch data for a specifies set of partitions.
 */
public class NewConsumerApiFetcher implements Fetcher, OffsetHandler {

	private static final String POLL_TIMEOUT_PROPERTY = "flink.kafka.consumer.poll.timeout";
	private static final long DEFAULT_POLL_TIMEOUT = 50;
	
	private static final ByteArrayDeserializer NO_OP_SERIALIZER = new ByteArrayDeserializer();

	
	private final KafkaConsumer<byte[], byte[]> fetcher;
	
	private final long pollTimeout;
	
	private volatile boolean running = true;

	
	public NewConsumerApiFetcher(Properties props) {
		this.pollTimeout = props.contains(POLL_TIMEOUT_PROPERTY) ?
				Long.valueOf(props.getProperty(POLL_TIMEOUT_PROPERTY)) :
				DEFAULT_POLL_TIMEOUT;
		
		this.fetcher = new KafkaConsumer<byte[], byte[]>(props, null, NO_OP_SERIALIZER, NO_OP_SERIALIZER);
	}

	@Override
	public void setPartitionsToRead(List<TopicPartition> partitions) {
		synchronized (fetcher) {
			if (fetcher.subscriptions().isEmpty()) {
				fetcher.subscribe(partitions.toArray(new TopicPartition[partitions.size()]));
			}
			else {
				throw new IllegalStateException("Fetcher has already subscribed to its set of partitions");
			}
		}
	}

	@Override
	public void seek(TopicPartition topicPartition, long offsetToRead) {
		synchronized (fetcher) {
			fetcher.seek(topicPartition, offsetToRead);
		}
	}

	@Override
	public void close() {
		running = false;
		synchronized (fetcher) {
			fetcher.close();
		}
	}

	@Override
	public <T> void run(SourceFunction.SourceContext<T> sourceContext,
						DeserializationSchema<T> valueDeserializer, long[] lastOffsets) {
		while (running) {
			// poll is always returning a new object.
			ConsumerRecords<byte[], byte[]> consumed;
			synchronized (fetcher) {
				consumed = fetcher.poll(pollTimeout);
			}

			final Iterator<ConsumerRecord<byte[], byte[]>> records = consumed.iterator();
			while (running && records.hasNext()) {
				ConsumerRecord<byte[], byte[]> record = records.next();
				T value = valueDeserializer.deserialize(record.value());
				
				// synchronize inside the loop to allow checkpoints in between batches
				synchronized (sourceContext.getCheckpointLock()) {
					sourceContext.collect(value);
					lastOffsets[record.partition()] = record.offset();
				}
			}
		}
	}

	@Override
	public void commit(Map<TopicPartition, Long> offsetsToCommit) {
		synchronized (fetcher) {
			fetcher.commit(offsetsToCommit, CommitType.SYNC);
		}
	}

	@Override
	public void seekFetcherToInitialOffsets(List<TopicPartition> partitions, Fetcher fetcher) {
		// no need to do anything here.
		// if Kafka manages the offsets, it has them automatically
	}
}
