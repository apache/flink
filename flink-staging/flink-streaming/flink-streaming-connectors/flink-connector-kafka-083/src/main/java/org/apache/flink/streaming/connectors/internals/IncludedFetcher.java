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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Properties;

public class IncludedFetcher implements Fetcher {
	public static Logger LOG = LoggerFactory.getLogger(IncludedFetcher.class);

	public final static String POLL_TIMEOUT = "flink.kafka.consumer.poll.timeout";
	public final static long DEFAULT_POLL_TIMEOUT = 50;

	final KafkaConsumer<byte[], byte[]> fetcher;
	final Properties props;
	boolean running = true;

	public IncludedFetcher(Properties props) {
		this.props = props;
		fetcher = new KafkaConsumer<byte[], byte[]>(props, null, new ByteArrayDeserializer(), new ByteArrayDeserializer());
	}

	@Override
	public void partitionsToRead(List<TopicPartition> partitions) {
		fetcher.subscribe(partitions.toArray(new TopicPartition[partitions.size()]));
	}

	@Override
	public void seek(TopicPartition topicPartition, long offsetToRead) {
		fetcher.seek(topicPartition, offsetToRead);
	}

	@Override
	public void close() {
		synchronized (fetcher) {
			fetcher.close();
		}
	}

	@Override
	public <T> void run(SourceFunction.SourceContext<T> sourceContext, DeserializationSchema<T> valueDeserializer, long[] lastOffsets) {
		long pollTimeout = DEFAULT_POLL_TIMEOUT;
		if(props.contains(POLL_TIMEOUT)) {
			pollTimeout = Long.valueOf(props.getProperty(POLL_TIMEOUT));
		}
		while(running) {
			// poll is always returning a new object.
			ConsumerRecords<byte[], byte[]> consumed;
			synchronized (fetcher) {
				consumed = fetcher.poll(pollTimeout);
			}
			if(!consumed.isEmpty()) {
				for(ConsumerRecord<byte[], byte[]> record : consumed) {
					// synchronize inside the loop to allow checkpoints in between
					synchronized (sourceContext.getCheckpointLock()) {
						T value = valueDeserializer.deserialize(record.value());
						sourceContext.collect(value);
						lastOffsets[record.partition()] = record.offset();
					}
				}
			}
		}

		sourceContext.close();
	}

	@Override
	public void stop() {
		running = false;
	}

	@Override
	public void commit(Map<TopicPartition, Long> offsetsToCommit) {
		synchronized (fetcher) {
			fetcher.commit(offsetsToCommit, CommitType.SYNC);
		}
	}

}
