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

package org.apache.flink.streaming.connectors.kafka.v2;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/** Kafka08 options. **/
public class Kafka08Options {

	public static final Set<String> ESSENTIAL_CONSUMER_KEYS = new HashSet<>(Arrays.asList("group.id", "zookeeper.connect"));
	private static final String[] optionalConsumerKeys = new String[]{
		"bootstrap.servers", "consumer.id", "socket.timeout.ms",
		"fetch.message.max.bytes", "num.consumer.fetchers", "auto.commit.enable", "auto.commit.interval.ms",
		"queued.max.message.chunks", "rebalance.max.retries", "fetch.min.bytes", "fetch.wait.max.ms",
		"rebalance.backoff.ms", "refresh.leader.backoff.ms", "auto.offset.reset", "consumer.timeout.ms",
		"exclude.internal.topics", "partition.assignment.strategy", "client.id", "zookeeper.session.timeout.ms",
		"zookeeper.connection.timeout.ms", "zookeeper.sync.time.ms", "offsets.storage",
		"offsets.channel.backoff.ms", "offsets.channel.socket.timeout.ms", "offsets.commit.max.retries",
		"dual.commit.enabled", "partition.assignment.strategy", "socket.receive.buffer.bytes",
		"fetch.min.bytes"};
	public static final Set<String> OPTIONAL_CONSUMER_KEYS = new HashSet<>(Arrays.asList(optionalConsumerKeys));
	public static final Set<String> ESSENTIAL_PRODUCER_KEYS = new HashSet<>(Arrays.asList("bootstrap.servers"));
	private static final String[] optionalProducerKeys = new String[]{
		"acks", "buffer.memory", "compression.type", "batch.size", "client.id",
		"linger.ms", "max.request.size", "receive.buffer.bytes", "send.buffer.bytes", "timeout.ms",
		"block.on.buffer.full", "metadata.fetch.timeout.ms", "metadata.max.age.ms", "metric.reporters",
		"metrics.num.samples", "metrics.sample.window.ms", "reconnect.backoff.ms", "retry.backoff.ms",
		"key.serializer", "value.serializer", "retries"
	};
	public static final Set<String> OPTIONAL_PRODUCER_KEYS = new HashSet<>(Arrays.asList(optionalProducerKeys));
}
