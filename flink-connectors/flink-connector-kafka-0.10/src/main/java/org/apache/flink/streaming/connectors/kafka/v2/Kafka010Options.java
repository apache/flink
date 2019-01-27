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

/** Kafka10 options. **/
public class Kafka010Options {
	public static final Set<String> ESSENTIAL_CONSUMER_KEYS = new HashSet<>(Arrays.asList("group.id", "bootstrap.servers"));
	private static final String[] optionalConsumerKeys = new String[]{
		"bootstrap.servers", "fetch.min.bytes", "group.id", "heartbeat.interval.ms",
		"max.partition.fetch.bytes", "session.timeout.ms", "group.min.session.timeout.ms", "ssl.key.password",
		"ssl.keystore.password", "ssl.keystore.location", "ssl.truststore.location", "ssl.truststore.password",
		"auto.offset.reset", "connections.max.idle.ms", "enable.auto.commit", "exclude.internal.topics",
		"fetch.max.bytes", "max.poll.interval.ms", "max.poll.records", "partition.assignment.strategy",
		"receive.buffer.bytes", "request.timeout.ms", "sasl.jaas.config", "sasl.kerberos.service.name",
		"sasl.mechanism", "security.protocol", "send.buffer.bytes", "ssl.enabled.protocols",
		"ssl.keystore.type", "ssl.keystore.location", "client.id", "ssl.trustmanager.algorithm",
		"ssl.protocol", "ssl.provider", "ssl.truststore.type", "auto.commit.interval.ms", "check.crcs",
		"fetch.max.wait.ms", "interceptor.classes", "metadata.max.age.ms", "metric.reporters",
		"metrics.num.samples", "metrics.recording.level", "metrics.sample.window.ms", "reconnect.backoff.ms",
		"retry.backoff.ms", "sasl.kerberos.kinit.cmd", "sasl.kerberos.min.time.before.relogin",
		"sasl.kerberos.ticket.renew.jitter", "sasl.kerberos.ticket.renew.window.factor", "ssl.cipher.suites",
		"ssl.endpoint.identification.algorithm", "ssl.keymanager.algorithm", "ssl.secure.random.implementation"
	};
	public static final Set<String> OPTIONAL_CONSUMER_KEYS = new HashSet<>(Arrays.asList(optionalConsumerKeys));
	public static final Set<String> ESSENTIAL_PRODUCER_KEYS = new HashSet<>(Arrays.asList("bootstrap.servers"));
	private static final String[] optionalProducerKeys = new String[]{
		"bootstrap.servers", "key.serializer", "value.serializer", "acks", "buffer.memory", "compression.type",
		"ssl.key.password", "ssl.keystore.location", "ssl.keystore.password", "ssl.truststore.location",
		"ssl.truststore.password", "batch.size", "client.id", "connections.max.idle.ms", "linger.ms",
		"max.block.ms", "max.request.size", "partitioner.class", "receive.buffer.bytes", "request.timeout.ms",
		"sasl.jaas.config", "sasl.kerberos.service.name", "sasl.mechanism", "security.protocol",
		"send.buffer.bytes", "ssl.enabled.protocols", "ssl.keystore.type", "ssl.protocol", "ssl.provider",
		"ssl.truststore.type", "timeout.ms", "block.on.buffer.full", "interceptor.classes",
		"max.in.flight.requests.per.connection", "metadata.fetch.timeout.ms", "metadata.max.age.ms",
		"metric.reporters", "metrics.num.samples", "metrics.sample.window.ms", "reconnect.backoff.ms",
		"retry.backoff.ms", "sasl.kerberos.kinit.cmd", "sasl.kerberos.min.time.before.relogin",
		"sasl.kerberos.ticket.renew.jitter", "sasl.kerberos.ticket.renew.window.factor", "ssl.cipher.suites",
		"ssl.endpoint.identification.algorithm", "ssl.keymanager.algorithm", "ssl.secure.random.implementation",
		"ssl.trustmanager.algorithm", "retries"
	};
	public static final Set<String> OPTIONAL_PRODUCER_KEYS = new HashSet<>(Arrays.asList(optionalProducerKeys));
}
