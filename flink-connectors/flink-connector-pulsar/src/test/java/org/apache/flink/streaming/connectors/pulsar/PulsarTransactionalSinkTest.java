/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.pulsar;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.pulsar.table.PulsarSinkSemantic;
import org.apache.flink.streaming.connectors.pulsar.testutils.FailingIdentityMapper;
import org.apache.flink.streaming.connectors.pulsar.testutils.IntegerSource;
import org.apache.flink.streaming.connectors.pulsar.testutils.PulsarContainer;
import org.apache.flink.streaming.util.TestStreamEnvironment;
import org.apache.flink.streaming.util.serialization.PulsarSerializationSchemaWrapper;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.test.util.TestUtils;

import io.streamnative.tests.pulsar.service.PulsarServiceSpec;
import io.streamnative.tests.pulsar.service.testcontainers.containers.PulsarStandaloneContainer;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.compress.utils.Sets;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Test for pulsar transactional sink that guarantee exactly-once semantic.
 */
@Slf4j
public class PulsarTransactionalSinkTest {
	private PulsarAdmin admin;
	public static final String CLUSTER_NAME = "standalone";

	private static PulsarStandaloneContainer container;
	private static String serviceUrl;
	private static String adminUrl;

	@BeforeClass
	public static void prepare() throws Exception {

		log.info("-------------------------------------------------------------------------");
		log.info("    Starting PulsarTestBase ");
		log.info("-------------------------------------------------------------------------");
		if (System.getProperty("pulsar.systemtest.image") == null) {
			System.setProperty("pulsar.systemtest.image", "apachepulsar/pulsar:2.7.0");
		}
		PulsarServiceSpec spec = PulsarServiceSpec.builder()
			.clusterName("standalone-" + UUID.randomUUID())
			.enableContainerLogging(false)
			.build();
		container = new PulsarContainer(spec.clusterName())
			.withClasspathResourceMapping(
				"txnStandalone.conf",
				"/pulsar/conf/standalone.conf",
				BindMode.READ_ONLY)
			.withNetwork(Network.newNetwork())
			.withNetworkAliases(PulsarStandaloneContainer.NAME + "-" + spec.clusterName());
		if (spec.enableContainerLogging()) {
			container.withLogConsumer(new Slf4jLogConsumer(log));
		}
		container.start();
		serviceUrl = container.getExposedPlainTextServiceUrl();
		adminUrl = container.getExposedHttpServiceUrl();

		Thread.sleep(80 * 100L);
		log.info("-------------------------------------------------------------------------");
		log.info("Successfully started pulsar service at cluster " + spec.clusterName());
		log.info("-------------------------------------------------------------------------");

	}

	@AfterClass
	public static void shutDownServices() throws Exception {
		log.info("-------------------------------------------------------------------------");
		log.info("    Shut down PulsarTestBase ");
		log.info("-------------------------------------------------------------------------");

		TestStreamEnvironment.unsetAsContext();

		if (container != null) {
			container.stop();
		}

		log.info("-------------------------------------------------------------------------");
		log.info("    PulsarTestBase finished");
		log.info("-------------------------------------------------------------------------");
	}

	/**
	 * Tests the exactly-once semantic for the simple writes into Pulsar.
	 */
	@Test
	public void testExactlyOnceRegularSink() throws Exception {
		admin = PulsarAdmin.builder().serviceHttpUrl(adminUrl).build();
		admin.tenants().createTenant(
			NamespaceName.SYSTEM_NAMESPACE.getTenant(),
			new TenantInfo(Sets.newHashSet("app1"), Sets.newHashSet(CLUSTER_NAME)));
		admin.namespaces().createNamespace(NamespaceName.SYSTEM_NAMESPACE.toString());
		admin
			.topics()
			.createPartitionedTopic(TopicName.TRANSACTION_COORDINATOR_ASSIGN.toString(), 16);

		testExactlyOnce(1);
	}

	protected void testExactlyOnce(int sinksCount) throws Exception {
		final String topic = "ExactlyOnceTopicSink" + UUID.randomUUID();
		final int numElements = 1000;
		final int failAfterElements = 333;

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.enableCheckpointing(500);
		env.setParallelism(1);
		env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 0));

		// process exactly failAfterElements number of elements and then shutdown Pulsar broker and fail application
		List<Integer> expectedElements = getIntegersSequence(numElements);

		DataStream<Integer> inputStream = env
			.addSource(new IntegerSource(numElements))
			.map(new FailingIdentityMapper<Integer>(failAfterElements));

		for (int i = 0; i < sinksCount; i++) {
			ClientConfigurationData clientConfigurationData = new ClientConfigurationData();
			clientConfigurationData.setServiceUrl(serviceUrl);
			SinkFunction<Integer> sink = new FlinkPulsarSink<>(
				adminUrl,
				Optional.of(topic),
				clientConfigurationData,
				new Properties(),
				new PulsarSerializationSchemaWrapper.Builder<>
					((SerializationSchema<Integer>) element -> Schema.INT32.encode(element))
					.useAtomicMode(DataTypes.INT())
					.build(),
				PulsarSinkSemantic.EXACTLY_ONCE
			);
			inputStream.addSink(sink);
		}

		FailingIdentityMapper.failedBefore = false;
		TestUtils.tryExecute(env, "Exactly once test");
		for (int i = 0; i < sinksCount; i++) {
			// assert that before failure we successfully snapshot/flushed all expected elements
			assertExactlyOnceForTopic(
				topic,
				expectedElements,
				60000L);
		}

	}

	private List<Integer> getIntegersSequence(int size) {
		List<Integer> result = new ArrayList<>(size);
		for (int i = 0; i < size; i++) {
			result.add(i);
		}
		return result;
	}

	/**
	 * We manually handle the timeout instead of using JUnit's timeout to return failure instead of timeout error.
	 * After timeout we assume that there are missing records and there is a bug, not that the test has run out of time.
	 */
	public void assertExactlyOnceForTopic(
		String topic,
		List<Integer> expectedElements,
		long timeoutMillis) throws Exception {

		long startMillis = System.currentTimeMillis();
		List<Integer> actualElements = new ArrayList<>();

		// until we timeout...
		PulsarClient client = PulsarClient
			.builder()
			.enableTransaction(true)
			.serviceUrl(serviceUrl)
			.build();
		Consumer<Integer> test = client
			.newConsumer(Schema.INT32)
			.topic(topic)
			.subscriptionName("test-exactly" + UUID.randomUUID())
			.subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
			.subscribe();
		while (System.currentTimeMillis() < startMillis + timeoutMillis) {
			// query pulsar for new records ...
			Message<Integer> message = test.receive();
			log.info(
				"consume the message {} with the value {}",
				message.getMessageId(),
				message.getValue());
			actualElements.add(message.getValue());
			// succeed if we got all expectedElements
			if (actualElements.size() == expectedElements.size()) {
				assertEquals(expectedElements, actualElements);
				return;
			}
			if (actualElements.equals(expectedElements)) {
				return;
			}
			// fail early if we already have too many elements
			if (actualElements.size() > expectedElements.size()) {
				break;
			}
		}

		fail(String.format(
			"Expected %s, but was: %s",
			formatElements(expectedElements),
			formatElements(actualElements)));
	}

	private String formatElements(List<Integer> elements) {
		if (elements.size() > 50) {
			return String.format("number of elements: <%s>", elements.size());
		} else {
			return String.format("elements: <%s>", elements);
		}
	}
}
