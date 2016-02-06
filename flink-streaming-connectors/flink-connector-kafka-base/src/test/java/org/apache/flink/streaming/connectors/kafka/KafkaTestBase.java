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

package org.apache.flink.streaming.connectors.kafka;

import kafka.consumer.ConsumerConfig;


import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.util.ForkableFlinkMiniCluster;
import org.apache.flink.test.util.SuccessException;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.TestLogger;


import org.junit.AfterClass;
import org.junit.BeforeClass;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.concurrent.duration.FiniteDuration;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;


/**
 * The base for the Kafka tests. It brings up:
 * <ul>
 *     <li>A ZooKeeper mini cluster</li>
 *     <li>Three Kafka Brokers (mini clusters)</li>
 *     <li>A Flink mini cluster</li>
 * </ul>
 * 
 * <p>Code in this test is based on the following GitHub repository:
 * <a href="https://github.com/sakserv/hadoop-mini-clusters">
 *   https://github.com/sakserv/hadoop-mini-clusters</a> (ASL licensed),
 * as per commit <i>bc6b2b2d5f6424d5f377aa6c0871e82a956462ef</i></p>
 */
@SuppressWarnings("serial")
public abstract class KafkaTestBase extends TestLogger {

	protected static final Logger LOG = LoggerFactory.getLogger(KafkaTestBase.class);
	
	protected static final int NUMBER_OF_KAFKA_SERVERS = 3;

	protected static String brokerConnectionStrings;

	protected static Properties standardProps;
	
	protected static ForkableFlinkMiniCluster flink;

	protected static int flinkPort;

	protected static FiniteDuration timeout = new FiniteDuration(10, TimeUnit.SECONDS);

	protected static KafkaTestEnvironment kafkaServer;

	// ------------------------------------------------------------------------
	//  Setup and teardown of the mini clusters
	// ------------------------------------------------------------------------
	
	@BeforeClass
	public static void prepare() throws IOException, ClassNotFoundException {
		LOG.info("-------------------------------------------------------------------------");
		LOG.info("    Starting KafkaITCase ");
		LOG.info("-------------------------------------------------------------------------");
		


		// dynamically load the implementation for the test
		Class<?> clazz = Class.forName("org.apache.flink.streaming.connectors.kafka.KafkaTestEnvironmentImpl");
		kafkaServer = (KafkaTestEnvironment) InstantiationUtil.instantiate(clazz);

		LOG.info("Starting KafkaITCase.prepare() for Kafka " + kafkaServer.getVersion());

		kafkaServer.prepare(NUMBER_OF_KAFKA_SERVERS);

		standardProps = kafkaServer.getStandardProperties();
		brokerConnectionStrings = kafkaServer.getBrokerConnectionString();

		// start also a re-usable Flink mini cluster
		Configuration flinkConfig = new Configuration();
		flinkConfig.setInteger(ConfigConstants.LOCAL_NUMBER_TASK_MANAGER, 1);
		flinkConfig.setInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, 8);
		flinkConfig.setInteger(ConfigConstants.TASK_MANAGER_MEMORY_SIZE_KEY, 16);
		flinkConfig.setString(ConfigConstants.EXECUTION_RETRY_DELAY_KEY, "0 s");

		flink = new ForkableFlinkMiniCluster(flinkConfig, false);
		flink.start();

		flinkPort = flink.getLeaderRPCPort();
	}

	@AfterClass
	public static void shutDownServices() {

		LOG.info("-------------------------------------------------------------------------");
		LOG.info("    Shut down KafkaITCase ");
		LOG.info("-------------------------------------------------------------------------");

		flinkPort = -1;
		if (flink != null) {
			flink.shutdown();
		}

		kafkaServer.shutdown();

		LOG.info("-------------------------------------------------------------------------");
		LOG.info("    KafkaITCase finished"); 
		LOG.info("-------------------------------------------------------------------------");
	}



	// ------------------------------------------------------------------------
	//  Execution utilities
	// ------------------------------------------------------------------------
	

	protected static void tryExecutePropagateExceptions(StreamExecutionEnvironment see, String name) throws Exception {
		try {
			see.execute(name);
		}
		catch (ProgramInvocationException | JobExecutionException root) {
			Throwable cause = root.getCause();

			// search for nested SuccessExceptions
			int depth = 0;
			while (!(cause instanceof SuccessException)) {
				if (cause == null || depth++ == 20) {
					throw root;
				}
				else {
					cause = cause.getCause();
				}
			}
		}
	}

	protected static void createTestTopic(String topic, int numberOfPartitions, int replicationFactor) {
		kafkaServer.createTestTopic(topic, numberOfPartitions, replicationFactor);
	}
	
	protected static void deleteTestTopic(String topic) {
		kafkaServer.deleteTestTopic(topic);
	}
}
