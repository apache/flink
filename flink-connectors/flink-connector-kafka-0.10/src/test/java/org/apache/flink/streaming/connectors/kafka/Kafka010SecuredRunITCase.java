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

import org.apache.flink.test.util.SecureTestEnvironment;
import org.apache.flink.testutils.junit.FailsOnJava11;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Kafka Secure Connection (kerberos) IT test case.
 */
@Category(FailsOnJava11.class)
public class Kafka010SecuredRunITCase extends KafkaConsumerTestBase {

	protected static final Logger LOG = LoggerFactory.getLogger(Kafka010SecuredRunITCase.class);

	@BeforeClass
	public static void prepare() throws Exception {
		LOG.info("-------------------------------------------------------------------------");
		LOG.info("    Starting Kafka010SecuredRunITCase ");
		LOG.info("-------------------------------------------------------------------------");

		SecureTestEnvironment.prepare(tempFolder);
		SecureTestEnvironment.populateFlinkSecureConfigurations(getFlinkConfiguration());

		startClusters(true, false);
	}

	@AfterClass
	public static void shutDownServices() throws Exception {
		shutdownClusters();
		SecureTestEnvironment.cleanup();
	}

	//timeout interval is large since in Travis, ZK connection timeout occurs frequently
	//The timeout for the test case is 2 times timeout of ZK connection
	@Test(timeout = 600000)
	public void testMultipleTopics() throws Exception {
		runProduceConsumeMultipleTopics(true);
	}

}
