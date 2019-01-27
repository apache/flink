/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 *
 */

package org.apache.flink.streaming.connectors.kafka;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

/**
 * A Junit rule to provide Kafka as a service in IT cases.
 */
public class KafkaITService extends KafkaTestBase implements TestRule {
	@Override
	public Statement apply(Statement base, Description description) {
		return new Statement() {
			@Override
			public void evaluate() throws Throwable {
				prepare();
				try {
					base.evaluate();
				} finally {
					shutDownServices();
				}
			}
		};
	}

	public static void createTopic(String topic, int numberOfPartitions, int replicationFactor) {
		createTestTopic(topic, numberOfPartitions, replicationFactor);
	}

	public static String brokerConnectionStrings() {
		return brokerConnectionStrings;
	}
}
