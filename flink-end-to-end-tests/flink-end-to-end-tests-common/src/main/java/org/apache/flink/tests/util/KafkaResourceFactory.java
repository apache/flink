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

package org.apache.flink.tests.util;

import java.nio.file.Path;

/**
 * Kafka resource factory to create {@link KafkaResource}. it will be a {@link LocalStandaloneKafkaResource} or
 * {@link DistributionKafkaResource}, depends on the JVM property -De2e.kafka.mode setting.
 */
public class KafkaResourceFactory {

	public static final String E2E_KAFKA_MODE = "e2e.kafka.mode";
	public static final String E2E_KAFKA_MODE_LOCAL_STANDALONE = "localStandalone";
	public static final String E2E_KAFKA_MODE_DISTRIBUTED = "distributed";

	public static KafkaResource create(String fileURL, String packageName, Path testDataDir) {
		String kafkaMode = System.getProperty(E2E_KAFKA_MODE, E2E_KAFKA_MODE_LOCAL_STANDALONE);

		// Initialize the Kafka resource.
		if (E2E_KAFKA_MODE_LOCAL_STANDALONE.equalsIgnoreCase(kafkaMode)) {
			return new LocalStandaloneKafkaResource(fileURL, packageName, testDataDir);
		} else if (E2E_KAFKA_MODE_DISTRIBUTED.equalsIgnoreCase(kafkaMode)) {
			return new DistributionKafkaResource(fileURL, packageName, testDataDir);
		} else {
			throw new IllegalArgumentException("Invalid JVM property -D" + E2E_KAFKA_MODE + "=" + kafkaMode
				+ ", use -D" + E2E_KAFKA_MODE + "=" + E2E_KAFKA_MODE_LOCAL_STANDALONE
				+ " or use -D" + E2E_KAFKA_MODE + "=" + E2E_KAFKA_MODE_DISTRIBUTED);
		}
	}
}
