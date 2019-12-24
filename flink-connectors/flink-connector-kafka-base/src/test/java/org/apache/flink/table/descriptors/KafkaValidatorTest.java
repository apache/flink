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

package org.apache.flink.table.descriptors;

import org.apache.flink.util.ExceptionUtils;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.junit.Assert.assertTrue;

/**
 * Tests for the {@link KafkaValidator}.
 */
public class KafkaValidatorTest {

	@Test
	public void testValidateTimestampStartupMode() {
		// start from timestamp with old Kafka version
		final Map<String, String> props1 = new HashMap<>();
		props1.put("connector.property-version", "1");
		props1.put("connector.type", "kafka");
		props1.put("connector.version", "0.8");
		props1.put("connector.topic", "MyTopic");
		props1.put("connector.startup-mode", "timestamp");
		props1.put("connector.startup-timestamp", "2019-12-20 20:11:38.221");

		final DescriptorProperties descriptorProperties1 = new DescriptorProperties();
		descriptorProperties1.putProperties(props1);
		try {
			new KafkaValidator().validate(descriptorProperties1);
		} catch (Exception e) {
			Optional<Throwable> throwable =
				ExceptionUtils.findThrowableWithMessage(e, "Starting from timestamp requires Kafka 0.10 above");
			assertTrue(throwable.isPresent());
		}

		// start from timestamp with duplicate timestamp properties
		final Map<String, String> props2 = new HashMap<>();
		props2.put("connector.property-version", "1");
		props2.put("connector.type", "kafka");
		props2.put("connector.version", "0.11");
		props2.put("connector.topic", "MyTopic");
		props2.put("connector.startup-mode", "timestamp");
		props2.put("connector.startup-timestamp", "2019-12-20 20:11:38.221");
		props2.put("connector.startup-timestamp-millis", "1577087582000");

		final DescriptorProperties descriptorProperties2 = new DescriptorProperties();
		descriptorProperties2.putProperties(props2);
		try {
			new KafkaValidator().validate(descriptorProperties2);
		} catch (Exception e) {
			Optional<Throwable> throwable =
				ExceptionUtils.findThrowableWithMessage(e, "One and only one of `connector.startup-" +
					"timestamp-millis` or `connector.startup-timestamp` should be provided.");
			assertTrue(throwable.isPresent());
		}

		// start from timestamp without timestamp property
		final Map<String, String> props3 = new HashMap<>();
		props3.put("connector.property-version", "1");
		props3.put("connector.type", "kafka");
		props3.put("connector.version", "0.10");
		props3.put("connector.topic", "MyTopic");
		props3.put("connector.startup-mode", "timestamp");

		final DescriptorProperties descriptorProperties3 = new DescriptorProperties();
		descriptorProperties3.putProperties(props3);
		try {
			new KafkaValidator().validate(descriptorProperties3);
		} catch (Exception e) {
			Optional<Throwable> throwable =
				ExceptionUtils.findThrowableWithMessage(e, "One and only one of `connector.startup-" +
					"timestamp-millis` or `connector.startup-timestamp` should be provided.");
			assertTrue(throwable.isPresent());
		}
	}
}
