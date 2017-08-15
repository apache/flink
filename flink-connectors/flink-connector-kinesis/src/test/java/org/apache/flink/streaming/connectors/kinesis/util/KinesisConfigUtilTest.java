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

package org.apache.flink.streaming.connectors.kinesis.util;

import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer;
import org.apache.flink.streaming.connectors.kinesis.config.AWSConfigConstants;
import org.apache.flink.streaming.connectors.kinesis.config.ProducerConfigConstants;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.Properties;

import static org.junit.Assert.assertEquals;

/**
 * Tests for KinesisConfigUtil.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({FlinkKinesisConsumer.class, KinesisConfigUtil.class})
public class KinesisConfigUtilTest {
	@Rule
	private ExpectedException exception = ExpectedException.none();

	@Test
	public void testUnparsableLongForProducerConfiguration() {
		exception.expect(IllegalArgumentException.class);
		exception.expectMessage("Error trying to set field RateLimit with the value 'unparsableLong'");

		Properties testConfig = new Properties();
		testConfig.setProperty(AWSConfigConstants.AWS_REGION, "us-east-1");
		testConfig.setProperty("RateLimit", "unparsableLong");

		KinesisConfigUtil.validateProducerConfiguration(testConfig);
	}

	@Test
	public void testReplaceDeprecatedKeys() {
		Properties testConfig = new Properties();
		testConfig.setProperty(AWSConfigConstants.AWS_REGION, "us-east-1");
		// these deprecated keys should be replaced
		testConfig.setProperty(ProducerConfigConstants.AGGREGATION_MAX_COUNT, "1");
		testConfig.setProperty(ProducerConfigConstants.COLLECTION_MAX_COUNT, "2");
		Properties replacedConfig = KinesisConfigUtil.replaceDeprecatedProducerKeys(testConfig);

		assertEquals("1", replacedConfig.getProperty(KinesisConfigUtil.AGGREGATION_MAX_COUNT));
		assertEquals("2", replacedConfig.getProperty(KinesisConfigUtil.COLLECTION_MAX_COUNT));
	}
}
