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

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Test case for the {@link KafkaResourceFactory}.
 */
public class TestKafkaResourceFactory {

	private static final String E2E_KAFKA_URL = "";
	private static final String E2E_KAFKA_PKG_NAME = "";
	private static final Path E2E_KAFKA_TEST_DATA_DIR = Paths.get("");

	@After
	public void tearDown() {
		System.clearProperty(KafkaResourceFactory.E2E_KAFKA_MODE);
	}

	@Test
	public void testCreateKafkaLocalStandaloneResource() {
		System.setProperty(KafkaResourceFactory.E2E_KAFKA_MODE, KafkaResourceFactory.E2E_KAFKA_MODE_LOCAL_STANDALONE);
		KafkaResource resource = KafkaResourceFactory.create(E2E_KAFKA_URL, E2E_KAFKA_PKG_NAME, E2E_KAFKA_TEST_DATA_DIR);
		Assert.assertTrue(resource instanceof LocalStandaloneKafkaResource);
	}

	@Test
	public void testCreateKafkaDistributedResource() {
		System.setProperty(KafkaResourceFactory.E2E_KAFKA_MODE, KafkaResourceFactory.E2E_KAFKA_MODE_DISTRIBUTED);
		KafkaResource resource = KafkaResourceFactory.create(E2E_KAFKA_URL, E2E_KAFKA_PKG_NAME, E2E_KAFKA_TEST_DATA_DIR);
		Assert.assertTrue(resource instanceof DistributionKafkaResource);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testInvalidKafkaMode() {
		System.setProperty(KafkaResourceFactory.E2E_KAFKA_MODE, "InvalidMode");
		KafkaResourceFactory.create(E2E_KAFKA_URL, E2E_KAFKA_PKG_NAME, E2E_KAFKA_TEST_DATA_DIR);
	}
}
