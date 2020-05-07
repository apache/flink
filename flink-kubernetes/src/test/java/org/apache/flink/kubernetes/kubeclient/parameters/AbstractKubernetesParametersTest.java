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

package org.apache.flink.kubernetes.kubeclient.parameters;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.utils.Constants;
import org.apache.flink.util.StringUtils;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.apache.flink.core.testutils.CommonTestUtils.assertThrows;

/**
 * General tests for the {@link AbstractKubernetesParameters}.
 */
public class AbstractKubernetesParametersTest extends TestLogger {

	private final Configuration flinkConfig = new Configuration();
	private final TestingKubernetesParameters testingKubernetesParameters = new TestingKubernetesParameters(flinkConfig);

	@Test
	public void testClusterIdMustNotBeBlank() {
		flinkConfig.set(KubernetesConfigOptions.CLUSTER_ID, "  ");
		assertThrows(
			"must not be blank",
			IllegalArgumentException.class,
			testingKubernetesParameters::getClusterId
		);
	}

	@Test
	public void testClusterIdLengthLimitation() {
		final String stringWithIllegalLength =
			StringUtils.generateRandomAlphanumericString(new Random(), Constants.MAXIMUM_CHARACTERS_OF_CLUSTER_ID + 1);
		flinkConfig.set(KubernetesConfigOptions.CLUSTER_ID, stringWithIllegalLength);
		assertThrows(
			"must be no more than " + Constants.MAXIMUM_CHARACTERS_OF_CLUSTER_ID + " characters",
			IllegalArgumentException.class,
			testingKubernetesParameters::getClusterId
		);
	}

	private class TestingKubernetesParameters extends AbstractKubernetesParameters {

		public TestingKubernetesParameters(Configuration flinkConfig) {
			super(flinkConfig);
		}

		@Override
		public Map<String, String> getLabels() {
			throw new UnsupportedOperationException("NOT supported");
		}

		@Override
		public Map<String, String> getNodeSelector() {
			throw new UnsupportedOperationException("NOT supported");
		}

		@Override
		public Map<String, String> getEnvironments() {
			throw new UnsupportedOperationException("NOT supported");
		}

		@Override
		public Map<String, String> getAnnotations() {
			throw new UnsupportedOperationException("NOT supported");
		}

		@Override
		public List<Map<String, String>> getTolerations() {
			throw new UnsupportedOperationException("NOT supported");
		}
	}
}
