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

package org.apache.flink.kubernetes.configuration;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Test class for KubernetesConfigOptions.
 */
public class KubernetesConfigOptionsTest {

	@Test
	public void testServiceExposedTypeFromString() {

		assertEquals(KubernetesConfigOptions.ServiceExposedType.CLUSTER_IP,
			KubernetesConfigOptions.ServiceExposedType.fromString("ClusterIp"));
		assertEquals(KubernetesConfigOptions.ServiceExposedType.NODE_PORT,
			KubernetesConfigOptions.ServiceExposedType.fromString("NodePort"));
		assertEquals(KubernetesConfigOptions.ServiceExposedType.LOAD_BALANCER,
			KubernetesConfigOptions.ServiceExposedType.fromString("LoadBalancer"));
		assertEquals(KubernetesConfigOptions.ServiceExposedType.EXTERNAL_NAME,
			KubernetesConfigOptions.ServiceExposedType.fromString("ExternalName"));

		// unknown exposed type, will return default
		assertEquals(KubernetesConfigOptions.ServiceExposedType.CLUSTER_IP,
			KubernetesConfigOptions.ServiceExposedType.fromString("TestType"));
	}

	@Test
	public void testGetServiceExposedType() {
		assertEquals("ClusterIP",
			KubernetesConfigOptions.ServiceExposedType.CLUSTER_IP.getServiceExposedType());
		assertEquals("NodePort",
			KubernetesConfigOptions.ServiceExposedType.NODE_PORT.getServiceExposedType());
		assertEquals("LoadBalancer",
			KubernetesConfigOptions.ServiceExposedType.LOAD_BALANCER.getServiceExposedType());
		assertEquals("ExternalName",
			KubernetesConfigOptions.ServiceExposedType.EXTERNAL_NAME.getServiceExposedType());
	}
}
