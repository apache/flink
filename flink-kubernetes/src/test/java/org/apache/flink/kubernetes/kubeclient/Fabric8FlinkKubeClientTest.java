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

package org.apache.flink.kubernetes.kubeclient;

import org.apache.flink.kubernetes.KubernetesTestBase;

import org.junit.Before;
import org.junit.Test;

/**
 * Tests for Fabric implementation of {@link FlinkKubeClient}.
 */
public class Fabric8FlinkKubeClientTest extends KubernetesTestBase {

	@Before
	public void setup() throws Exception {
		super.setup();
	}

	@Test
	public void testCreateTaskManagerPod() {
		// todo
	}

	@Test
	public void testServiceLoadBalancerWithNoIP() throws Exception {
		// todo
	}

	@Test
	public void testServiceLoadBalancerEmptyHostAndIP() throws Exception {
		// todo
	}
}
