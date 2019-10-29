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

package org.apache.flink.kubernetes;

import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.util.TestLogger;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * Tests for {@link KubernetesWorkerNode}.
 */
public class KubernetesWorkerNodeTest extends TestLogger {

	@Rule
	public final ExpectedException exception = ExpectedException.none();

	@Test
	public void testGetAttemptFromWorkerNode() throws Exception {
		final String correctPodName = "flink-on-kubernetes-taskmanager-11-5";
		final KubernetesWorkerNode workerNode = new KubernetesWorkerNode(new ResourceID(correctPodName));
		Assert.assertEquals(11L, workerNode.getAttempt());

		final String wrongPodName = "flink-on-kubernetes-xxxtaskmanager-11-5";
		final KubernetesWorkerNode workerNode1 = new KubernetesWorkerNode(new ResourceID(wrongPodName));
		exception.expect(Exception.class);
		exception.expectMessage("Error to parse KubernetesWorkerNode from " + wrongPodName + ".");
		workerNode1.getAttempt();
	}
}
