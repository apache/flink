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

import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.test.util.MiniClusterWithClientResource;

import org.junit.ClassRule;

/**
 * The base for the Kafka tests with Flink's MiniCluster.
 */
@SuppressWarnings("serial")
public abstract class KafkaTestBaseWithFlink extends KafkaTestBase {

	protected static final int NUM_TMS = 1;

	protected static final int TM_SLOTS = 8;

	@ClassRule
	public static MiniClusterWithClientResource flink = new MiniClusterWithClientResource(
		new MiniClusterResourceConfiguration.Builder()
			.setConfiguration(getFlinkConfiguration())
			.setNumberTaskManagers(NUM_TMS)
			.setNumberSlotsPerTaskManager(TM_SLOTS)
			.build());
}
