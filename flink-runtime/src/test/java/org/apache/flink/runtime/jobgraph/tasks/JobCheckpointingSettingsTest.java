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

package org.apache.flink.runtime.jobgraph.tasks;

import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class JobCheckpointingSettingsTest {

	/**
	 * Tests that the settings are actually serializable.
	 */
	@Test
	public void testIsJavaSerializable() throws Exception {
		JobCheckpointingSettings settings = new JobCheckpointingSettings(
			Arrays.asList(new JobVertexID(), new JobVertexID()),
			Arrays.asList(new JobVertexID(), new JobVertexID()),
			Arrays.asList(new JobVertexID(), new JobVertexID()),
			1231231,
			1231,
			112,
			12,
			ExternalizedCheckpointSettings.externalizeCheckpoints(true),
			new MemoryStateBackend(),
			false);

		JobCheckpointingSettings copy = CommonTestUtils.createCopySerializable(settings);
		assertEquals(settings.getVerticesToAcknowledge(), copy.getVerticesToAcknowledge());
		assertEquals(settings.getVerticesToConfirm(), copy.getVerticesToConfirm());
		assertEquals(settings.getVerticesToTrigger(), copy.getVerticesToTrigger());
		assertEquals(settings.getCheckpointInterval(), copy.getCheckpointInterval());
		assertEquals(settings.getCheckpointTimeout(), copy.getCheckpointTimeout());
		assertEquals(settings.getMinPauseBetweenCheckpoints(), copy.getMinPauseBetweenCheckpoints());
		assertEquals(settings.getMaxConcurrentCheckpoints(), copy.getMaxConcurrentCheckpoints());
		assertEquals(settings.getExternalizedCheckpointSettings().externalizeCheckpoints(), copy.getExternalizedCheckpointSettings().externalizeCheckpoints());
		assertEquals(settings.getExternalizedCheckpointSettings().deleteOnCancellation(), copy.getExternalizedCheckpointSettings().deleteOnCancellation());
		assertEquals(settings.isExactlyOnce(), copy.isExactlyOnce());
		assertNotNull(copy.getDefaultStateBackend());
		assertTrue(copy.getDefaultStateBackend().getClass() == MemoryStateBackend.class);
	}
}
