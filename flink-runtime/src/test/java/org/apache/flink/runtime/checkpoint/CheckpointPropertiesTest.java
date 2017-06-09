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

package org.apache.flink.runtime.checkpoint;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests for the default checkpoint properties.
 */
public class CheckpointPropertiesTest {

	/**
	 * Tests the default checkpoint properties.
	 */
	@Test
	public void testCheckpointProperties() {
		CheckpointProperties props = CheckpointProperties.forStandardCheckpoint();

		assertFalse(props.forceCheckpoint());
		assertFalse(props.externalizeCheckpoint());
		assertTrue(props.discardOnSubsumed());
		assertTrue(props.discardOnJobFinished());
		assertTrue(props.discardOnJobCancelled());
		assertTrue(props.discardOnJobFailed());
		assertTrue(props.discardOnJobSuspended());
	}

	/**
	 * Tests the external checkpoints properties.
	 */
	@Test
	public void testExternalizedCheckpointProperties() {
		CheckpointProperties props = CheckpointProperties.forExternalizedCheckpoint(true);

		assertFalse(props.forceCheckpoint());
		assertTrue(props.externalizeCheckpoint());
		assertTrue(props.discardOnSubsumed());
		assertTrue(props.discardOnJobFinished());
		assertTrue(props.discardOnJobCancelled());
		assertFalse(props.discardOnJobFailed());
		assertTrue(props.discardOnJobSuspended());

		props = CheckpointProperties.forExternalizedCheckpoint(false);

		assertFalse(props.forceCheckpoint());
		assertTrue(props.externalizeCheckpoint());
		assertTrue(props.discardOnSubsumed());
		assertTrue(props.discardOnJobFinished());
		assertFalse(props.discardOnJobCancelled());
		assertFalse(props.discardOnJobFailed());
		assertFalse(props.discardOnJobSuspended());
	}

	/**
	 * Tests the default (manually triggered) savepoint properties.
	 */
	@Test
	public void testSavepointProperties() {
		CheckpointProperties props = CheckpointProperties.forStandardSavepoint();

		assertTrue(props.forceCheckpoint());
		assertTrue(props.externalizeCheckpoint());
		assertFalse(props.discardOnSubsumed());
		assertFalse(props.discardOnJobFinished());
		assertFalse(props.discardOnJobCancelled());
		assertFalse(props.discardOnJobFailed());
		assertFalse(props.discardOnJobSuspended());
	}

	/**
	 * Tests the isSavepoint utility works as expected.
	 */
	@Test
	public void testIsSavepoint() throws Exception {
		{
			CheckpointProperties props = CheckpointProperties.forStandardCheckpoint();
			assertFalse(props.isSavepoint());
		}

		{
			CheckpointProperties props = CheckpointProperties.forExternalizedCheckpoint(true);
			assertFalse(props.isSavepoint());
		}

		{
			CheckpointProperties props = CheckpointProperties.forExternalizedCheckpoint(false);
			assertFalse(props.isSavepoint());
		}

		{
			CheckpointProperties props = CheckpointProperties.forStandardSavepoint();
			assertTrue(props.isSavepoint());
		}

	}
}
