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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.apache.flink.runtime.checkpoint.CheckpointOptions.CheckpointType;
import org.junit.Test;

public class CheckpointOptionsTest {

	@Test
	public void testFullCheckpoint() throws Exception {
		CheckpointOptions options = CheckpointOptions.forFullCheckpoint();
		assertEquals(CheckpointType.FULL_CHECKPOINT, options.getCheckpointType());
		assertNull(options.getTargetLocation());
	}

	@Test
	public void testSavepoint() throws Exception {
		String location = "asdasdadasdasdja7931481398123123123kjhasdkajsd";
		CheckpointOptions options = CheckpointOptions.forSavepoint(location);
		assertEquals(CheckpointType.SAVEPOINT, options.getCheckpointType());
		assertEquals(location, options.getTargetLocation());
	}

	@Test(expected = NullPointerException.class)
	public void testSavepointNullCheck() throws Exception {
		CheckpointOptions.forSavepoint(null);
	}
}
