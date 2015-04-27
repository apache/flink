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

package org.apache.flink.runtime.jobgraph;

import org.apache.flink.api.common.JobID;
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.fail;
import static org.junit.Assert.assertEquals;

public class JobIdTest {

	@Test
	public void testConvertToByteBuffer() {
		try {
			JobID origID = new JobID();

			byte[] bytes = origID.getBytes();
			ByteBuffer buffer = ByteBuffer.wrap(bytes);

			JobID copy1 = JobID.fromByteBuffer(buffer);
			JobID copy2 = JobID.fromByteArray(bytes);

			assertEquals(origID, copy1);
			assertEquals(origID, copy2);
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
}
