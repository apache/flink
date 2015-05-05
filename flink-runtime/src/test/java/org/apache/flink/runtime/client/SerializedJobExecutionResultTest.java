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

package org.apache.flink.runtime.client;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.runtime.util.SerializedValue;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

/**
 * Tests for the SerializedJobExecutionResult
 */
public class SerializedJobExecutionResultTest {

	@Test
	public void testSerialization() {
		try {
			final ClassLoader classloader = getClass().getClassLoader();

			JobID origJobId = new JobID();
			long origTime = 65927436589267L;

			Map<String, SerializedValue<Object>> origMap = new HashMap<String, SerializedValue<Object>>();
			origMap.put("name1", new SerializedValue<Object>(723L));
			origMap.put("name2", new SerializedValue<Object>("peter"));

			SerializedJobExecutionResult result = new SerializedJobExecutionResult(origJobId, origTime, origMap);

			// serialize and deserialize the object
			SerializedJobExecutionResult cloned = CommonTestUtils.createCopySerializable(result);

			assertEquals(origJobId, cloned.getJobId());
			assertEquals(origTime, cloned.getNetRuntime());
			assertEquals(origTime, cloned.getNetRuntime(TimeUnit.MILLISECONDS));
			assertEquals(origMap, cloned.getSerializedAccumulatorResults());

			// convert to deserialized result
			JobExecutionResult jResult = result.toJobExecutionResult(classloader);
			JobExecutionResult jResultCopied = result.toJobExecutionResult(classloader);

			assertEquals(origJobId, jResult.getJobID());
			assertEquals(origJobId, jResultCopied.getJobID());
			assertEquals(origTime, jResult.getNetRuntime());
			assertEquals(origTime, jResult.getNetRuntime(TimeUnit.MILLISECONDS));
			assertEquals(origTime, jResultCopied.getNetRuntime());
			assertEquals(origTime, jResultCopied.getNetRuntime(TimeUnit.MILLISECONDS));

			for (Map.Entry<String, SerializedValue<Object>> entry : origMap.entrySet()) {
				String name = entry.getKey();
				Object value = entry.getValue().deserializeValue(classloader);
				assertEquals(value, jResult.getAccumulatorResult(name));
				assertEquals(value, jResultCopied.getAccumulatorResult(name));
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testSerializationWithNullValues() {
		try {
			SerializedJobExecutionResult result = new SerializedJobExecutionResult(null, 0L, null);
			SerializedJobExecutionResult cloned = CommonTestUtils.createCopySerializable(result);

			assertNull(cloned.getJobId());
			assertEquals(0L, cloned.getNetRuntime());
			assertNull(cloned.getSerializedAccumulatorResults());

			JobExecutionResult jResult = result.toJobExecutionResult(getClass().getClassLoader());
			assertNull(jResult.getJobID());
			assertNull(jResult.getAllAccumulatorResults());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
}
