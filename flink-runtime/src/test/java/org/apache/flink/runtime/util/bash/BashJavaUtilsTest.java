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

package org.apache.flink.runtime.util.bash;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 * Tests for {@link BashJavaUtils}
 */
public class BashJavaUtilsTest extends TestLogger {
	@Test
	public void testJmLegacyHeapOptionSetsNewJvmHeap() {
		Configuration configuration = new Configuration();
		MemorySize heapSize = MemorySize.ofMebiBytes(10);
		configuration.set(JobManagerOptions.JOB_MANAGER_HEAP_MEMORY, heapSize);
		String jvmArgsLine = BashJavaUtils.getJmResourceParams(configuration).get(0);
		Map<String, String> jvmArgs = ConfigurationUtils.parseJvmArgString(jvmArgsLine);
		String heapSizeStr = Long.toString(heapSize.getBytes());
		assertThat(jvmArgs.get("-Xmx"), is(heapSizeStr));
		assertThat(jvmArgs.get("-Xms"), is(heapSizeStr));
	}
}
