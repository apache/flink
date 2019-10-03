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

package org.apache.flink.runtime.jobmaster;

import org.junit.Test;

import static junit.framework.TestCase.assertEquals;

public class JobExecutionHistoryTest {

	@Test
	public void testJobCompletion() throws Exception {
		String[] jvmOptions = {
			"-Xmx424m",
			"-Dlog.file=/data/nvme1n1/userlogs/application_1560449379756_1548/container_e02_1560449379756_1548_01_000001/jobmanager.log",
			"-Dlogback.configurationFile=file:logback.xml",
			"-Dlog4j.configuration=file:log4j.properties"
		};
		String dirPath = JobExecutionHistory.getFlinkConfDirectory(jvmOptions);

		String expectedPath= "/data/nvme1n1/nm-local-dir/usercache/" + System.getProperty("user.name")
			+ "/appcache/application_1560449379756_1548/container_e02_1560449379756_1548_01_000001";
		assertEquals(dirPath, expectedPath);
	}
}
