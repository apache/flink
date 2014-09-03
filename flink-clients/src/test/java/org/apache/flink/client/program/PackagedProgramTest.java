/**
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

package org.apache.flink.client.program;

import java.io.File;

import org.apache.flink.client.CliFrontendTestUtils;
import org.apache.flink.client.program.PackagedProgram;
import org.junit.Assert;
import org.junit.Test;


public class PackagedProgramTest {

	@Test
	public void testGetPreviewPlan() {
		try {
			
			PackagedProgram prog = new PackagedProgram(new File(CliFrontendTestUtils.getTestJarPath()));
			Assert.assertNotNull(prog.getPreviewPlan());
		}
		catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			Assert.fail("Test is erroneous: " + e.getMessage());
		}
	}
}
