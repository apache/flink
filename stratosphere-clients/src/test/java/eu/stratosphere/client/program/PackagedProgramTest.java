/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/
package eu.stratosphere.client.program;

import java.io.File;

import org.junit.Assert;
import org.junit.Test;

import eu.stratosphere.client.CliFrontendTestUtils;


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
