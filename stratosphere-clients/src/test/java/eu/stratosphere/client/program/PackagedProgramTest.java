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
import java.net.URL;

import org.junit.Assert;
import org.junit.Test;


public class PackagedProgramTest {

	private static final String TEST_PROG_FILE_PATH = "/test.jar";
	
	@Test
	public void testGetPreviewPlan() {
		try {
			
			URL jarFileURL = getClass().getResource(TEST_PROG_FILE_PATH);
			
			PackagedProgram prog = new PackagedProgram(new File(jarFileURL.getFile()));
			Assert.assertNotNull(prog.getPreviewPlan());
		}
		catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			Assert.fail("Test is erroneous: " + e.getMessage());
		}
	}
}
