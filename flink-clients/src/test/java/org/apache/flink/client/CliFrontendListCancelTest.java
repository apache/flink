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

package org.apache.flink.client;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.flink.runtime.jobgraph.JobID;
import org.junit.BeforeClass;
import org.junit.Test;

//TODO: Update test case
public class CliFrontendListCancelTest {
	
	@BeforeClass
	public static void init() {
		CliFrontendTestUtils.pipeSystemOutToNull();
		CliFrontendTestUtils.clearGlobalConfiguration();
	}
	
	@Test
	public void testCancel() {
		try {
			// test unrecognized option
			{
				String[] parameters = {"-v", "-l"};
				CliFrontend testFrontend = new CliFrontend();
				int retCode = testFrontend.cancel(parameters);
				assertTrue(retCode == 2);
			}
			
			// test missing job id
			{
				String[] parameters = {};
				CliFrontend testFrontend = new CliFrontend();
				int retCode = testFrontend.cancel(parameters);
				assertTrue(retCode != 0);
			}
			
			// test cancel properly
			{
				JobID jid = new JobID();
				String jidString = jid.toString();
				
				String[] parameters = {"-i", jidString};
				InfoListTestCliFrontend testFrontend = new InfoListTestCliFrontend();
				int retCode = testFrontend.cancel(parameters);
				assertTrue(retCode == 0);
			}
		}
		catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			fail("Program caused an exception: " + e.getMessage());
		}
	}
	
	
	@Test
	public void testList() {
		try {
			// test unrecognized option
			{
				String[] parameters = {"-v", "-k"};
				CliFrontend testFrontend = new CliFrontend();
				int retCode = testFrontend.list(parameters);
				assertTrue(retCode == 2);
			}
			
			// test missing flags
			{
				String[] parameters = {};
				CliFrontend testFrontend = new CliFrontend();
				int retCode = testFrontend.list(parameters);
				assertTrue(retCode != 0);
			}
			
			// test list properly
			{
				String[] parameters = {"-r", "-s"};
				InfoListTestCliFrontend testFrontend = new InfoListTestCliFrontend();
				int retCode = testFrontend.list(parameters);
				assertTrue(retCode == 0);
			}
		}
		catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			fail("Program caused an exception: " + e.getMessage());
		}
	}


	protected static final class InfoListTestCliFrontend extends CliFrontendTestUtils.TestingCliFrontend {

		public InfoListTestCliFrontend() {
		}

		public int getTotalNumberOfRegisteredSlots() {
			return 1;
		}

		@Override
		public int getNumberOfSlotsAvailableToScheduler() throws IOException {

		@Override
		public int getBlobServerPort() {
			throw new UnsupportedOperationException();
		}
	}
}
