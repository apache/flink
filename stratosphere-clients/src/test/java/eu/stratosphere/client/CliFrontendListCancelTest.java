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

package eu.stratosphere.client;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import eu.stratosphere.core.io.StringRecord;
import eu.stratosphere.nephele.client.JobCancelResult;
import eu.stratosphere.nephele.client.JobProgressResult;
import eu.stratosphere.nephele.client.JobSubmissionResult;
import eu.stratosphere.nephele.event.job.AbstractEvent;
import eu.stratosphere.nephele.event.job.RecentJobEvent;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.managementgraph.ManagementGraph;
import eu.stratosphere.nephele.managementgraph.ManagementVertexID;
import eu.stratosphere.nephele.protocols.ExtendedManagementProtocol;
import eu.stratosphere.nephele.topology.NetworkTopology;
import eu.stratosphere.nephele.types.IntegerRecord;

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
				InfoListTestCliFrontend testFrontend = new InfoListTestCliFrontend(new TestProtocol(jid));
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
				InfoListTestCliFrontend testFrontend = new InfoListTestCliFrontend(new TestProtocol());
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
		
		private final ExtendedManagementProtocol protocol;
		
		public InfoListTestCliFrontend(ExtendedManagementProtocol protocol) {
			this.protocol = protocol;
		}

		@Override
		protected ExtendedManagementProtocol getJobManagerConnection(CommandLine line) {
			return this.protocol;
		}
	}

	protected static final class TestProtocol implements ExtendedManagementProtocol {
		
		private final JobID expectedCancelId;
		
		public TestProtocol() {
			this.expectedCancelId = null;
		}
		
		public TestProtocol(JobID expectedCancelId) {
			this.expectedCancelId = expectedCancelId;
		}

		@Override
		public JobSubmissionResult submitJob(JobGraph job) throws IOException {
			throw new UnsupportedOperationException();
		}

		@Override
		public JobProgressResult getJobProgress(JobID jobID) throws IOException {
			throw new UnsupportedOperationException();
		}

		@Override
		public JobCancelResult cancelJob(JobID jobID) throws IOException {
			if (this.expectedCancelId == null) {
				throw new UnsupportedOperationException();
			} else {
				Assert.assertEquals(expectedCancelId, jobID);
				return null;
			}
		}

		@Override
		public IntegerRecord getRecommendedPollingInterval() throws IOException {
			throw new UnsupportedOperationException();
		}

		@Override
		public ManagementGraph getManagementGraph(JobID jobID) throws IOException {
			throw new UnsupportedOperationException();
		}

		@Override
		public List<RecentJobEvent> getRecentJobs() throws IOException {
			return new ArrayList<RecentJobEvent>();
		}

		@Override
		public List<AbstractEvent> getEvents(JobID jobID) throws IOException {
			throw new UnsupportedOperationException();
		}

		@Override
		public void killTask(JobID jobID, ManagementVertexID id) throws IOException {
			throw new UnsupportedOperationException();
		}

		@Override
		public void killInstance(StringRecord instanceName) throws IOException {
			throw new UnsupportedOperationException();
		}

		@Override
		public void logBufferUtilization(JobID jobID) throws IOException {
			throw new UnsupportedOperationException();
		}

		@Override
		public NetworkTopology getNetworkTopology(JobID jobID) throws IOException {
			throw new UnsupportedOperationException();
		}

		@Override
		public int getAvailableSlots() {
			return 1;
		}
	}
}
