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

package org.apache.flink.runtime.event.job;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.jobgraph.JobID;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.junit.Test;


public class EventsTest {

	@Test
	public void testEqualsHashCodeToString() {
		try {
			
			// ExecutionStateChangeEvent
			{
				JobVertexID jid = new JobVertexID();
				ExecutionAttemptID eid = new ExecutionAttemptID();
				
				ExecutionStateChangeEvent e1 = new ExecutionStateChangeEvent(429345231796596L, jid, 17, eid, ExecutionState.CANCELING);
				ExecutionStateChangeEvent e2 = new ExecutionStateChangeEvent(429345231796596L, jid, 17, eid, ExecutionState.CANCELING);
				
				assertTrue(e1.equals(e2));
				assertEquals(e1.hashCode(), e2.hashCode());
				e1.toString();
			}
			
			// JobEvent
			{
				JobEvent e1 = new JobEvent(429345231796596L, JobStatus.CANCELED, "testmessage");
				JobEvent e2 = new JobEvent(429345231796596L, JobStatus.CANCELED, "testmessage");
				JobEvent e3 = new JobEvent(237217579123412431L, JobStatus.RUNNING, null);
				JobEvent e4 = new JobEvent(237217579123412431L, JobStatus.RUNNING, null);
				
				assertTrue(e1.equals(e2));
				assertTrue(e3.equals(e4));
				
				assertEquals(e1.hashCode(), e2.hashCode());
				assertEquals(e3.hashCode(), e4.hashCode());
				e1.toString();
				e3.toString();
			}
			
			// RecentJobEvent
			{
				JobID jid = new JobID();
				RecentJobEvent e1 = new RecentJobEvent(jid, "some name", JobStatus.FAILED, false, 634563546, 546734734672572457L);
				RecentJobEvent e2 = new RecentJobEvent(jid, "some name", JobStatus.FAILED, false, 634563546, 546734734672572457L);
				RecentJobEvent e3 = new RecentJobEvent(jid, null, JobStatus.RUNNING, false, 42364716239476L, 127843618273607L);
				RecentJobEvent e4 = new RecentJobEvent(jid, null, JobStatus.RUNNING, false, 42364716239476L, 127843618273607L);
				
				assertTrue(e1.equals(e2));
				assertTrue(e3.equals(e4));
				
				assertEquals(e1.hashCode(), e2.hashCode());
				assertEquals(e3.hashCode(), e4.hashCode());
				e1.toString();
				e3.toString();
			}
			
			// VertexEvent
			{
				JobVertexID jid = new JobVertexID();
				ExecutionAttemptID eid = new ExecutionAttemptID();
				
				VertexEvent e1 = new VertexEvent(64619276017401234L, jid, "peter", 44, 13, eid, ExecutionState.DEPLOYING, "foo");
				VertexEvent e2 = new VertexEvent(64619276017401234L, jid, "peter", 44, 13, eid, ExecutionState.DEPLOYING, "foo");
				
				VertexEvent e3 = new VertexEvent(64619276017401234L, jid, null, 44, 13, eid, ExecutionState.DEPLOYING, null);
				VertexEvent e4 = new VertexEvent(64619276017401234L, jid, null, 44, 13, eid, ExecutionState.DEPLOYING, null);
				
				assertTrue(e1.equals(e2));
				assertTrue(e3.equals(e4));
				
				assertFalse(e1.equals(e3));
				
				assertEquals(e1.hashCode(), e2.hashCode());
				assertEquals(e3.hashCode(), e4.hashCode());
				e1.toString();
				e3.toString();
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testSerialization() {
		try {
			JobID jid = new JobID();
			JobVertexID vid = new JobVertexID();
			ExecutionAttemptID eid = new ExecutionAttemptID();
			
			ExecutionStateChangeEvent esce = new ExecutionStateChangeEvent(429345231796596L, vid, 17, eid, ExecutionState.CANCELING);

			JobEvent je1 = new JobEvent(429345231796596L, JobStatus.CANCELED, "testmessage");
			JobEvent je2 = new JobEvent(237217579123412431L, JobStatus.RUNNING, null);

			RecentJobEvent rce1 = new RecentJobEvent(jid, "some name", JobStatus.FAILED, false, 634563546, 546734734672572457L);
			RecentJobEvent rce2 = new RecentJobEvent(jid, null, JobStatus.RUNNING, false, 42364716239476L, 127843618273607L);

			VertexEvent ve1 = new VertexEvent(64619276017401234L, vid, "peter", 44, 13, eid, ExecutionState.DEPLOYING, "foo");
			VertexEvent ve2 = new VertexEvent(64619276017401234L, vid, null, 44, 13, eid, ExecutionState.DEPLOYING, null);
			
			assertEquals(esce, CommonTestUtils.createCopyWritable(esce));
			assertEquals(je1, CommonTestUtils.createCopyWritable(je1));
			assertEquals(je2, CommonTestUtils.createCopyWritable(je2));
			assertEquals(rce1, CommonTestUtils.createCopyWritable(rce1));
			assertEquals(rce2, CommonTestUtils.createCopyWritable(rce2));
			assertEquals(ve1, CommonTestUtils.createCopyWritable(ve1));
			assertEquals(ve2, CommonTestUtils.createCopyWritable(ve2));
			
			assertEquals(esce, CommonTestUtils.createCopySerializable(esce));
			assertEquals(je1, CommonTestUtils.createCopySerializable(je1));
			assertEquals(je2, CommonTestUtils.createCopySerializable(je2));
			assertEquals(rce1, CommonTestUtils.createCopySerializable(rce1));
			assertEquals(rce2, CommonTestUtils.createCopySerializable(rce2));
			assertEquals(ve1, CommonTestUtils.createCopySerializable(ve1));
			assertEquals(ve2, CommonTestUtils.createCopySerializable(ve2));
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
}
