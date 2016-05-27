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
package org.apache.flink.metrics.groups;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.MetricRegistry;
import org.apache.flink.util.AbstractID;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

public class TaskManagerGroupTest {

	// ------------------------------------------------------------------------
	//  adding and removing jobs
	// ------------------------------------------------------------------------

	@Test
	public void addAndRemoveJobs() {
		final TaskManagerMetricGroup group = new TaskManagerMetricGroup(
				new MetricRegistry(new Configuration()), "localhost", new AbstractID().toString());
		
		
		final JobID jid1 = new JobID();
		final JobID jid2 = new JobID();
		
		final String jobName1 = "testjob";
		final String jobName2 = "anotherJob";
		
		final AbstractID vertex11 = new AbstractID();
		final AbstractID vertex12 = new AbstractID();
		final AbstractID vertex13 = new AbstractID();
		final AbstractID vertex21 = new AbstractID();

		final AbstractID execution11 = new AbstractID();
		final AbstractID execution12 = new AbstractID();
		final AbstractID execution13 = new AbstractID();
		final AbstractID execution21 = new AbstractID();
		
		TaskMetricGroup tmGroup11 = group.addTaskForJob(jid1, jobName1, vertex11, execution11, 17, "test");
		TaskMetricGroup tmGroup12 = group.addTaskForJob(jid1, jobName1, vertex12, execution12, 13, "test");
		TaskMetricGroup tmGroup21 = group.addTaskForJob(jid2, jobName2, vertex21, execution21, 7, "test");
		
		assertEquals(2, group.numRegisteredJobMetricGroups());
		assertFalse(tmGroup11.parent().isClosed());
		assertFalse(tmGroup12.parent().isClosed());
		assertFalse(tmGroup21.parent().isClosed());
		
		// close all for job 2 and one from job 1
		tmGroup11.close();
		tmGroup21.close();
		assertTrue(tmGroup11.isClosed());
		assertTrue(tmGroup21.isClosed());
		
		// job 2 should be removed, job should still be there
		assertFalse(tmGroup11.parent().isClosed());
		assertFalse(tmGroup12.parent().isClosed());
		assertTrue(tmGroup21.parent().isClosed());
		assertEquals(1, group.numRegisteredJobMetricGroups());
		
		// add one more to job one
		TaskMetricGroup tmGroup13 = group.addTaskForJob(jid1, jobName1, vertex13, execution13, 0, "test");
		tmGroup12.close();
		tmGroup13.close();

		assertTrue(tmGroup11.parent().isClosed());
		assertTrue(tmGroup12.parent().isClosed());
		assertTrue(tmGroup13.parent().isClosed());
		
		assertEquals(0, group.numRegisteredJobMetricGroups());
	}

	@Test
	public void testCloseClosesAll() {
		final TaskManagerMetricGroup group = new TaskManagerMetricGroup(
				new MetricRegistry(new Configuration()), "localhost", new AbstractID().toString());


		final JobID jid1 = new JobID();
		final JobID jid2 = new JobID();

		final String jobName1 = "testjob";
		final String jobName2 = "anotherJob";

		final AbstractID vertex11 = new AbstractID();
		final AbstractID vertex12 = new AbstractID();
		final AbstractID vertex21 = new AbstractID();

		final AbstractID execution11 = new AbstractID();
		final AbstractID execution12 = new AbstractID();
		final AbstractID execution21 = new AbstractID();

		TaskMetricGroup tmGroup11 = group.addTaskForJob(jid1, jobName1, vertex11, execution11, 17, "test");
		TaskMetricGroup tmGroup12 = group.addTaskForJob(jid1, jobName1, vertex12, execution12, 13, "test");
		TaskMetricGroup tmGroup21 = group.addTaskForJob(jid2, jobName2, vertex21, execution21, 7, "test");
		
		group.close();
		
		assertTrue(tmGroup11.isClosed());
		assertTrue(tmGroup12.isClosed());
		assertTrue(tmGroup21.isClosed());
	}
	
	// ------------------------------------------------------------------------
	//  scope tests
	// ------------------------------------------------------------------------

	@Test
	public void testGenerateScopeDefault() {
		MetricRegistry registry = new MetricRegistry(new Configuration());
		TaskManagerMetricGroup operator = new TaskManagerMetricGroup(registry, "host", "id");

		List<String> scope = operator.generateScope();
		assertEquals(3, scope.size());
		assertEquals("host", scope.get(0));
		assertEquals("taskmanager", scope.get(1));
		assertEquals("id", scope.get(2));
	}

	@Test
	public void testGenerateScopeWildcard() {
		MetricRegistry registry = new MetricRegistry(new Configuration());
		TaskManagerMetricGroup operator = new TaskManagerMetricGroup(registry, "host", "id");

		Scope.ScopeFormat format = new Scope.ScopeFormat();
		format.setTaskManagerFormat(Scope.concat(Scope.SCOPE_WILDCARD, "superhost", TaskManagerMetricGroup.SCOPE_TM_HOST));

		List<String> scope = operator.generateScope(format);
		assertEquals(2, scope.size());
		assertEquals("superhost", scope.get(0));
		assertEquals("host", scope.get(1));
	}

	@Test
	public void testGenerateScopeCustom() {
		MetricRegistry registry = new MetricRegistry(new Configuration());
		TaskManagerMetricGroup operator = new TaskManagerMetricGroup(registry, "host", "id");

		Scope.ScopeFormat format = new Scope.ScopeFormat();
		format.setTaskManagerFormat(Scope.concat("h", TaskManagerMetricGroup.SCOPE_TM_HOST, "t", TaskManagerMetricGroup.SCOPE_TM_ID));

		List<String> scope = operator.generateScope(format);
		assertEquals(4, scope.size());
		assertEquals("h", scope.get(0));
		assertEquals("host", scope.get(1));
		assertEquals("t", scope.get(2));
		assertEquals("id", scope.get(3));
	}
}
