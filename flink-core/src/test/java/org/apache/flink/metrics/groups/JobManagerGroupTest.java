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
import org.apache.flink.metrics.groups.scope.ScopeFormat.JobManagerScopeFormat;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class JobManagerGroupTest {

	// ------------------------------------------------------------------------
	//  adding and removing jobs
	// ------------------------------------------------------------------------

	@Test
	public void addAndRemoveJobs() {
		final JobManagerMetricGroup group = new JobManagerMetricGroup(
			new MetricRegistry(new Configuration()), "localhost");

		final JobID jid1 = new JobID();
		final JobID jid2 = new JobID();

		final String jobName1 = "testjob";
		final String jobName2 = "anotherJob";

		JobManagerJobMetricGroup jmJobGroup11 = group.addJob(jid1, jobName1);
		JobManagerJobMetricGroup jmJobGroup12 = group.addJob(jid1, jobName1);
		JobManagerJobMetricGroup jmJobGroup21 = group.addJob(jid2, jobName2);

		assertEquals(jmJobGroup11, jmJobGroup12);

		assertEquals(2, group.numRegisteredJobMetricGroups());

		group.removeJob(jid1);

		assertTrue(jmJobGroup11.isClosed());
		assertEquals(1, group.numRegisteredJobMetricGroups());

		group.removeJob(jid2);

		assertTrue(jmJobGroup21.isClosed());
		assertEquals(0, group.numRegisteredJobMetricGroups());
	}

	@Test
	public void testCloseClosesAll() {
		final JobManagerMetricGroup group = new JobManagerMetricGroup(
			new MetricRegistry(new Configuration()), "localhost");

		final JobID jid1 = new JobID();
		final JobID jid2 = new JobID();

		final String jobName1 = "testjob";
		final String jobName2 = "anotherJob";

		JobManagerJobMetricGroup jmJobGroup11 = group.addJob(jid1, jobName1);
		JobManagerJobMetricGroup jmJobGroup21 = group.addJob(jid2, jobName2);

		group.close();

		assertTrue(jmJobGroup11.isClosed());
		assertTrue(jmJobGroup21.isClosed());
	}

	// ------------------------------------------------------------------------
	//  scope name tests
	// ------------------------------------------------------------------------

	@Test
	public void testGenerateScopeDefault() {
		MetricRegistry registry = new MetricRegistry(new Configuration());
		JobManagerMetricGroup group = new JobManagerMetricGroup(registry, "localhost");

		assertArrayEquals(new String[]{"localhost", "jobmanager"}, group.getScopeComponents());
		assertEquals("localhost.jobmanager", group.getScopeString());
	}

	@Test
	public void testGenerateScopeCustom() {
		MetricRegistry registry = new MetricRegistry(new Configuration());
		JobManagerScopeFormat format = new JobManagerScopeFormat("constant.<host>.foo.<host>");
		JobManagerMetricGroup group = new JobManagerMetricGroup(registry, format, "host");

		assertArrayEquals(new String[]{"constant", "host", "foo", "host"}, group.getScopeComponents());
		assertEquals("constant.host.foo.host", group.getScopeString());
	}
}
