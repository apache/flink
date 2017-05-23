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

package org.apache.flink.runtime.webmonitor.metrics;

import org.apache.flink.runtime.metrics.dump.MetricDump;
import org.apache.flink.runtime.metrics.dump.QueryScopeInfo;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

/**
 * Tests for the MetricStore.
 */
public class MetricStoreTest extends TestLogger {
	@Test
	public void testAdd() throws IOException {
		MetricStore store = setupStore(new MetricStore());

		assertEquals("0", store.getJobManagerMetricStore().getMetric("abc.metric1", "-1"));
		assertEquals("1", store.getTaskManagerMetricStore("tmid").getMetric("abc.metric2", "-1"));
		assertEquals("2", store.getJobMetricStore("jobid").getMetric("abc.metric3", "-1"));
		assertEquals("3", store.getTaskMetricStore("jobid", "taskid").getMetric("8.abc.metric4", "-1"));
		assertEquals("4", store.getTaskMetricStore("jobid", "taskid").getMetric("8.opname.abc.metric5", "-1"));
	}

	@Test
	public void testMalformedNameHandling() {
		MetricStore store = new MetricStore();
		//-----verify that no exceptions are thrown

		// null
		store.add(null);
		// empty name
		QueryScopeInfo.JobManagerQueryScopeInfo info = new QueryScopeInfo.JobManagerQueryScopeInfo("");
		MetricDump.CounterDump cd = new MetricDump.CounterDump(info, "", 0);
		store.add(cd);

		//-----verify that no side effects occur
		assertEquals(0, store.jobManager.metrics.size());
		assertEquals(0, store.taskManagers.size());
		assertEquals(0, store.jobs.size());
	}

	public static MetricStore setupStore(MetricStore store) {
		QueryScopeInfo.JobManagerQueryScopeInfo jm = new QueryScopeInfo.JobManagerQueryScopeInfo("abc");
		MetricDump.CounterDump cd1 = new MetricDump.CounterDump(jm, "metric1", 0);

		QueryScopeInfo.TaskManagerQueryScopeInfo tm = new QueryScopeInfo.TaskManagerQueryScopeInfo("tmid", "abc");
		MetricDump.CounterDump cd2 = new MetricDump.CounterDump(tm, "metric2", 1);

		QueryScopeInfo.JobQueryScopeInfo job = new QueryScopeInfo.JobQueryScopeInfo("jobid", "abc");
		MetricDump.CounterDump cd3 = new MetricDump.CounterDump(job, "metric3", 2);

		QueryScopeInfo.TaskQueryScopeInfo task = new QueryScopeInfo.TaskQueryScopeInfo("jobid", "taskid", 8, "abc");
		MetricDump.CounterDump cd4 = new MetricDump.CounterDump(task, "metric4", 3);

		QueryScopeInfo.OperatorQueryScopeInfo operator = new QueryScopeInfo.OperatorQueryScopeInfo("jobid", "taskid", 8, "opname", "abc");
		MetricDump.CounterDump cd5 = new MetricDump.CounterDump(operator, "metric5", 4);

		store.add(cd1);
		store.add(cd2);
		store.add(cd3);
		store.add(cd4);
		store.add(cd5);

		return store;
	}
}
