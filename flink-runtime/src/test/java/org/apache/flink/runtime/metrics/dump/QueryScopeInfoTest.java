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
package org.apache.flink.runtime.metrics.dump;

import org.junit.Test;

import static org.apache.flink.runtime.metrics.dump.QueryScopeInfo.INFO_CATEGORY_JM;
import static org.apache.flink.runtime.metrics.dump.QueryScopeInfo.INFO_CATEGORY_JOB;
import static org.apache.flink.runtime.metrics.dump.QueryScopeInfo.INFO_CATEGORY_OPERATOR;
import static org.apache.flink.runtime.metrics.dump.QueryScopeInfo.INFO_CATEGORY_TASK;
import static org.apache.flink.runtime.metrics.dump.QueryScopeInfo.INFO_CATEGORY_TM;
import static org.junit.Assert.assertEquals;

public class QueryScopeInfoTest {
	@Test
	public void testJobManagerMetricInfo() {
		QueryScopeInfo.JobManagerQueryScopeInfo info = new QueryScopeInfo.JobManagerQueryScopeInfo("abc");
		assertEquals("abc", info.scope);
		assertEquals(INFO_CATEGORY_JM, info.getCategory());
	}

	@Test
	public void testTaskManagerMetricInfo() {
		QueryScopeInfo.TaskManagerQueryScopeInfo info = new QueryScopeInfo.TaskManagerQueryScopeInfo("tmid", "abc");
		assertEquals("abc", info.scope);
		assertEquals("tmid", info.taskManagerID);
		assertEquals(INFO_CATEGORY_TM, info.getCategory());
	}

	@Test
	public void testJobMetricInfo() {
		QueryScopeInfo.JobQueryScopeInfo info = new QueryScopeInfo.JobQueryScopeInfo("jobid", "abc");
		assertEquals("abc", info.scope);
		assertEquals("jobid", info.jobID);
		assertEquals(INFO_CATEGORY_JOB, info.getCategory());
	}

	@Test
	public void testTaskMetricInfo() {
		QueryScopeInfo.TaskQueryScopeInfo info = new QueryScopeInfo.TaskQueryScopeInfo("jid", "vid", 2, "abc");
		assertEquals("abc", info.scope);
		assertEquals("jid", info.jobID);
		assertEquals("vid", info.vertexID);
		assertEquals(2, info.subtaskIndex);
		assertEquals(INFO_CATEGORY_TASK, info.getCategory());
	}

	@Test
	public void testOperatorMetricInfo() {
		QueryScopeInfo.OperatorQueryScopeInfo info = new QueryScopeInfo.OperatorQueryScopeInfo("jid", "vid", 2, "opname", "abc");
		assertEquals("abc", info.scope);
		assertEquals("jid", info.jobID);
		assertEquals("vid", info.vertexID);
		assertEquals("opname", info.operatorName);
		assertEquals(2, info.subtaskIndex);
		assertEquals(INFO_CATEGORY_OPERATOR, info.getCategory());
	}
}
