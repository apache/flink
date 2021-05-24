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

import static org.junit.Assert.assertEquals;

/** Tests for the {@link QueryScopeInfo} classes. */
public class QueryScopeInfoTest {
    @Test
    public void testJobManagerQueryScopeInfo() {
        QueryScopeInfo.JobManagerQueryScopeInfo info =
                new QueryScopeInfo.JobManagerQueryScopeInfo();
        assertEquals(QueryScopeInfo.INFO_CATEGORY_JM, info.getCategory());
        assertEquals("", info.scope);

        info = info.copy("world");
        assertEquals(QueryScopeInfo.INFO_CATEGORY_JM, info.getCategory());
        assertEquals("world", info.scope);

        info = new QueryScopeInfo.JobManagerQueryScopeInfo("hello");
        assertEquals(QueryScopeInfo.INFO_CATEGORY_JM, info.getCategory());
        assertEquals("hello", info.scope);

        info = info.copy("world");
        assertEquals(QueryScopeInfo.INFO_CATEGORY_JM, info.getCategory());
        assertEquals("hello.world", info.scope);
    }

    @Test
    public void testTaskManagerQueryScopeInfo() {
        QueryScopeInfo.TaskManagerQueryScopeInfo info =
                new QueryScopeInfo.TaskManagerQueryScopeInfo("tmid");
        assertEquals(QueryScopeInfo.INFO_CATEGORY_TM, info.getCategory());
        assertEquals("", info.scope);
        assertEquals("tmid", info.taskManagerID);

        info = info.copy("world");
        assertEquals(QueryScopeInfo.INFO_CATEGORY_TM, info.getCategory());
        assertEquals("world", info.scope);
        assertEquals("tmid", info.taskManagerID);

        info = new QueryScopeInfo.TaskManagerQueryScopeInfo("tmid", "hello");
        assertEquals(QueryScopeInfo.INFO_CATEGORY_TM, info.getCategory());
        assertEquals("hello", info.scope);
        assertEquals("tmid", info.taskManagerID);

        info = info.copy("world");
        assertEquals(QueryScopeInfo.INFO_CATEGORY_TM, info.getCategory());
        assertEquals("hello.world", info.scope);
        assertEquals("tmid", info.taskManagerID);
    }

    @Test
    public void testJobQueryScopeInfo() {
        QueryScopeInfo.JobQueryScopeInfo info = new QueryScopeInfo.JobQueryScopeInfo("jobid");
        assertEquals(QueryScopeInfo.INFO_CATEGORY_JOB, info.getCategory());
        assertEquals("", info.scope);
        assertEquals("jobid", info.jobID);

        info = info.copy("world");
        assertEquals(QueryScopeInfo.INFO_CATEGORY_JOB, info.getCategory());
        assertEquals("world", info.scope);
        assertEquals("jobid", info.jobID);

        info = new QueryScopeInfo.JobQueryScopeInfo("jobid", "hello");
        assertEquals(QueryScopeInfo.INFO_CATEGORY_JOB, info.getCategory());
        assertEquals("hello", info.scope);
        assertEquals("jobid", info.jobID);

        info = info.copy("world");
        assertEquals(QueryScopeInfo.INFO_CATEGORY_JOB, info.getCategory());
        assertEquals("hello.world", info.scope);
        assertEquals("jobid", info.jobID);
    }

    @Test
    public void testTaskQueryScopeInfo() {
        QueryScopeInfo.TaskQueryScopeInfo info =
                new QueryScopeInfo.TaskQueryScopeInfo("jobid", "taskid", 2);
        assertEquals(QueryScopeInfo.INFO_CATEGORY_TASK, info.getCategory());
        assertEquals("", info.scope);
        assertEquals("jobid", info.jobID);
        assertEquals("taskid", info.vertexID);
        assertEquals(2, info.subtaskIndex);

        info = info.copy("world");
        assertEquals(QueryScopeInfo.INFO_CATEGORY_TASK, info.getCategory());
        assertEquals("world", info.scope);
        assertEquals("jobid", info.jobID);
        assertEquals("taskid", info.vertexID);
        assertEquals(2, info.subtaskIndex);

        info = new QueryScopeInfo.TaskQueryScopeInfo("jobid", "taskid", 2, "hello");
        assertEquals(QueryScopeInfo.INFO_CATEGORY_TASK, info.getCategory());
        assertEquals("hello", info.scope);
        assertEquals("jobid", info.jobID);
        assertEquals("taskid", info.vertexID);
        assertEquals(2, info.subtaskIndex);

        info = info.copy("world");
        assertEquals(QueryScopeInfo.INFO_CATEGORY_TASK, info.getCategory());
        assertEquals("hello.world", info.scope);
        assertEquals("jobid", info.jobID);
        assertEquals("taskid", info.vertexID);
        assertEquals(2, info.subtaskIndex);
    }

    @Test
    public void testOperatorQueryScopeInfo() {
        QueryScopeInfo.OperatorQueryScopeInfo info =
                new QueryScopeInfo.OperatorQueryScopeInfo("jobid", "taskid", 2, "opname");
        assertEquals(QueryScopeInfo.INFO_CATEGORY_OPERATOR, info.getCategory());
        assertEquals("", info.scope);
        assertEquals("jobid", info.jobID);
        assertEquals("taskid", info.vertexID);
        assertEquals("opname", info.operatorName);
        assertEquals(2, info.subtaskIndex);

        info = info.copy("world");
        assertEquals(QueryScopeInfo.INFO_CATEGORY_OPERATOR, info.getCategory());
        assertEquals("world", info.scope);
        assertEquals("jobid", info.jobID);
        assertEquals("taskid", info.vertexID);
        assertEquals("opname", info.operatorName);
        assertEquals(2, info.subtaskIndex);

        info = new QueryScopeInfo.OperatorQueryScopeInfo("jobid", "taskid", 2, "opname", "hello");
        assertEquals(QueryScopeInfo.INFO_CATEGORY_OPERATOR, info.getCategory());
        assertEquals("hello", info.scope);
        assertEquals("jobid", info.jobID);
        assertEquals("taskid", info.vertexID);
        assertEquals("opname", info.operatorName);
        assertEquals(2, info.subtaskIndex);

        info = info.copy("world");
        assertEquals(QueryScopeInfo.INFO_CATEGORY_OPERATOR, info.getCategory());
        assertEquals("hello.world", info.scope);
        assertEquals("jobid", info.jobID);
        assertEquals("taskid", info.vertexID);
        assertEquals("opname", info.operatorName);
        assertEquals(2, info.subtaskIndex);
    }
}
