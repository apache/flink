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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link QueryScopeInfo} classes. */
class QueryScopeInfoTest {
    @Test
    void testJobManagerQueryScopeInfo() {
        QueryScopeInfo.JobManagerQueryScopeInfo info =
                new QueryScopeInfo.JobManagerQueryScopeInfo();
        assertThat(info.getCategory()).isEqualTo(QueryScopeInfo.INFO_CATEGORY_JM);
        assertThat(info.scope).isEmpty();

        info = info.copy("world");
        assertThat(info.getCategory()).isEqualTo(QueryScopeInfo.INFO_CATEGORY_JM);
        assertThat(info.scope).isEqualTo("world");

        info = new QueryScopeInfo.JobManagerQueryScopeInfo("hello");
        assertThat(info.getCategory()).isEqualTo(QueryScopeInfo.INFO_CATEGORY_JM);
        assertThat(info.scope).isEqualTo("hello");

        info = info.copy("world");
        assertThat(info.getCategory()).isEqualTo(QueryScopeInfo.INFO_CATEGORY_JM);
        assertThat(info.scope).isEqualTo("hello.world");
    }

    @Test
    void testTaskManagerQueryScopeInfo() {
        QueryScopeInfo.TaskManagerQueryScopeInfo info =
                new QueryScopeInfo.TaskManagerQueryScopeInfo("tmid");
        assertThat(info.getCategory()).isEqualTo(QueryScopeInfo.INFO_CATEGORY_TM);
        assertThat(info.scope).isEmpty();
        assertThat(info.taskManagerID).isEqualTo("tmid");

        info = info.copy("world");
        assertThat(info.getCategory()).isEqualTo(QueryScopeInfo.INFO_CATEGORY_TM);
        assertThat(info.scope).isEqualTo("world");
        assertThat(info.taskManagerID).isEqualTo("tmid");

        info = new QueryScopeInfo.TaskManagerQueryScopeInfo("tmid", "hello");
        assertThat(info.getCategory()).isEqualTo(QueryScopeInfo.INFO_CATEGORY_TM);
        assertThat(info.scope).isEqualTo("hello");
        assertThat(info.taskManagerID).isEqualTo("tmid");

        info = info.copy("world");
        assertThat(info.getCategory()).isEqualTo(QueryScopeInfo.INFO_CATEGORY_TM);
        assertThat(info.scope).isEqualTo("hello.world");
        assertThat(info.taskManagerID).isEqualTo("tmid");
    }

    @Test
    void testJobQueryScopeInfo() {
        QueryScopeInfo.JobQueryScopeInfo info = new QueryScopeInfo.JobQueryScopeInfo("jobid");
        assertThat(info.getCategory()).isEqualTo(QueryScopeInfo.INFO_CATEGORY_JOB);
        assertThat(info.scope).isEmpty();
        assertThat(info.jobID).isEqualTo("jobid");

        info = info.copy("world");
        assertThat(info.getCategory()).isEqualTo(QueryScopeInfo.INFO_CATEGORY_JOB);
        assertThat(info.scope).isEqualTo("world");
        assertThat(info.jobID).isEqualTo("jobid");

        info = new QueryScopeInfo.JobQueryScopeInfo("jobid", "hello");
        assertThat(info.getCategory()).isEqualTo(QueryScopeInfo.INFO_CATEGORY_JOB);
        assertThat(info.scope).isEqualTo("hello");
        assertThat(info.jobID).isEqualTo("jobid");

        info = info.copy("world");
        assertThat(info.getCategory()).isEqualTo(QueryScopeInfo.INFO_CATEGORY_JOB);
        assertThat(info.scope).isEqualTo("hello.world");
        assertThat(info.jobID).isEqualTo("jobid");
    }

    @Test
    void testTaskQueryScopeInfo() {
        QueryScopeInfo.TaskQueryScopeInfo info =
                new QueryScopeInfo.TaskQueryScopeInfo("jobid", "taskid", 2, 0);
        assertThat(info.getCategory()).isEqualTo(QueryScopeInfo.INFO_CATEGORY_TASK);
        assertThat(info.scope).isEmpty();
        assertThat(info.jobID).isEqualTo("jobid");
        assertThat(info.vertexID).isEqualTo("taskid");
        assertThat(info.subtaskIndex).isEqualTo(2);

        info = info.copy("world");
        assertThat(info.getCategory()).isEqualTo(QueryScopeInfo.INFO_CATEGORY_TASK);
        assertThat(info.scope).isEqualTo("world");
        assertThat(info.jobID).isEqualTo("jobid");
        assertThat(info.vertexID).isEqualTo("taskid");
        assertThat(info.subtaskIndex).isEqualTo(2);

        info = new QueryScopeInfo.TaskQueryScopeInfo("jobid", "taskid", 2, 0, "hello");
        assertThat(info.getCategory()).isEqualTo(QueryScopeInfo.INFO_CATEGORY_TASK);
        assertThat(info.scope).isEqualTo("hello");
        assertThat(info.jobID).isEqualTo("jobid");
        assertThat(info.vertexID).isEqualTo("taskid");
        assertThat(info.subtaskIndex).isEqualTo(2);

        info = info.copy("world");
        assertThat(info.getCategory()).isEqualTo(QueryScopeInfo.INFO_CATEGORY_TASK);
        assertThat(info.scope).isEqualTo("hello.world");
        assertThat(info.jobID).isEqualTo("jobid");
        assertThat(info.vertexID).isEqualTo("taskid");
        assertThat(info.subtaskIndex).isEqualTo(2);
    }

    @Test
    void testOperatorQueryScopeInfo() {
        QueryScopeInfo.OperatorQueryScopeInfo info =
                new QueryScopeInfo.OperatorQueryScopeInfo("jobid", "taskid", 2, 0, "opname");
        assertThat(info.getCategory()).isEqualTo(QueryScopeInfo.INFO_CATEGORY_OPERATOR);
        assertThat(info.scope).isEmpty();
        assertThat(info.jobID).isEqualTo("jobid");
        assertThat(info.vertexID).isEqualTo("taskid");
        assertThat(info.operatorName).isEqualTo("opname");
        assertThat(info.subtaskIndex).isEqualTo(2);

        info = info.copy("world");
        assertThat(info.getCategory()).isEqualTo(QueryScopeInfo.INFO_CATEGORY_OPERATOR);
        assertThat(info.scope).isEqualTo("world");
        assertThat(info.jobID).isEqualTo("jobid");
        assertThat(info.vertexID).isEqualTo("taskid");
        assertThat(info.operatorName).isEqualTo("opname");
        assertThat(info.subtaskIndex).isEqualTo(2);

        info =
                new QueryScopeInfo.OperatorQueryScopeInfo(
                        "jobid", "taskid", 2, 0, "opname", "hello");
        assertThat(info.getCategory()).isEqualTo(QueryScopeInfo.INFO_CATEGORY_OPERATOR);
        assertThat(info.scope).isEqualTo("hello");
        assertThat(info.jobID).isEqualTo("jobid");
        assertThat(info.vertexID).isEqualTo("taskid");
        assertThat(info.operatorName).isEqualTo("opname");
        assertThat(info.subtaskIndex).isEqualTo(2);

        info = info.copy("world");
        assertThat(info.getCategory()).isEqualTo(QueryScopeInfo.INFO_CATEGORY_OPERATOR);
        assertThat(info.scope).isEqualTo("hello.world");
        assertThat(info.jobID).isEqualTo("jobid");
        assertThat(info.vertexID).isEqualTo("taskid");
        assertThat(info.operatorName).isEqualTo("opname");
        assertThat(info.subtaskIndex).isEqualTo(2);
    }

    @Test
    void testJobManagerOperatorQueryScopeInfo() {
        QueryScopeInfo.JobManagerOperatorQueryScopeInfo info =
                new QueryScopeInfo.JobManagerOperatorQueryScopeInfo("jobid", "taskid", "opname");
        assertThat(info.getCategory()).isEqualTo(QueryScopeInfo.INFO_CATEGORY_JM_OPERATOR);
        assertThat(info.scope).isEmpty();
        assertThat(info.jobID).isEqualTo("jobid");
        assertThat(info.vertexID).isEqualTo("taskid");
        assertThat(info.operatorName).isEqualTo("opname");

        info = info.copy("world");
        assertThat(info.getCategory()).isEqualTo(QueryScopeInfo.INFO_CATEGORY_JM_OPERATOR);
        assertThat(info.scope).isEqualTo("world");
        assertThat(info.jobID).isEqualTo("jobid");
        assertThat(info.vertexID).isEqualTo("taskid");
        assertThat(info.operatorName).isEqualTo("opname");

        info =
                new QueryScopeInfo.JobManagerOperatorQueryScopeInfo(
                        "jobid", "taskid", "opname", "hello");
        assertThat(info.getCategory()).isEqualTo(QueryScopeInfo.INFO_CATEGORY_JM_OPERATOR);
        assertThat(info.scope).isEqualTo("hello");
        assertThat(info.jobID).isEqualTo("jobid");
        assertThat(info.vertexID).isEqualTo("taskid");
        assertThat(info.operatorName).isEqualTo("opname");

        info = info.copy("world");
        assertThat(info.getCategory()).isEqualTo(QueryScopeInfo.INFO_CATEGORY_JM_OPERATOR);
        assertThat(info.scope).isEqualTo("hello.world");
        assertThat(info.jobID).isEqualTo("jobid");
        assertThat(info.vertexID).isEqualTo("taskid");
        assertThat(info.operatorName).isEqualTo("opname");
    }
}
