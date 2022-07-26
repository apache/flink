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

package org.apache.flink.runtime.rest.handler.legacy.metrics;

import org.apache.flink.runtime.metrics.dump.MetricDump;
import org.apache.flink.runtime.metrics.dump.QueryScopeInfo;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the MetricStore. */
class MetricStoreTest {

    @Test
    void testAdd() throws IOException {
        MetricStore store = setupStore(new MetricStore());

        assertThat(store.getJobManagerMetricStore().getMetric("abc.metric1", "-1")).isEqualTo("0");
        assertThat(store.getTaskManagerMetricStore("tmid").getMetric("abc.metric2", "-1"))
                .isEqualTo("1");
        assertThat(store.getJobMetricStore("jobid").getMetric("abc.metric3", "-1")).isEqualTo("2");
        assertThat(store.getJobMetricStore("jobid").getMetric("abc.metric4", "-1")).isEqualTo("3");

        assertThat(store.getTaskMetricStore("jobid", "taskid").getMetric("8.abc.metric5", "-1"))
                .isEqualTo("14");
        assertThat(store.getSubtaskMetricStore("jobid", "taskid", 8).getMetric("abc.metric5", "-1"))
                .isEqualTo("14");
        assertThat(
                        store.getSubtaskAttemptMetricStore("jobid", "taskid", 8, 1)
                                .getMetric("abc.metric5", "-1"))
                .isEqualTo("4");
        assertThat(
                        store.getSubtaskAttemptMetricStore("jobid", "taskid", 8, 2)
                                .getMetric("abc.metric5", "-1"))
                .isEqualTo("14");

        assertThat(
                        store.getTaskMetricStore("jobid", "taskid")
                                .getMetric("8.opname.abc.metric6", "-1"))
                .isEqualTo("5");
        assertThat(
                        store.getTaskMetricStore("jobid", "taskid")
                                .getMetric("8.opname.abc.metric7", "-1"))
                .isEqualTo("6");
        assertThat(
                        store.getTaskMetricStore("jobid", "taskid")
                                .getMetric("1.opname.abc.metric7", "-1"))
                .isEqualTo("6");
        assertThat(
                        store.getSubtaskMetricStore("jobid", "taskid", 1)
                                .getMetric("opname.abc.metric7", "-1"))
                .isEqualTo("6");
        assertThat(store.getSubtaskAttemptMetricStore("jobid", "taskid", 1, 2)).isNull();
        assertThat(
                        store.getSubtaskAttemptMetricStore("jobid", "taskid", 1, 3)
                                .getMetric("opname.abc.metric7", "-1"))
                .isEqualTo("6");
        assertThat(
                        store.getSubtaskAttemptMetricStore("jobid", "taskid", 8, 2)
                                .getMetric("opname.abc.metric7", "-1"))
                .isEqualTo("6");
        assertThat(
                        store.getSubtaskAttemptMetricStore("jobid", "taskid", 8, 4)
                                .getMetric("opname.abc.metric7", "-1"))
                .isEqualTo("16");
    }

    @Test
    void testMalformedNameHandling() {
        MetricStore store = new MetricStore();
        // -----verify that no exceptions are thrown

        // null
        store.add(null);
        // empty name
        QueryScopeInfo.JobManagerQueryScopeInfo info =
                new QueryScopeInfo.JobManagerQueryScopeInfo("");
        MetricDump.CounterDump cd = new MetricDump.CounterDump(info, "", 0);
        store.add(cd);

        // -----verify that no side effects occur
        assertThat(store.getJobManager().metrics).isEmpty();
        assertThat(store.getTaskManagers()).isEmpty();
        assertThat(store.getJobs()).isEmpty();
    }

    static MetricStore setupStore(MetricStore store) {
        Map<Integer, Integer> currentExecutionAttempts = new HashMap<>();
        currentExecutionAttempts.put(8, 2);
        store.getCurrentExecutionAttempts()
                .put("jobid", Collections.singletonMap("taskid", currentExecutionAttempts));

        QueryScopeInfo.JobManagerQueryScopeInfo jm =
                new QueryScopeInfo.JobManagerQueryScopeInfo("abc");
        MetricDump.CounterDump cd1 = new MetricDump.CounterDump(jm, "metric1", 0);

        QueryScopeInfo.TaskManagerQueryScopeInfo tm =
                new QueryScopeInfo.TaskManagerQueryScopeInfo("tmid", "abc");
        MetricDump.CounterDump cd2 = new MetricDump.CounterDump(tm, "metric2", 1);
        MetricDump.CounterDump cd2a = new MetricDump.CounterDump(tm, "metric22", 1);

        QueryScopeInfo.TaskManagerQueryScopeInfo tm2 =
                new QueryScopeInfo.TaskManagerQueryScopeInfo("tmid2", "abc");
        MetricDump.CounterDump cd22 = new MetricDump.CounterDump(tm2, "metric2", 10);
        MetricDump.CounterDump cd22a = new MetricDump.CounterDump(tm2, "metric2b", 10);

        QueryScopeInfo.JobQueryScopeInfo job = new QueryScopeInfo.JobQueryScopeInfo("jobid", "abc");
        MetricDump.CounterDump cd3 = new MetricDump.CounterDump(job, "metric3", 2);
        MetricDump.CounterDump cd4 = new MetricDump.CounterDump(job, "metric4", 3);

        QueryScopeInfo.JobQueryScopeInfo job2 =
                new QueryScopeInfo.JobQueryScopeInfo("jobid2", "abc");
        MetricDump.CounterDump cd32 = new MetricDump.CounterDump(job2, "metric3", 2);
        MetricDump.CounterDump cd42 = new MetricDump.CounterDump(job2, "metric4", 3);

        QueryScopeInfo.TaskQueryScopeInfo task =
                new QueryScopeInfo.TaskQueryScopeInfo("jobid", "taskid", 8, 1, "abc");
        MetricDump.CounterDump cd5 = new MetricDump.CounterDump(task, "metric5", 4);

        QueryScopeInfo.TaskQueryScopeInfo speculativeTask =
                new QueryScopeInfo.TaskQueryScopeInfo("jobid", "taskid", 8, 2, "abc");
        MetricDump.CounterDump cd52 = new MetricDump.CounterDump(speculativeTask, "metric5", 14);

        QueryScopeInfo.OperatorQueryScopeInfo operator =
                new QueryScopeInfo.OperatorQueryScopeInfo("jobid", "taskid", 8, 2, "opname", "abc");
        MetricDump.CounterDump cd6 = new MetricDump.CounterDump(operator, "metric6", 5);
        MetricDump.CounterDump cd7 = new MetricDump.CounterDump(operator, "metric7", 6);

        QueryScopeInfo.OperatorQueryScopeInfo operator2 =
                new QueryScopeInfo.OperatorQueryScopeInfo("jobid", "taskid", 1, 3, "opname", "abc");
        MetricDump.CounterDump cd62 = new MetricDump.CounterDump(operator2, "metric6", 5);
        MetricDump.CounterDump cd72 = new MetricDump.CounterDump(operator2, "metric7", 6);

        QueryScopeInfo.OperatorQueryScopeInfo speculativeOperator2 =
                new QueryScopeInfo.OperatorQueryScopeInfo("jobid", "taskid", 8, 4, "opname", "abc");
        MetricDump.CounterDump cd63 =
                new MetricDump.CounterDump(speculativeOperator2, "metric6", 15);
        MetricDump.CounterDump cd73 =
                new MetricDump.CounterDump(speculativeOperator2, "metric7", 16);

        store.add(cd1);
        store.add(cd2);
        store.add(cd2a);
        store.add(cd3);
        store.add(cd4);
        store.add(cd5);
        store.add(cd6);
        store.add(cd7);

        store.add(cd62);
        store.add(cd72);
        store.add(cd22);
        store.add(cd22a);
        store.add(cd32);
        store.add(cd42);

        store.add(cd52);
        store.add(cd63);
        store.add(cd73);

        return store;
    }
}
