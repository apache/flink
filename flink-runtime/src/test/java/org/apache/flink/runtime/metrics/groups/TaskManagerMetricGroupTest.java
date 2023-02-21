package org.apache.flink.runtime.metrics.groups;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.flink.api.common.JobID;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.apache.flink.runtime.metrics.NoOpMetricRegistry.INSTANCE;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** {@link TaskManagerMetricGroup} test. */
public class TaskManagerMetricGroupTest {
    private static final JobID JOB_ID = new JobID();
    private static final String JOB_NAME = "test job";
    private TaskManagerMetricGroup metricGroup;

    @BeforeEach
    public void before() {
        metricGroup = new TaskManagerMetricGroup(INSTANCE, "testHost", "testTm");
    }

    @AfterEach
    public void after() {
        if (!metricGroup.isClosed()) {
            metricGroup.close();
        }
    }

    @Test
    public void testGetSameJob() {
        assertSame(metricGroup.addJob(JOB_ID, JOB_NAME), metricGroup.addJob(JOB_ID, JOB_NAME));
        assertNotSame(
                metricGroup.addJob(JOB_ID, JOB_NAME),
                metricGroup.addJob(new JobID(), "another job"));
    }

    @Test
    public void testReCreateAfterRemoval() {
        TaskManagerJobMetricGroup oldGroup = metricGroup.addJob(JOB_ID, JOB_NAME);
        metricGroup.removeJobMetricsGroup(JOB_ID);
        assertNotSame(oldGroup, metricGroup.addJob(JOB_ID, JOB_NAME));
    }

    @Test
    public void testCloseOnRemove() {
        TaskManagerJobMetricGroup tmJobMetricGroup = metricGroup.addJob(JOB_ID, JOB_NAME);
        metricGroup.removeJobMetricsGroup(JOB_ID);
        assertTrue(tmJobMetricGroup.isClosed());
    }

    @Test
    public void testCloseWithoutRemoval() {
        TaskManagerJobMetricGroup jobGroup = metricGroup.addJob(JOB_ID, JOB_NAME);
        metricGroup.close();
        assertTrue(jobGroup.isClosed());
    }

    @Test
    public void testRemoveNullJobID() {
        metricGroup.removeJobMetricsGroup(null);
    }

    @Test
    public void testRemoveInvalidJobID() {
        metricGroup.removeJobMetricsGroup(JOB_ID);
    }
}
