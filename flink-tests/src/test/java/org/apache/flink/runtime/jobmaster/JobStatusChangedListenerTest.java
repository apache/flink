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

package org.apache.flink.runtime.jobmaster;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobExecutionStatusEvent;
import org.apache.flink.core.execution.JobStatusChangedEvent;
import org.apache.flink.core.execution.JobStatusChangedListener;
import org.apache.flink.core.execution.JobStatusChangedListenerFactory;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.CloseableIterator;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.configuration.DeploymentOptions.JOB_STATUS_CHANGED_LISTENERS;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for job status changed listener. */
public class JobStatusChangedListenerTest {
    private static List<JobStatusChangedEvent> statusChangedEvents = new ArrayList<>();

    @Test
    void testJobStatusChanged() throws Exception {
        Configuration configuration = new Configuration();
        configuration.set(
                JOB_STATUS_CHANGED_LISTENERS,
                Collections.singletonList(TestingJobStatusChangedListenerFactory.class.getName()));
        try (StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(configuration)) {
            List<String> sourceValues = Arrays.asList("a", "b", "c");
            List<String> resultValues = new ArrayList<>();
            try (CloseableIterator<String> iterator =
                    env.fromCollection(sourceValues).executeAndCollect()) {
                while (iterator.hasNext()) {
                    resultValues.add(iterator.next());
                }
            }
            assertThat(resultValues).containsExactlyInAnyOrder(sourceValues.toArray(new String[0]));
        }
        assertThat(statusChangedEvents.size()).isEqualTo(2);
        assertThat(statusChangedEvents.get(0).jobId())
                .isEqualTo(statusChangedEvents.get(1).jobId());
        assertThat(statusChangedEvents.get(0).jobName())
                .isEqualTo(statusChangedEvents.get(1).jobName());

        statusChangedEvents.forEach(
                event -> {
                    JobExecutionStatusEvent status = (JobExecutionStatusEvent) event;
                    assertThat(
                                    (status.oldStatus() == JobStatus.CREATED
                                                    && status.newStatus() == JobStatus.RUNNING)
                                            || (status.oldStatus() == JobStatus.RUNNING
                                                    && status.newStatus() == JobStatus.FINISHED))
                            .isTrue();
                });
    }

    /** Testing job status changed listener factory. */
    public static class TestingJobStatusChangedListenerFactory
            implements JobStatusChangedListenerFactory {

        @Override
        public JobStatusChangedListener createListener(Context context) {
            return new TestingJobStatusChangedListener();
        }
    }

    /** Testing job status changed listener. */
    private static class TestingJobStatusChangedListener implements JobStatusChangedListener {

        @Override
        public void onEvent(JobStatusChangedEvent event) {
            statusChangedEvents.add(event);
        }
    }
}
