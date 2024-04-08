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

package org.apache.flink.datastream.impl.context;

import org.apache.flink.datastream.api.context.JobInfo;
import org.apache.flink.runtime.jobgraph.JobType;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.operators.testutils.MockEnvironmentBuilder;
import org.apache.flink.streaming.util.MockStreamingRuntimeContext;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link DefaultJobInfo}. */
class DefaultJobInfoTest {
    @Test
    void testGetJobName() {
        String jobName = "Test-Job";
        DefaultJobInfo jobInfo = createDefaultJobInfo(jobName);
        assertThat(jobInfo.getJobName()).isEqualTo(jobName);
    }

    @Test
    void testGetExecutionMode() {
        DefaultJobInfo batchJob = createDefaultJobInfo(JobType.BATCH);
        assertThat(batchJob.getExecutionMode()).isEqualTo(JobInfo.ExecutionMode.BATCH);

        DefaultJobInfo streamingJob = createDefaultJobInfo(JobType.STREAMING);
        assertThat(streamingJob.getExecutionMode()).isEqualTo(JobInfo.ExecutionMode.STREAMING);
    }

    private static DefaultJobInfo createDefaultJobInfo(String jobName) {
        return createDefaultJobInfo(JobType.STREAMING, jobName);
    }

    private static DefaultJobInfo createDefaultJobInfo(JobType jobType) {
        return createDefaultJobInfo(jobType, "mock-job");
    }

    private static DefaultJobInfo createDefaultJobInfo(JobType jobType, String jobName) {
        return new DefaultJobInfo(
                new MockStreamingRuntimeContext(
                        false,
                        2,
                        1,
                        new MockEnvironmentBuilder()
                                .setTaskName("mockTask")
                                .setManagedMemorySize(4 * MemoryManager.DEFAULT_PAGE_SIZE)
                                .setParallelism(2)
                                .setMaxParallelism(2)
                                .setSubtaskIndex(1)
                                .setJobType(jobType)
                                .setJobName(jobName)
                                .build()));
    }
}
