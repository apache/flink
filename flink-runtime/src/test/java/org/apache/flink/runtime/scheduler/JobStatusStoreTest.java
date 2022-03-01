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

package org.apache.flink.runtime.scheduler;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;

import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

class JobStatusStoreTest {

    @Test
    void initialState() {
        final JobStatusStore store = new JobStatusStore(0);
        assertThat(store.getState(), is(JobStatus.INITIALIZING));
    }

    @Test
    void initialTimestamps() {
        final JobStatusStore store = new JobStatusStore(967823L);

        for (JobStatus jobStatus : JobStatus.values()) {
            switch (jobStatus) {
                case INITIALIZING:
                    assertThat(store.getStatusTimestamp(JobStatus.INITIALIZING), is(967823L));
                    break;
                default:
                    assertThat(store.getStatusTimestamp(jobStatus), is(0L));
            }
        }
    }

    @Test
    void getState() {
        final JobStatusStore store = new JobStatusStore(0L);
        store.jobStatusChanges(new JobID(), JobStatus.RUNNING, 1L);

        assertThat(store.getState(), is(JobStatus.RUNNING));
    }

    @Test
    void getStatusTimestamp() {
        final JobStatusStore store = new JobStatusStore(0L);
        store.jobStatusChanges(new JobID(), JobStatus.RUNNING, 1L);

        assertThat(store.getStatusTimestamp(JobStatus.RUNNING), is(1L));
    }
}
