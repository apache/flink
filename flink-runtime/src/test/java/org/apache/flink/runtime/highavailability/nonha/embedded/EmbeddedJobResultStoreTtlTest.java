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

package org.apache.flink.runtime.highavailability.nonha.embedded;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.highavailability.JobResultEntry;
import org.apache.flink.runtime.highavailability.JobResultStore;
import org.apache.flink.runtime.highavailability.JobResultStoreOptions;
import org.apache.flink.runtime.testutils.TestingJobResultStore;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests the eviction of the clean job result entries of the {@link EmbeddedJobResultStore}. The
 * entries are only evicted if {@link JobResultStoreOptions#CLEAN_JOB_RESULT_TTL} is configured.
 * Otherwise, they are retained indefinitely which is the behaviour this store always had.
 */
class EmbeddedJobResultStoreTtlTest {

    private static final Duration TTL = Duration.ofMillis(50);

    @Test
    void testCleanEntryIsRetainedIndefinitelyWithoutConfiguration() throws Exception {
        assertThatCleanEntryIsStillPresentAfterTtl(new EmbeddedJobResultStore());
    }

    @Test
    void testCleanEntryIsRetainedIndefinitelyIfTtlIsNotConfigured() throws Exception {
        assertThatCleanEntryIsStillPresentAfterTtl(new EmbeddedJobResultStore(new Configuration()));
    }

    @Test
    void testCleanEntryIsRetainedWithinConfiguredTtl() throws Exception {
        final JobResultStore jobResultStore = new EmbeddedJobResultStore(configurationWithTtl());
        final JobResultEntry entry = new JobResultEntry(TestingJobResultStore.DUMMY_JOB_RESULT);

        jobResultStore.createDirtyResultAsync(entry).join();
        jobResultStore.markResultAsCleanAsync(entry.getJobId()).join();

        assertThat(jobResultStore.hasCleanJobResultEntryAsync(entry.getJobId()).join()).isTrue();
    }

    @Test
    void testCleanEntryIsEvictedAfterConfiguredTtl() throws Exception {
        final JobResultStore jobResultStore = new EmbeddedJobResultStore(configurationWithTtl());
        final JobResultEntry entry = new JobResultEntry(TestingJobResultStore.DUMMY_JOB_RESULT);

        jobResultStore.createDirtyResultAsync(entry).join();
        jobResultStore.markResultAsCleanAsync(entry.getJobId()).join();
        assertThat(jobResultStore.hasCleanJobResultEntryAsync(entry.getJobId()).join()).isTrue();

        Thread.sleep(TTL.toMillis() * 4);

        assertThat(jobResultStore.hasCleanJobResultEntryAsync(entry.getJobId()).join()).isFalse();
        assertThat(jobResultStore.hasJobResultEntryAsync(entry.getJobId()).join()).isFalse();
    }

    private static Configuration configurationWithTtl() {
        final Configuration configuration = new Configuration();
        configuration.set(JobResultStoreOptions.CLEAN_JOB_RESULT_TTL, TTL);
        return configuration;
    }

    private static void assertThatCleanEntryIsStillPresentAfterTtl(JobResultStore jobResultStore)
            throws Exception {
        final JobResultEntry entry = new JobResultEntry(TestingJobResultStore.DUMMY_JOB_RESULT);

        jobResultStore.createDirtyResultAsync(entry).join();
        jobResultStore.markResultAsCleanAsync(entry.getJobId()).join();

        Thread.sleep(TTL.toMillis() * 4);

        assertThat(jobResultStore.hasCleanJobResultEntryAsync(entry.getJobId()).join()).isTrue();
    }
}
