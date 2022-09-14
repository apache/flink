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

package org.apache.flink.runtime.highavailability;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.testutils.TestingJobResultStore;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

/**
 * This interface defines a series of tests for any implementation of the {@link JobResultStore} to
 * determine whether they correctly implement the contracts defined by the interface.
 */
public interface JobResultStoreContractTest {

    JobResultEntry DUMMY_JOB_RESULT_ENTRY =
            new JobResultEntry(TestingJobResultStore.DUMMY_JOB_RESULT);

    JobResultStore createJobResultStore() throws IOException;

    @Test
    default void testStoreJobResultsWithDuplicateIDsThrowsException() throws IOException {
        JobResultStore jobResultStore = createJobResultStore();
        jobResultStore.createDirtyResult(DUMMY_JOB_RESULT_ENTRY);
        final JobResultEntry otherEntryWithDuplicateId =
                new JobResultEntry(
                        TestingJobResultStore.createSuccessfulJobResult(
                                DUMMY_JOB_RESULT_ENTRY.getJobId()));
        assertThatThrownBy(() -> jobResultStore.createDirtyResult(otherEntryWithDuplicateId))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    default void testStoreDirtyEntryForAlreadyCleanedJobResultThrowsException() throws IOException {
        JobResultStore jobResultStore = createJobResultStore();
        jobResultStore.createDirtyResult(DUMMY_JOB_RESULT_ENTRY);
        jobResultStore.markResultAsClean(DUMMY_JOB_RESULT_ENTRY.getJobId());
        assertThatThrownBy(() -> jobResultStore.createDirtyResult(DUMMY_JOB_RESULT_ENTRY))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    default void testCleaningDuplicateEntryThrowsNoException() throws IOException {
        JobResultStore jobResultStore = createJobResultStore();
        jobResultStore.createDirtyResult(DUMMY_JOB_RESULT_ENTRY);
        jobResultStore.markResultAsClean(DUMMY_JOB_RESULT_ENTRY.getJobId());
        assertThatNoException()
                .isThrownBy(
                        () -> jobResultStore.markResultAsClean(DUMMY_JOB_RESULT_ENTRY.getJobId()));
    }

    @Test
    default void testCleaningNonExistentEntryThrowsException() throws IOException {
        JobResultStore jobResultStore = createJobResultStore();
        assertThatThrownBy(
                        () -> jobResultStore.markResultAsClean(DUMMY_JOB_RESULT_ENTRY.getJobId()))
                .isInstanceOf(NoSuchElementException.class);
    }

    @Test
    default void testHasJobResultEntryWithDirtyEntry() throws IOException {
        JobResultStore jobResultStore = createJobResultStore();
        jobResultStore.createDirtyResult(DUMMY_JOB_RESULT_ENTRY);
        assertThat(jobResultStore.hasDirtyJobResultEntry(DUMMY_JOB_RESULT_ENTRY.getJobId()))
                .isTrue();
        assertThat(jobResultStore.hasCleanJobResultEntry(DUMMY_JOB_RESULT_ENTRY.getJobId()))
                .isFalse();
        assertThat(jobResultStore.hasJobResultEntry(DUMMY_JOB_RESULT_ENTRY.getJobId())).isTrue();
    }

    @Test
    default void testHasJobResultEntryWithCleanEntry() throws IOException {
        JobResultStore jobResultStore = createJobResultStore();
        jobResultStore.createDirtyResult(DUMMY_JOB_RESULT_ENTRY);
        jobResultStore.markResultAsClean(DUMMY_JOB_RESULT_ENTRY.getJobId());
        assertThat(jobResultStore.hasDirtyJobResultEntry(DUMMY_JOB_RESULT_ENTRY.getJobId()))
                .isFalse();
        assertThat(jobResultStore.hasCleanJobResultEntry(DUMMY_JOB_RESULT_ENTRY.getJobId()))
                .isTrue();
        assertThat(jobResultStore.hasJobResultEntry(DUMMY_JOB_RESULT_ENTRY.getJobId())).isTrue();
    }

    @Test
    default void testHasJobResultEntryWithEmptyStore() throws IOException {
        JobResultStore jobResultStore = createJobResultStore();
        JobID jobId = new JobID();
        assertThat(jobResultStore.hasDirtyJobResultEntry(jobId)).isFalse();
        assertThat(jobResultStore.hasCleanJobResultEntry(jobId)).isFalse();
        assertThat(jobResultStore.hasJobResultEntry(jobId)).isFalse();
    }

    @Test
    default void testGetDirtyResultsWithNoEntry() throws IOException {
        JobResultStore jobResultStore = createJobResultStore();
        assertThat(jobResultStore.getDirtyResults()).isEmpty();
    }

    @Test
    default void testGetDirtyResultsWithDirtyEntry() throws IOException {
        JobResultStore jobResultStore = createJobResultStore();
        jobResultStore.createDirtyResult(DUMMY_JOB_RESULT_ENTRY);
        assertThat(
                        jobResultStore.getDirtyResults().stream()
                                .map(JobResult::getJobId)
                                .collect(Collectors.toList()))
                .singleElement()
                .isEqualTo(DUMMY_JOB_RESULT_ENTRY.getJobId());
    }

    @Test
    default void testGetDirtyResultsWithDirtyAndCleanEntry() throws IOException {
        JobResultStore jobResultStore = createJobResultStore();
        jobResultStore.createDirtyResult(DUMMY_JOB_RESULT_ENTRY);
        jobResultStore.markResultAsClean(DUMMY_JOB_RESULT_ENTRY.getJobId());

        final JobResultEntry otherDirtyJobResultEntry =
                new JobResultEntry(TestingJobResultStore.createSuccessfulJobResult(new JobID()));
        jobResultStore.createDirtyResult(otherDirtyJobResultEntry);

        assertThat(
                        jobResultStore.getDirtyResults().stream()
                                .map(JobResult::getJobId)
                                .collect(Collectors.toList()))
                .singleElement()
                .isEqualTo(otherDirtyJobResultEntry.getJobId());
    }
}
