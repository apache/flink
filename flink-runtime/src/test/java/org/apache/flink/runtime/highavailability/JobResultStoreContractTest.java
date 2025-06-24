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
import java.util.concurrent.CompletionException;
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
        jobResultStore.createDirtyResultAsync(DUMMY_JOB_RESULT_ENTRY).join();
        final JobResultEntry otherEntryWithDuplicateId =
                new JobResultEntry(
                        TestingJobResultStore.createSuccessfulJobResult(
                                DUMMY_JOB_RESULT_ENTRY.getJobId()));
        assertThatThrownBy(
                        () ->
                                jobResultStore
                                        .createDirtyResultAsync(otherEntryWithDuplicateId)
                                        .join())
                .isInstanceOf(CompletionException.class)
                .hasCauseInstanceOf(IllegalStateException.class);
    }

    @Test
    default void testStoreDirtyEntryForAlreadyCleanedJobResultThrowsException() throws IOException {
        JobResultStore jobResultStore = createJobResultStore();
        jobResultStore.createDirtyResultAsync(DUMMY_JOB_RESULT_ENTRY).join();
        jobResultStore.markResultAsCleanAsync(DUMMY_JOB_RESULT_ENTRY.getJobId()).join();
        assertThatThrownBy(
                        () -> jobResultStore.createDirtyResultAsync(DUMMY_JOB_RESULT_ENTRY).join())
                .isInstanceOf(CompletionException.class)
                .hasCauseInstanceOf(IllegalStateException.class);
    }

    @Test
    default void testCleaningDuplicateEntryThrowsNoException() throws IOException {
        JobResultStore jobResultStore = createJobResultStore();
        jobResultStore.createDirtyResultAsync(DUMMY_JOB_RESULT_ENTRY).join();
        jobResultStore.markResultAsCleanAsync(DUMMY_JOB_RESULT_ENTRY.getJobId()).join();
        assertThatNoException()
                .isThrownBy(
                        () ->
                                jobResultStore
                                        .markResultAsCleanAsync(DUMMY_JOB_RESULT_ENTRY.getJobId())
                                        .join());
    }

    @Test
    default void testCleaningNonExistentEntryThrowsException() throws IOException {
        JobResultStore jobResultStore = createJobResultStore();
        assertThatThrownBy(
                        () ->
                                jobResultStore
                                        .markResultAsCleanAsync(DUMMY_JOB_RESULT_ENTRY.getJobId())
                                        .join())
                .hasCauseInstanceOf(NoSuchElementException.class);
    }

    @Test
    default void testHasJobResultEntryWithDirtyEntry() throws IOException {
        JobResultStore jobResultStore = createJobResultStore();
        jobResultStore.createDirtyResultAsync(DUMMY_JOB_RESULT_ENTRY).join();
        assertThat(
                        jobResultStore
                                .hasDirtyJobResultEntryAsync(DUMMY_JOB_RESULT_ENTRY.getJobId())
                                .join())
                .isTrue();
        assertThat(
                        jobResultStore
                                .hasCleanJobResultEntryAsync(DUMMY_JOB_RESULT_ENTRY.getJobId())
                                .join())
                .isFalse();
        assertThat(jobResultStore.hasJobResultEntryAsync(DUMMY_JOB_RESULT_ENTRY.getJobId()).join())
                .isTrue();
    }

    @Test
    default void testHasJobResultEntryWithCleanEntry() throws IOException {
        JobResultStore jobResultStore = createJobResultStore();
        jobResultStore.createDirtyResultAsync(DUMMY_JOB_RESULT_ENTRY).join();
        jobResultStore.markResultAsCleanAsync(DUMMY_JOB_RESULT_ENTRY.getJobId()).join();
        assertThat(
                        jobResultStore
                                .hasDirtyJobResultEntryAsync(DUMMY_JOB_RESULT_ENTRY.getJobId())
                                .join())
                .isFalse();
        assertThat(
                        jobResultStore
                                .hasCleanJobResultEntryAsync(DUMMY_JOB_RESULT_ENTRY.getJobId())
                                .join())
                .isTrue();
        assertThat(jobResultStore.hasJobResultEntryAsync(DUMMY_JOB_RESULT_ENTRY.getJobId()).join())
                .isTrue();
    }

    @Test
    default void testHasJobResultEntryWithEmptyStore() throws IOException {
        JobResultStore jobResultStore = createJobResultStore();
        JobID jobId = new JobID();
        assertThat(jobResultStore.hasDirtyJobResultEntryAsync(jobId).join()).isFalse();
        assertThat(jobResultStore.hasCleanJobResultEntryAsync(jobId).join()).isFalse();
        assertThat(jobResultStore.hasJobResultEntryAsync(jobId).join()).isFalse();
    }

    @Test
    default void testGetDirtyResultsWithNoEntry() throws IOException {
        JobResultStore jobResultStore = createJobResultStore();
        assertThat(jobResultStore.getDirtyResults()).isEmpty();
    }

    @Test
    default void testGetDirtyResultsWithDirtyEntry() throws IOException {
        JobResultStore jobResultStore = createJobResultStore();
        jobResultStore.createDirtyResultAsync(DUMMY_JOB_RESULT_ENTRY).join();
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
        jobResultStore.createDirtyResultAsync(DUMMY_JOB_RESULT_ENTRY).join();
        jobResultStore.markResultAsCleanAsync(DUMMY_JOB_RESULT_ENTRY.getJobId()).join();

        final JobResultEntry otherDirtyJobResultEntry =
                new JobResultEntry(TestingJobResultStore.createSuccessfulJobResult(new JobID()));
        jobResultStore.createDirtyResultAsync(otherDirtyJobResultEntry).join();

        assertThat(
                        jobResultStore.getDirtyResults().stream()
                                .map(JobResult::getJobId)
                                .collect(Collectors.toList()))
                .singleElement()
                .isEqualTo(otherDirtyJobResultEntry.getJobId());
    }
}
