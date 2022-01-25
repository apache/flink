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

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.highavailability.JobResultEntry;
import org.apache.flink.runtime.testutils.TestingJobResultStore;
import org.apache.flink.util.TestLoggerExtension;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.NoSuchElementException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.assertj.core.api.AssertionsForClassTypes.entry;

/** Tests for the {@link EmbeddedJobResultStore}. */
@ExtendWith(TestLoggerExtension.class)
public class EmbeddedJobResultStoreTest {

    private static final JobResultEntry DUMMY_JOB_RESULT_ENTRY =
            new JobResultEntry(TestingJobResultStore.DUMMY_JOB_RESULT);

    private EmbeddedJobResultStore embeddedJobResultStore;

    @BeforeEach
    public void setupTest() {
        embeddedJobResultStore = new EmbeddedJobResultStore();
    }

    @Test
    public void testStoreDirtyJobResult() throws Exception {
        embeddedJobResultStore.createDirtyResult(DUMMY_JOB_RESULT_ENTRY);
        assertThat(embeddedJobResultStore.hasJobResultEntry(DUMMY_JOB_RESULT_ENTRY.getJobId()))
                .isTrue();

        assertThat(embeddedJobResultStore.dirtyJobResults)
                .containsExactly(entry(DUMMY_JOB_RESULT_ENTRY.getJobId(), DUMMY_JOB_RESULT_ENTRY));
        assertThat(embeddedJobResultStore.cleanJobResults).isEmpty();
    }

    @Test
    public void testStoreDirtyJobResultTwice() {
        embeddedJobResultStore.createDirtyResult(DUMMY_JOB_RESULT_ENTRY);

        assertThatThrownBy(() -> embeddedJobResultStore.createDirtyResult(DUMMY_JOB_RESULT_ENTRY))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void testStoreDirtyJobResultForCleanJobEntry() {
        embeddedJobResultStore.createDirtyResult(DUMMY_JOB_RESULT_ENTRY);
        embeddedJobResultStore.markResultAsClean(DUMMY_JOB_RESULT_ENTRY.getJobId());

        assertThatThrownBy(() -> embeddedJobResultStore.createDirtyResult(DUMMY_JOB_RESULT_ENTRY))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void testCleanDirtyJobResult() throws Exception {
        embeddedJobResultStore.createDirtyResult(DUMMY_JOB_RESULT_ENTRY);
        embeddedJobResultStore.markResultAsClean(DUMMY_JOB_RESULT_ENTRY.getJobId());

        assertThat(embeddedJobResultStore.dirtyJobResults).isEmpty();
        assertThat(embeddedJobResultStore.cleanJobResults)
                .containsExactly(entry(DUMMY_JOB_RESULT_ENTRY.getJobId(), DUMMY_JOB_RESULT_ENTRY));
    }

    @Test
    public void testCleanDirtyJobResultTwice() {
        final JobID jobId = DUMMY_JOB_RESULT_ENTRY.getJobId();
        embeddedJobResultStore.createDirtyResult(DUMMY_JOB_RESULT_ENTRY);
        embeddedJobResultStore.markResultAsClean(jobId);

        assertThat(embeddedJobResultStore.cleanJobResults)
                .containsExactly(entry(jobId, DUMMY_JOB_RESULT_ENTRY));
        embeddedJobResultStore.markResultAsClean(jobId);
        assertThat(embeddedJobResultStore.cleanJobResults)
                .as("Marking the same job %s as clean should be idempotent.", jobId)
                .containsExactly(entry(jobId, DUMMY_JOB_RESULT_ENTRY));
    }

    @Test
    public void testCleanNonExistentJobResult() throws Exception {
        assertThatThrownBy(() -> embeddedJobResultStore.markResultAsClean(new JobID()))
                .isInstanceOf(NoSuchElementException.class);
    }

    @Test
    public void testHasJobResultEntryWithNoEntry() {
        assertThat(embeddedJobResultStore.hasJobResultEntry(new JobID())).isFalse();
    }

    @Test
    public void testHasJobResultEntryWithDirtyEntry() {
        embeddedJobResultStore.createDirtyResult(DUMMY_JOB_RESULT_ENTRY);

        assertThat(embeddedJobResultStore.hasJobResultEntry(DUMMY_JOB_RESULT_ENTRY.getJobId()))
                .isTrue();
    }

    @Test
    public void testHasJobResultEntryWithCleanEntry() {
        embeddedJobResultStore.createDirtyResult(DUMMY_JOB_RESULT_ENTRY);
        embeddedJobResultStore.markResultAsClean(DUMMY_JOB_RESULT_ENTRY.getJobId());

        assertThat(embeddedJobResultStore.hasJobResultEntry(DUMMY_JOB_RESULT_ENTRY.getJobId()))
                .isTrue();
    }

    @Test
    public void testHasDirtyJobResultEntryWithNoDirtyEntry() {
        assertThat(embeddedJobResultStore.hasDirtyJobResultEntry(new JobID())).isFalse();
    }

    @Test
    public void testHasDirtyJobResultEntryWithDirtyEntry() {
        embeddedJobResultStore.createDirtyResult(DUMMY_JOB_RESULT_ENTRY);

        assertThat(embeddedJobResultStore.hasDirtyJobResultEntry(DUMMY_JOB_RESULT_ENTRY.getJobId()))
                .isTrue();
    }

    @Test
    public void testHasDirtyJobResultEntryWithCleanEntry() {
        embeddedJobResultStore.createDirtyResult(DUMMY_JOB_RESULT_ENTRY);
        embeddedJobResultStore.markResultAsClean(DUMMY_JOB_RESULT_ENTRY.getJobId());

        assertThat(embeddedJobResultStore.hasDirtyJobResultEntry(DUMMY_JOB_RESULT_ENTRY.getJobId()))
                .isFalse();
    }

    @Test
    public void testHasCleanJobResultEntryWithNoEntry() {
        assertThat(embeddedJobResultStore.hasCleanJobResultEntry(new JobID())).isFalse();
    }

    @Test
    public void testHasCleanJobResultEntryWithDirtyEntry() {
        embeddedJobResultStore.createDirtyResult(DUMMY_JOB_RESULT_ENTRY);
        assertThat(embeddedJobResultStore.hasCleanJobResultEntry(DUMMY_JOB_RESULT_ENTRY.getJobId()))
                .isFalse();
    }

    @Test
    public void testHasCleanJobResultEntryWithCleanEntry() {
        embeddedJobResultStore.createDirtyResult(DUMMY_JOB_RESULT_ENTRY);
        embeddedJobResultStore.markResultAsClean(DUMMY_JOB_RESULT_ENTRY.getJobId());

        assertThat(embeddedJobResultStore.hasCleanJobResultEntry(DUMMY_JOB_RESULT_ENTRY.getJobId()))
                .isTrue();
    }

    @Test
    public void testGetDirtyResultsWithNoEntry() {
        assertThat(embeddedJobResultStore.getDirtyResults()).isEmpty();
    }

    @Test
    public void testGetDirtyResultsWithDirtyEntry() {
        embeddedJobResultStore.createDirtyResult(DUMMY_JOB_RESULT_ENTRY);

        assertThat(embeddedJobResultStore.getDirtyResults())
                .containsExactlyInAnyOrder(DUMMY_JOB_RESULT_ENTRY.getJobResult());
    }

    @Test
    public void testGetDirtyResultsWithDirtyAndCleanEntry() {
        embeddedJobResultStore.createDirtyResult(DUMMY_JOB_RESULT_ENTRY);
        embeddedJobResultStore.markResultAsClean(DUMMY_JOB_RESULT_ENTRY.getJobId());

        final JobResultEntry otherDirtyJobResultEntry =
                new JobResultEntry(TestingJobResultStore.createSuccessfulJobResult(new JobID()));
        embeddedJobResultStore.createDirtyResult(otherDirtyJobResultEntry);

        assertThat(embeddedJobResultStore.dirtyJobResults)
                .containsExactly(
                        entry(otherDirtyJobResultEntry.getJobId(), otherDirtyJobResultEntry));
        assertThat(embeddedJobResultStore.cleanJobResults)
                .containsExactly(entry(DUMMY_JOB_RESULT_ENTRY.getJobId(), DUMMY_JOB_RESULT_ENTRY));

        assertThat(embeddedJobResultStore.getDirtyResults())
                .containsExactlyInAnyOrder(otherDirtyJobResultEntry.getJobResult());
    }
}
