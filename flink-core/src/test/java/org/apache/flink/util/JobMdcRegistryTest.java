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

package org.apache.flink.util;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.function.Consumer;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link JobMdcRegistry}. */
class JobMdcRegistryTest {

    @AfterEach
    void clearRegistry() {
        JobMdcRegistry.clear();
    }

    @Test
    void testEnrichedContextStoredOnRegister() {
        final JobID jobID = new JobID();
        JobMdcRegistry.registerOrClear(
                jobID, MdcTestFixtures.enrichedConfiguration("val-1", "val-2"));
        assertThat(JobMdcRegistry.lookup(jobID))
                .containsEntry(MdcUtils.JOB_ID, jobID.toHexString())
                .containsEntry("mdc-key-1", "val-1")
                .containsEntry("mdc-key-2", "val-2")
                .hasSize(3);
    }

    private static Stream<Arguments> clearingActions() {
        return Stream.of(
                // explicit unregister — also verifies idempotency (double unregister stays null)
                Arguments.of(
                        "after explicit unregister",
                        (Consumer<JobID>)
                                jobID -> {
                                    JobMdcRegistry.unregister(jobID);
                                    assertThat(JobMdcRegistry.lookup(jobID)).isNull();
                                    JobMdcRegistry.unregister(jobID);
                                }),
                // unenriched config on a fresh job stores nothing
                Arguments.of(
                        "after registerOrClear with empty config (no prior entry)",
                        (Consumer<JobID>)
                                jobID ->
                                        JobMdcRegistry.registerOrClear(jobID, new Configuration())),
                // unenriched config overwrites an existing enriched entry
                Arguments.of(
                        "after registerOrClear with empty config (clears prior entry)",
                        (Consumer<JobID>)
                                jobID -> {
                                    JobMdcRegistry.registerOrClear(
                                            jobID, MdcTestFixtures.enrichedConfiguration("val-1"));
                                    assertThat(JobMdcRegistry.lookup(jobID)).isNotNull();
                                    JobMdcRegistry.registerOrClear(jobID, new Configuration());
                                }));
    }

    @ParameterizedTest
    @MethodSource("clearingActions")
    void testLookupReturnsNullAfterRemoval(String scenario, Consumer<JobID> clearAction) {
        final JobID jobID = new JobID();
        clearAction.accept(jobID);
        assertThat(JobMdcRegistry.lookup(jobID)).as(scenario).isNull();
    }

    @Test
    void testLatestEnrichmentWinsOnReRegister() {
        final JobID jobID = new JobID();
        JobMdcRegistry.registerOrClear(jobID, MdcTestFixtures.enrichedConfiguration("val-first"));
        JobMdcRegistry.registerOrClear(jobID, MdcTestFixtures.enrichedConfiguration("val-second"));
        assertThat(JobMdcRegistry.lookup(jobID)).containsEntry("mdc-key-1", "val-second");
    }

    @Test
    void testContextIsolatedPerJob() {
        final JobID first = new JobID();
        final JobID second = new JobID();
        JobMdcRegistry.registerOrClear(first, MdcTestFixtures.enrichedConfiguration("val-first"));
        JobMdcRegistry.registerOrClear(second, MdcTestFixtures.enrichedConfiguration("val-second"));
        assertThat(JobMdcRegistry.lookup(first)).containsEntry("mdc-key-1", "val-first");
        assertThat(JobMdcRegistry.lookup(second)).containsEntry("mdc-key-1", "val-second");
    }
}
