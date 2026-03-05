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

import org.apache.flink.api.common.ApplicationID;
import org.apache.flink.runtime.testutils.TestingApplicationResultStore;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletionException;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

/**
 * This interface defines a series of tests for any implementation of the {@link
 * ApplicationResultStore} to determine whether they correctly implement the contracts defined by
 * the interface.
 */
public interface ApplicationResultStoreContractTest {

    ApplicationResultEntry DUMMY_APPLICATION_RESULT_ENTRY =
            new ApplicationResultEntry(TestingApplicationResultStore.DUMMY_APPLICATION_RESULT);

    ApplicationResultStore createApplicationResultStore() throws IOException;

    @Test
    default void testStoreApplicationResultsWithDuplicateIDsThrowsException() throws IOException {
        ApplicationResultStore applicationResultStore = createApplicationResultStore();
        applicationResultStore.createDirtyResultAsync(DUMMY_APPLICATION_RESULT_ENTRY).join();
        final ApplicationResultEntry otherEntryWithDuplicateId =
                new ApplicationResultEntry(
                        TestingApplicationResultStore.createSuccessfulApplicationResult(
                                DUMMY_APPLICATION_RESULT_ENTRY.getApplicationId()));
        assertThatThrownBy(
                        () ->
                                applicationResultStore
                                        .createDirtyResultAsync(otherEntryWithDuplicateId)
                                        .join())
                .isInstanceOf(CompletionException.class)
                .hasCauseInstanceOf(IllegalStateException.class);
    }

    @Test
    default void testStoreDirtyEntryForAlreadyCleanedApplicationResultThrowsException()
            throws IOException {
        ApplicationResultStore applicationResultStore = createApplicationResultStore();
        applicationResultStore.createDirtyResultAsync(DUMMY_APPLICATION_RESULT_ENTRY).join();
        applicationResultStore
                .markResultAsCleanAsync(DUMMY_APPLICATION_RESULT_ENTRY.getApplicationId())
                .join();
        assertThatThrownBy(
                        () ->
                                applicationResultStore
                                        .createDirtyResultAsync(DUMMY_APPLICATION_RESULT_ENTRY)
                                        .join())
                .isInstanceOf(CompletionException.class)
                .hasCauseInstanceOf(IllegalStateException.class);
    }

    @Test
    default void testCleaningDuplicateEntryThrowsNoException() throws IOException {
        ApplicationResultStore applicationResultStore = createApplicationResultStore();
        applicationResultStore.createDirtyResultAsync(DUMMY_APPLICATION_RESULT_ENTRY).join();
        applicationResultStore
                .markResultAsCleanAsync(DUMMY_APPLICATION_RESULT_ENTRY.getApplicationId())
                .join();
        assertThatNoException()
                .isThrownBy(
                        () ->
                                applicationResultStore
                                        .markResultAsCleanAsync(
                                                DUMMY_APPLICATION_RESULT_ENTRY.getApplicationId())
                                        .join());
    }

    @Test
    default void testCleaningNonExistentEntryThrowsException() throws IOException {
        ApplicationResultStore applicationResultStore = createApplicationResultStore();
        assertThatThrownBy(
                        () ->
                                applicationResultStore
                                        .markResultAsCleanAsync(
                                                DUMMY_APPLICATION_RESULT_ENTRY.getApplicationId())
                                        .join())
                .hasCauseInstanceOf(NoSuchElementException.class);
    }

    @Test
    default void testHasApplicationResultEntryWithDirtyEntry() throws IOException {
        ApplicationResultStore applicationResultStore = createApplicationResultStore();
        applicationResultStore.createDirtyResultAsync(DUMMY_APPLICATION_RESULT_ENTRY).join();
        assertThat(
                        applicationResultStore
                                .hasDirtyApplicationResultEntryAsync(
                                        DUMMY_APPLICATION_RESULT_ENTRY.getApplicationId())
                                .join())
                .isTrue();
        assertThat(
                        applicationResultStore
                                .hasCleanApplicationResultEntryAsync(
                                        DUMMY_APPLICATION_RESULT_ENTRY.getApplicationId())
                                .join())
                .isFalse();
        assertThat(
                        applicationResultStore
                                .hasApplicationResultEntryAsync(
                                        DUMMY_APPLICATION_RESULT_ENTRY.getApplicationId())
                                .join())
                .isTrue();
    }

    @Test
    default void testHasApplicationResultEntryWithCleanEntry() throws IOException {
        ApplicationResultStore applicationResultStore = createApplicationResultStore();
        applicationResultStore.createDirtyResultAsync(DUMMY_APPLICATION_RESULT_ENTRY).join();
        applicationResultStore
                .markResultAsCleanAsync(DUMMY_APPLICATION_RESULT_ENTRY.getApplicationId())
                .join();
        assertThat(
                        applicationResultStore
                                .hasDirtyApplicationResultEntryAsync(
                                        DUMMY_APPLICATION_RESULT_ENTRY.getApplicationId())
                                .join())
                .isFalse();
        assertThat(
                        applicationResultStore
                                .hasCleanApplicationResultEntryAsync(
                                        DUMMY_APPLICATION_RESULT_ENTRY.getApplicationId())
                                .join())
                .isTrue();
        assertThat(
                        applicationResultStore
                                .hasApplicationResultEntryAsync(
                                        DUMMY_APPLICATION_RESULT_ENTRY.getApplicationId())
                                .join())
                .isTrue();
    }

    @Test
    default void testHasApplicationResultEntryWithEmptyStore() throws IOException {
        ApplicationResultStore applicationResultStore = createApplicationResultStore();
        ApplicationID applicationId = new ApplicationID();
        assertThat(applicationResultStore.hasDirtyApplicationResultEntryAsync(applicationId).join())
                .isFalse();
        assertThat(applicationResultStore.hasCleanApplicationResultEntryAsync(applicationId).join())
                .isFalse();
        assertThat(applicationResultStore.hasApplicationResultEntryAsync(applicationId).join())
                .isFalse();
    }

    @Test
    default void testGetDirtyResultsWithNoEntry() throws IOException {
        ApplicationResultStore applicationResultStore = createApplicationResultStore();
        assertThat(applicationResultStore.getDirtyResults()).isEmpty();
    }

    @Test
    default void testGetDirtyResultsWithDirtyEntry() throws IOException {
        ApplicationResultStore applicationResultStore = createApplicationResultStore();
        applicationResultStore.createDirtyResultAsync(DUMMY_APPLICATION_RESULT_ENTRY).join();
        assertThat(
                        applicationResultStore.getDirtyResults().stream()
                                .map(ApplicationResult::getApplicationId)
                                .collect(Collectors.toList()))
                .singleElement()
                .isEqualTo(DUMMY_APPLICATION_RESULT_ENTRY.getApplicationId());
    }

    @Test
    default void testGetDirtyResultsWithDirtyAndCleanEntry() throws IOException {
        ApplicationResultStore applicationResultStore = createApplicationResultStore();
        applicationResultStore.createDirtyResultAsync(DUMMY_APPLICATION_RESULT_ENTRY).join();
        applicationResultStore
                .markResultAsCleanAsync(DUMMY_APPLICATION_RESULT_ENTRY.getApplicationId())
                .join();

        final ApplicationResultEntry otherDirtyApplicationResultEntry =
                new ApplicationResultEntry(
                        TestingApplicationResultStore.createSuccessfulApplicationResult(
                                new ApplicationID()));
        applicationResultStore.createDirtyResultAsync(otherDirtyApplicationResultEntry).join();

        assertThat(
                        applicationResultStore.getDirtyResults().stream()
                                .map(ApplicationResult::getApplicationId)
                                .collect(Collectors.toList()))
                .singleElement()
                .isEqualTo(otherDirtyApplicationResultEntry.getApplicationId());
    }
}
