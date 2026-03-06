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

import java.io.IOException;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * A storage for the results of globally terminated applications. These results can have the
 * following states:
 *
 * <ul>
 *   <li>{@code dirty} - indicating that the corresponding application is not properly cleaned up,
 *       yet.
 *   <li>{@code clean} - indicating that the cleanup of the corresponding application is performed
 *       and no further actions need to be applied.
 * </ul>
 */
public interface ApplicationResultStore {

    /**
     * Registers the passed {@link ApplicationResultEntry} instance as {@code dirty} which indicates
     * that clean-up operations still need to be performed. Once the application resource cleanup
     * has been finalized, we can mark the {@code ApplicationEntry} as {@code clean} result using
     * {@link #markResultAsCleanAsync(ApplicationID)}.
     *
     * @param applicationResultEntry The application result we wish to persist.
     * @return a successfully completed future if the dirty result is created successfully. The
     *     future will be completed with {@link IllegalStateException} if the passed {@code
     *     applicationEntry} has an {@code ApplicationID} attached that is already registered in
     *     this {@code ApplicationResultStore}.
     */
    CompletableFuture<Void> createDirtyResultAsync(ApplicationResultEntry applicationResultEntry);

    /**
     * Marks an existing {@link ApplicationResultEntry} as {@code clean}. This indicates that no
     * more resource cleanup steps need to be performed. No actions should be triggered if the
     * passed {@code ApplicationID} belongs to an application that was already marked as clean.
     *
     * @param applicationId Ident of the application we wish to mark as clean.
     * @return a successfully completed future if the result is marked successfully. The future can
     *     complete exceptionally with a {@link NoSuchElementException}. i.e. there is no
     *     corresponding {@code dirty} application present in the store for the given {@code
     *     ApplicationID}.
     */
    CompletableFuture<Void> markResultAsCleanAsync(ApplicationID applicationId);

    /**
     * Returns the future of whether the store already contains an entry for an application.
     *
     * @param applicationId Ident of the application we wish to check the store for.
     * @return a successfully completed future with {@code true} if a {@code dirty} or {@code clean}
     *     {@link ApplicationResultEntry} exists for the given {@code ApplicationID}; otherwise
     *     {@code false}.
     */
    default CompletableFuture<Boolean> hasApplicationResultEntryAsync(ApplicationID applicationId) {
        return hasDirtyApplicationResultEntryAsync(applicationId)
                .thenCombine(
                        hasCleanApplicationResultEntryAsync(applicationId),
                        (result1, result2) -> result1 || result2);
    }

    /**
     * Returns the future of whether the store contains a {@code dirty} entry for the given {@code
     * ApplicationID}.
     *
     * @param applicationId Ident of the application we wish to check the store for.
     * @return a successfully completed future with {@code true}, if a {@code dirty} entry exists
     *     for the given {@code ApplicationID}; otherwise {@code false}.
     */
    CompletableFuture<Boolean> hasDirtyApplicationResultEntryAsync(ApplicationID applicationId);

    /**
     * Returns the future of whether the store contains a {@code clean} entry for the given {@code
     * ApplicationID}.
     *
     * @param applicationId Ident of the application we wish to check the store for.
     * @return a successfully completed future with {@code true}, if a {@code clean} entry exists
     *     for the given {@code ApplicationID}; otherwise a successfully completed future with
     *     {@code false}.
     */
    CompletableFuture<Boolean> hasCleanApplicationResultEntryAsync(ApplicationID applicationId);

    /**
     * Get the persisted {@link ApplicationResult} instances that are marked as {@code dirty}. This
     * is useful for recovery of finalization steps.
     *
     * @return A set of dirty {@code ApplicationResults} from the store.
     * @throws IOException if collecting the set of dirty results failed for IO reasons.
     */
    Set<ApplicationResult> getDirtyResults() throws IOException;
}
