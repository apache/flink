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
import org.apache.flink.util.concurrent.Executors;

import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/** An embedded implementation of {@link ApplicationResultStore} for testing purposes. */
public class EmbeddedApplicationResultStore extends AbstractThreadsafeApplicationResultStore {

    private final Map<ApplicationID, ApplicationResultEntry> dirtyResults =
            new ConcurrentHashMap<>();
    private final Map<ApplicationID, ApplicationResultEntry> cleanResults =
            new ConcurrentHashMap<>();

    public EmbeddedApplicationResultStore() {
        super(Executors.directExecutor());
    }

    @Override
    protected void createDirtyResultInternal(ApplicationResultEntry applicationResultEntry) {
        dirtyResults.put(applicationResultEntry.getApplicationId(), applicationResultEntry);
    }

    @Override
    protected void markResultAsCleanInternal(ApplicationID applicationId)
            throws NoSuchElementException {
        final ApplicationResultEntry entry = dirtyResults.remove(applicationId);
        if (entry != null) {
            cleanResults.put(applicationId, entry);
        } else {
            throw new NoSuchElementException(
                    String.format(
                            "Could not mark application %s as clean as it is not present in the application result store.",
                            applicationId));
        }
    }

    @Override
    protected boolean hasDirtyApplicationResultEntryInternal(ApplicationID applicationId) {
        return dirtyResults.containsKey(applicationId);
    }

    @Override
    protected boolean hasCleanApplicationResultEntryInternal(ApplicationID applicationId) {
        return cleanResults.containsKey(applicationId);
    }

    @Override
    protected Set<ApplicationResult> getDirtyResultsInternal() {
        return dirtyResults.values().stream()
                .map(ApplicationResultEntry::getApplicationResult)
                .collect(Collectors.toSet());
    }

    @Override
    protected ApplicationResult getCleanApplicationResultInternal(ApplicationID applicationId) {
        final ApplicationResultEntry entry = cleanResults.get(applicationId);
        if (entry == null) {
            return null;
        }

        return entry.getApplicationResult();
    }

    /** Clears all stored results. */
    public void clear() {
        dirtyResults.clear();
        cleanResults.clear();
    }

    /** Gets the number of dirty results. */
    public int getDirtyResultCount() {
        return dirtyResults.size();
    }

    /** Gets the number of clean results. */
    public int getCleanResultCount() {
        return cleanResults.size();
    }
}
