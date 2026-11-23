/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.kubernetes.highavailability;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.ApplicationID;
import org.apache.flink.api.common.ApplicationState;
import org.apache.flink.kubernetes.kubeclient.FlinkKubeClient;
import org.apache.flink.runtime.highavailability.AbstractThreadsafeApplicationResultStore;
import org.apache.flink.runtime.highavailability.ApplicationResult;
import org.apache.flink.runtime.highavailability.ApplicationResultEntry;
import org.apache.flink.runtime.highavailability.ApplicationResultStore;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.function.BooleanSupplier;

/**
 * An {@link ApplicationResultStore} backed by a Kubernetes ConfigMap of the cluster. Each
 * application has one entry: the key is the application ID, the value is the {@link
 * ApplicationResult} prefixed with {@link #DIRTY_PREFIX} or {@link #CLEAN_PREFIX}. A retained clean
 * entry keeps the result, which a cluster in application mode reads after a failover to report the
 * status of its finished application. The {@link KubernetesResultStoreFencedClient} fences the
 * updates by a leadership check and makes them idempotent, so the Kubernetes client may retry them.
 */
public class KubernetesApplicationResultStore extends AbstractThreadsafeApplicationResultStore {

    @VisibleForTesting static final String DIRTY_PREFIX = "DIRTY:";

    @VisibleForTesting static final String CLEAN_PREFIX = "CLEAN:";

    private static final String FIELD_SEPARATOR = ":";

    private static final int FIELD_COUNT = 4;

    private final KubernetesResultStoreFencedClient fencedClient;

    /**
     * @param kubeClient the client used to access the ConfigMaps.
     * @param configMapName the name of the ConfigMap holding the entries.
     * @param clusterId the cluster ID that determines the labels of the ConfigMap.
     * @param leadershipCheck tells whether this JobManager holds the leadership; it is evaluated
     *     right before every update, and an update without leadership fails.
     * @param deleteOnCommit whether entries are deleted instead of marked as clean.
     * @param ioExecutor the executor running the store operations.
     */
    public KubernetesApplicationResultStore(
            FlinkKubeClient kubeClient,
            String configMapName,
            String clusterId,
            BooleanSupplier leadershipCheck,
            boolean deleteOnCommit,
            Executor ioExecutor) {
        super(ioExecutor);
        this.fencedClient =
                new KubernetesResultStoreFencedClient(
                        kubeClient,
                        configMapName,
                        clusterId,
                        leadershipCheck,
                        deleteOnCommit,
                        value -> value.startsWith(CLEAN_PREFIX),
                        dirtyValue -> CLEAN_PREFIX + dirtyValue.substring(DIRTY_PREFIX.length()));
    }

    @Override
    protected void createDirtyResultInternal(ApplicationResultEntry applicationResultEntry)
            throws IOException {
        fencedClient.createDirtyEntry(
                keyOf(applicationResultEntry.getApplicationId()),
                DIRTY_PREFIX + encodeResult(applicationResultEntry.getApplicationResult()));
    }

    @Override
    protected void markResultAsCleanInternal(ApplicationID applicationId)
            throws IOException, NoSuchElementException {
        fencedClient.markEntryAsClean(keyOf(applicationId));
    }

    @Override
    protected boolean hasDirtyApplicationResultEntryInternal(ApplicationID applicationId) {
        return fencedClient.hasDirtyEntry(keyOf(applicationId));
    }

    @Override
    protected boolean hasCleanApplicationResultEntryInternal(ApplicationID applicationId) {
        return fencedClient.hasCleanEntry(keyOf(applicationId));
    }

    @Override
    protected Set<ApplicationResult> getDirtyResultsInternal() throws IOException {
        final Set<ApplicationResult> dirtyResults = new HashSet<>();
        for (Map.Entry<String, String> entry : fencedClient.getDirtyEntries().entrySet()) {
            dirtyResults.add(decodeValue(entry.getKey(), entry.getValue(), DIRTY_PREFIX));
        }
        return dirtyResults;
    }

    @Override
    @Nullable
    protected ApplicationResult getCleanApplicationResultInternal(ApplicationID applicationId)
            throws IOException {
        final String key = keyOf(applicationId);
        final Optional<String> value =
                fencedClient.getValue(key).filter(fencedClient::isCleanValue);
        if (!value.isPresent()) {
            return null;
        }
        return decodeValue(key, value.get(), CLEAN_PREFIX);
    }

    private static String keyOf(ApplicationID applicationId) {
        return applicationId.toHexString();
    }

    /**
     * The result as {@code <state>:<start time>:<end time>:<name>}. The name comes last because it
     * may contain the separator.
     */
    @VisibleForTesting
    static String encodeResult(ApplicationResult result) {
        return result.getApplicationState().name()
                + FIELD_SEPARATOR
                + result.getStartTime()
                + FIELD_SEPARATOR
                + result.getEndTime()
                + FIELD_SEPARATOR
                + result.getApplicationName();
    }

    /** The counterpart of {@link #encodeResult}; fails on a value the store did not write. */
    private ApplicationResult decodeValue(String key, String value, String expectedPrefix)
            throws IOException {
        try {
            if (!value.startsWith(expectedPrefix)) {
                throw new IllegalArgumentException(
                        String.format("The value does not start with %s.", expectedPrefix));
            }
            final String[] fields =
                    value.substring(expectedPrefix.length()).split(FIELD_SEPARATOR, FIELD_COUNT);
            if (fields.length != FIELD_COUNT) {
                throw new IllegalArgumentException(
                        String.format(
                                "The value has %d fields instead of %d.",
                                fields.length, FIELD_COUNT));
            }
            return new ApplicationResult.Builder()
                    .applicationId(ApplicationID.fromHexString(key))
                    .applicationState(ApplicationState.valueOf(fields[0]))
                    .startTime(Long.parseLong(fields[1]))
                    .endTime(Long.parseLong(fields[2]))
                    .applicationName(fields[3])
                    .build();
        } catch (IllegalArgumentException e) {
            throw new IOException(
                    String.format(
                            "Could not parse the application result entry %s=%s in ConfigMap %s.",
                            key, value, fencedClient.getConfigMapName()),
                    e);
        }
    }
}
