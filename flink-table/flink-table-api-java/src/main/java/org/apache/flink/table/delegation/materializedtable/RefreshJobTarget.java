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

package org.apache.flink.table.delegation.materializedtable;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.refresh.ContinuousRefreshHandler;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** The execution target and cluster id a materialized table refresh job is submitted to. */
@Internal
public final class RefreshJobTarget {

    private static final String YARN_APPLICATION_ID_KEY = "yarn.application.id";
    private static final String KUBERNETES_CLUSTER_ID_KEY = "kubernetes.cluster-id";

    private static final String LOCAL_TARGET = "local";
    private static final String EMBEDDED_TARGET = "embedded";
    private static final String REMOTE_TARGET = "remote";
    private static final String STANDALONE_CLUSTER_ID = "StandaloneClusterId";

    private final String executionTarget;
    private final String clusterId;

    public RefreshJobTarget(String executionTarget, String clusterId) {
        this.executionTarget = checkNotNull(executionTarget);
        this.clusterId = checkNotNull(clusterId);
        checkArgument(!executionTarget.isEmpty(), "The execution target must not be empty.");
    }

    /** The target a new refresh job of a program is submitted to. */
    public static RefreshJobTarget forNewJob(ReadableConfig config) {
        final String executionTarget = config.getOptional(DeploymentOptions.TARGET).orElse(null);
        if (executionTarget == null
                || executionTarget.isEmpty()
                || LOCAL_TARGET.equals(executionTarget)
                || EMBEDDED_TARGET.equals(executionTarget)) {
            throw new ValidationException(
                    String.format(
                            "Unsupported execution target detected: %s. Materialized table refresh "
                                    + "jobs need a session cluster: set 'execution.target' to "
                                    + "'remote', 'yarn-session' or 'kubernetes-session' (with "
                                    + "'yarn.application.id' or 'kubernetes.cluster-id').",
                            executionTarget));
        }
        final RefreshJobTarget unresolvedTarget = new RefreshJobTarget(executionTarget, "");
        if (unresolvedTarget.isApplicationTarget()) {
            throw new ValidationException(
                    String.format(
                            "Application-mode materialized table refresh is not supported in this "
                                    + "environment: execution target '%s' requires the SQL Gateway. Use a "
                                    + "session cluster target such as 'remote', 'yarn-session' or "
                                    + "'kubernetes-session'.",
                            executionTarget));
        }
        return new RefreshJobTarget(executionTarget, unresolvedTarget.resolveClusterId(config));
    }

    /** The target for restarting a recorded refresh job. */
    public static RefreshJobTarget forRestart(
            ReadableConfig config,
            ObjectIdentifier tableIdentifier,
            ContinuousRefreshHandler refreshHandler) {
        final RefreshJobTarget recordedTarget = recordedIn(refreshHandler);
        if (recordedTarget.isApplicationTarget()) {
            throw new ValidationException(
                    String.format(
                            "The continuous refresh job of materialized table %s runs in its own "
                                    + "application cluster (%s). Restarting it deploys a new "
                                    + "application cluster, which only the SQL Gateway can do, so "
                                    + "resume, alter or redeploy the table through the SQL Gateway. "
                                    + "Suspending, refreshing and dropping it work here.",
                            tableIdentifier, recordedTarget.describe()));
        }

        final RefreshJobTarget programTarget = forNewJob(config);
        final boolean sameTarget =
                recordedTarget.executionTarget.equals(programTarget.executionTarget);
        final boolean sameCluster =
                !recordedTarget.identifiesClusterById()
                        || recordedTarget.clusterId.equals(programTarget.clusterId);
        if (!sameTarget || !sameCluster) {
            throw new ValidationException(
                    String.format(
                            "The continuous refresh job of materialized table %s runs on %s, but "
                                    + "this program submits refresh jobs to %s. RESUME, ALTER and a "
                                    + "redeploy never move a refresh job to another cluster. Set "
                                    + "'execution.target' and its cluster id to the recorded values "
                                    + "to restart it here.",
                            tableIdentifier, recordedTarget.describe(), programTarget.describe()));
        }
        return programTarget;
    }

    /** The target a refresh handler records. */
    public static RefreshJobTarget recordedIn(ContinuousRefreshHandler refreshHandler) {
        final String clusterId = refreshHandler.getClusterId();
        return new RefreshJobTarget(
                refreshHandler.getExecutionTarget(), clusterId == null ? "" : clusterId);
    }

    public String getExecutionTarget() {
        return executionTarget;
    }

    /** The cluster id, or an empty string when it is not known. */
    public String getClusterId() {
        return clusterId;
    }

    /** Whether the target deploys each job in its own application cluster. */
    public boolean isApplicationTarget() {
        return executionTarget.endsWith("application");
    }

    public void applyTo(Configuration configuration) {
        configuration.set(DeploymentOptions.TARGET, executionTarget);
        if (identifiesClusterById()) {
            configuration.setString(clusterIdKey().get(), clusterId);
        }
    }

    public Map<String, String> toClusterInfo() {
        final Map<String, String> clusterInfo = new HashMap<>();
        clusterInfo.put(DeploymentOptions.TARGET.key(), executionTarget);
        clusterIdKey().ifPresent(key -> clusterInfo.put(key, clusterId));
        return Collections.unmodifiableMap(clusterInfo);
    }

    private Optional<String> clusterIdKey() {
        if (executionTarget.startsWith("yarn")) {
            return Optional.of(YARN_APPLICATION_ID_KEY);
        } else if (executionTarget.startsWith("kubernetes")) {
            return Optional.of(KUBERNETES_CLUSTER_ID_KEY);
        }
        return Optional.empty();
    }

    private boolean identifiesClusterById() {
        return !clusterId.isEmpty() && clusterIdKey().isPresent();
    }

    private String describe() {
        if (identifiesClusterById()) {
            return String.format(
                    "execution.target '%s', %s '%s'",
                    executionTarget, clusterIdKey().get(), clusterId);
        }
        return String.format("execution.target '%s'", executionTarget);
    }

    private String resolveClusterId(ReadableConfig config) {
        final Optional<String> clusterIdKey = clusterIdKey();
        if (clusterIdKey.isEmpty()) {
            // record StandaloneClusterId#toString for 'remote'
            return REMOTE_TARGET.equals(executionTarget) ? STANDALONE_CLUSTER_ID : "";
        }
        return config.getOptional(
                        ConfigOptions.key(clusterIdKey.get()).stringType().noDefaultValue())
                .filter(configuredClusterId -> !configuredClusterId.isBlank())
                .orElseThrow(
                        () ->
                                new ValidationException(
                                        String.format(
                                                "Execution target '%s' requires '%s' to be configured to submit or control a materialized table refresh job.",
                                                executionTarget, clusterIdKey.get())));
    }
}
