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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.refresh.ContinuousRefreshHandler;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nullable;

import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link RefreshJobTarget}. */
class RefreshJobTargetTest {

    private static final ObjectIdentifier TABLE = ObjectIdentifier.of("cat", "db", "mt");
    private static final String CLUSTER_MOVE_FRAGMENT =
            "never move a refresh job to another cluster";

    @ParameterizedTest(name = "{0}")
    @MethodSource("unsupportedNewJobTargets")
    void newJobNeedsASessionCluster(Map<String, String> config, String expectedMessage) {
        assertThatThrownBy(() -> RefreshJobTarget.forNewJob(Configuration.fromMap(config)))
                .isInstanceOf(ValidationException.class)
                .hasMessage(expectedMessage);
    }

    @Test
    void sessionTargetsResolveTheirClusterId() {
        assertThat(newJobClusterId(Map.of("execution.target", "remote")))
                .isEqualTo("StandaloneClusterId");
        assertThat(
                        newJobClusterId(
                                Map.of(
                                        "execution.target", "yarn-session",
                                        "yarn.application.id", "app_1")))
                .isEqualTo("app_1");
    }

    @ParameterizedTest(name = "recorded {0} {1} -> program {2}")
    @MethodSource("clusterChangingRestarts")
    void restartStaysOnTheRecordedCluster(
            String recordedTarget,
            @Nullable String recordedClusterId,
            Map<String, String> program,
            String fragment) {
        final ContinuousRefreshHandler recorded = handler(recordedTarget, recordedClusterId);

        assertThatThrownBy(
                        () ->
                                RefreshJobTarget.forRestart(
                                        Configuration.fromMap(program), TABLE, recorded))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(fragment);
    }

    @ParameterizedTest(name = "recorded {0} {1} -> program {2}")
    @MethodSource("restartsOnTheRecordedCluster")
    void restartOnTheRecordedClusterIsAccepted(
            String recordedTarget,
            @Nullable String recordedClusterId,
            Map<String, String> program,
            String expectedClusterId) {
        final RefreshJobTarget restartTarget =
                RefreshJobTarget.forRestart(
                        Configuration.fromMap(program),
                        TABLE,
                        handler(recordedTarget, recordedClusterId));

        assertThat(restartTarget.getExecutionTarget()).isEqualTo(recordedTarget);
        assertThat(restartTarget.getClusterId()).isEqualTo(expectedClusterId);
    }

    static Stream<Arguments> unsupportedNewJobTargets() {
        return Stream.of(
                Arguments.of(Map.of(), unsupportedTargetMessage("null")),
                Arguments.of(Map.of("execution.target", ""), unsupportedTargetMessage("")),
                Arguments.of(
                        Map.of("execution.target", "local"), unsupportedTargetMessage("local")),
                Arguments.of(
                        Map.of("execution.target", "embedded"),
                        unsupportedTargetMessage("embedded")),
                Arguments.of(
                        Map.of("execution.target", "kubernetes-application"),
                        applicationModeMessage("kubernetes-application")),
                Arguments.of(
                        Map.of("execution.target", "yarn-application"),
                        applicationModeMessage("yarn-application")),
                Arguments.of(
                        Map.of("execution.target", "yarn-session"),
                        missingClusterIdMessage("yarn-session", "yarn.application.id")),
                Arguments.of(
                        kubernetesSession("  "),
                        missingClusterIdMessage("kubernetes-session", "kubernetes.cluster-id")));
    }

    static Stream<Arguments> clusterChangingRestarts() {
        return Stream.of(
                Arguments.of(
                        "remote",
                        "StandaloneClusterId",
                        kubernetesSession("c1"),
                        CLUSTER_MOVE_FRAGMENT),
                Arguments.of(
                        "kubernetes-session", "c1", kubernetesSession("c2"), CLUSTER_MOVE_FRAGMENT),
                Arguments.of(
                        "kubernetes-application",
                        "c1",
                        Map.of("execution.target", "remote"),
                        "runs in its own application cluster"));
    }

    static Stream<Arguments> restartsOnTheRecordedCluster() {
        return Stream.of(
                Arguments.of("kubernetes-session", "c1", kubernetesSession("c1"), "c1"),
                // a handler without a cluster id cannot pin one, so the program's id is taken
                Arguments.of("kubernetes-session", null, kubernetesSession("c2"), "c2"));
    }

    private static String unsupportedTargetMessage(String executionTarget) {
        return String.format(
                "Unsupported execution target detected: %s. Materialized table refresh jobs need"
                        + " a session cluster: set 'execution.target' to 'remote', 'yarn-session'"
                        + " or 'kubernetes-session' (with 'yarn.application.id' or"
                        + " 'kubernetes.cluster-id').",
                executionTarget);
    }

    private static String applicationModeMessage(String executionTarget) {
        return String.format(
                "Application-mode materialized table refresh is not supported in this"
                        + " environment: execution target '%s' requires the SQL Gateway. Use a"
                        + " session cluster target such as 'remote', 'yarn-session' or"
                        + " 'kubernetes-session'.",
                executionTarget);
    }

    private static String missingClusterIdMessage(String executionTarget, String clusterIdKey) {
        return String.format(
                "Execution target '%s' requires '%s' to be configured to submit or control a"
                        + " materialized table refresh job.",
                executionTarget, clusterIdKey);
    }

    private static String newJobClusterId(Map<String, String> config) {
        return RefreshJobTarget.forNewJob(Configuration.fromMap(config)).getClusterId();
    }

    private static Map<String, String> kubernetesSession(String clusterId) {
        return Map.of("execution.target", "kubernetes-session", "kubernetes.cluster-id", clusterId);
    }

    private static ContinuousRefreshHandler handler(
            String executionTarget, @Nullable String clusterId) {
        return new ContinuousRefreshHandler(executionTarget, clusterId, "job-1");
    }
}
