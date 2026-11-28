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

package org.apache.flink.kubernetes.utils;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.kubernetes.KubernetesTestUtils;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.utils.ActiveProcessorCountLimiter.JvmComponent;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.EnvVarBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link ActiveProcessorCountLimiter}. */
class ActiveProcessorCountLimiterTest {

    private static final String USER_FLAG = "-XX:ActiveProcessorCount=8";
    private static final String LIMIT_FLAG = "-XX:ActiveProcessorCount=2";

    @Test
    void testLimitUsesCeilOfCpuRequest() {
        assertThat(jvmArgs(limit(container(), new Configuration(), 2.5, 4.0)))
                .isEqualTo("-XX:ActiveProcessorCount=3");
        assertThat(jvmArgs(limit(container(), new Configuration(), 3.0, 2.0)))
                .isEqualTo("-XX:ActiveProcessorCount=3");
        assertThat(jvmArgs(limit(container(), new Configuration(), 1.5, 2.0)))
                .isEqualTo("-XX:ActiveProcessorCount=2");
        assertThat(jvmArgs(limit(container(), new Configuration(), 1.0, 2.0)))
                .isEqualTo("-XX:ActiveProcessorCount=1");
        assertThat(jvmArgs(limit(container(), new Configuration(), 0.5, 2.0)))
                .isEqualTo("-XX:ActiveProcessorCount=1");
    }

    @ParameterizedTest
    @ValueSource(doubles = {1.0, 0.5, Double.NaN})
    void testNoLimitWhenFactorNotAboveOne(double factor) {
        assertUnchanged(container(), new Configuration(), 2.0, factor);
    }

    @ParameterizedTest
    @ValueSource(
            doubles = {
                Double.NaN,
                Double.POSITIVE_INFINITY,
                Double.NEGATIVE_INFINITY,
                0.0,
                -2.0,
                3e9
            })
    void testNoLimitForInvalidCpuRequest(double cpu) {
        assertUnchanged(container(), new Configuration(), cpu, 4.0);
    }

    @ParameterizedTest
    @EnumSource(JvmComponent.class)
    void testNoLimitWhenDisabled(JvmComponent component) {
        final Configuration cfg = new Configuration();
        cfg.set(settingsOf(component).limitEnabled, false);
        assertUnchanged(component, container(), cfg);
    }

    @ParameterizedTest
    @EnumSource(JvmComponent.class)
    void testNoLimitWhenUserConfiguredViaConfigOptions(JvmComponent component) {
        for (ConfigOption<String> jvmOptions :
                List.of(
                        CoreOptions.FLINK_JVM_OPTIONS,
                        CoreOptions.FLINK_DEFAULT_JVM_OPTIONS,
                        settingsOf(component).jvmOptions,
                        settingsOf(component).defaultJvmOptions)) {
            final Configuration cfg = new Configuration();
            cfg.set(jvmOptions, USER_FLAG);
            assertUnchanged(component, container(), cfg);
        }
    }

    @ParameterizedTest
    @EnumSource(JvmComponent.class)
    void testNoLimitWhenUserConfiguredViaEnvVars(JvmComponent component) {
        for (String envVarName :
                List.of(
                        "FLINK_ENV_JAVA_OPTS",
                        settingsOf(component).javaOptsEnvVar,
                        "JVM_ARGS",
                        "JAVA_TOOL_OPTIONS",
                        "JDK_JAVA_OPTIONS",
                        "_JAVA_OPTIONS")) {
            assertUnchanged(
                    component, container(envVar(envVarName, USER_FLAG)), new Configuration());
        }
    }

    @ParameterizedTest
    @EnumSource(JvmComponent.class)
    void testConfigOptionsIgnoredWhenReplacedByEnvVar(JvmComponent component) {
        // config.sh does not read env.java.opts.* if the env var that replaces it is set.
        final Configuration allCfg = new Configuration();
        allCfg.set(CoreOptions.FLINK_JVM_OPTIONS, USER_FLAG);
        assertLimited(
                component, container(envVar("FLINK_ENV_JAVA_OPTS", "-Xss1m")), allCfg, LIMIT_FLAG);

        final Configuration componentCfg = new Configuration();
        componentCfg.set(settingsOf(component).defaultJvmOptions, USER_FLAG);
        assertLimited(
                component,
                container(valueFromEnvVar(settingsOf(component).javaOptsEnvVar)),
                componentCfg,
                LIMIT_FLAG);

        // An empty env var does not replace the config options.
        assertUnchanged(component, container(envVar("FLINK_ENV_JAVA_OPTS", "")), allCfg);
    }

    @ParameterizedTest
    @EnumSource(JvmComponent.class)
    void testOtherComponentSettingsDoNotAffectLimit(JvmComponent component) {
        final ComponentSettings other =
                settingsOf(
                        component == JvmComponent.JOB_MANAGER
                                ? JvmComponent.TASK_MANAGER
                                : JvmComponent.JOB_MANAGER);
        final Configuration cfg = new Configuration();
        cfg.set(other.limitEnabled, false);
        cfg.set(other.jvmOptions, USER_FLAG);
        cfg.set(other.defaultJvmOptions, USER_FLAG);

        assertLimited(
                component, container(envVar(other.javaOptsEnvVar, USER_FLAG)), cfg, LIMIT_FLAG);
    }

    @ParameterizedTest
    @ValueSource(strings = {"JVM_ARGS", "JAVA_TOOL_OPTIONS", "JDK_JAVA_OPTIONS"})
    void testNoLimitWhenJvmOptionsEnvVarIsSetFromValueFrom(String envVarName) {
        assertUnchanged(container(valueFromEnvVar(envVarName)), new Configuration(), 2.0, 4.0);
    }

    @Test
    void testMentionOfTheFlagInAnotherOptionDoesNotDisableLimit() {
        final Configuration cfg = new Configuration();
        cfg.set(CoreOptions.FLINK_JVM_OPTIONS, "-Dfoo=-XX:ActiveProcessorCount=4");
        assertLimited(JvmComponent.JOB_MANAGER, container(), cfg, LIMIT_FLAG);

        assertLimited(
                container(envVar("JVM_ARGS", "-Ddoc=-XX:ActiveProcessorCount=3")),
                "-Ddoc=-XX:ActiveProcessorCount=3 " + LIMIT_FLAG);
    }

    @Test
    void testNoLimitWhenEnvFromIsUsed() {
        final Container container =
                new ContainerBuilder()
                        .addNewEnvFrom()
                        .withNewConfigMapRef()
                        .withName("jvm-args")
                        .endConfigMapRef()
                        .endEnvFrom()
                        .build();
        assertUnchanged(container, new Configuration(), 2.0, 4.0);
    }

    @Test
    void testOnlyLastDuplicateEnvVarIsConsidered() {
        assertLimited(
                container(envVar("JVM_ARGS", USER_FLAG), envVar("JVM_ARGS", "-XX:+UseZGC")),
                "-XX:+UseZGC " + LIMIT_FLAG);
        assertUnchanged(
                container(envVar("JVM_ARGS", "-XX:+UseZGC"), envVar("JVM_ARGS", USER_FLAG)),
                new Configuration(),
                2.0,
                4.0);
        assertLimited(
                container(
                        envVar("FLINK_ENV_JAVA_OPTS_JM", USER_FLAG),
                        valueFromEnvVar("FLINK_ENV_JAVA_OPTS_JM")),
                LIMIT_FLAG);
        // Only the effective (last) JVM_ARGS entry may make it opaque.
        assertLimited(
                container(valueFromEnvVar("JVM_ARGS"), envVar("JVM_ARGS", "-XX:+UseZGC")),
                "-XX:+UseZGC " + LIMIT_FLAG);
        assertUnchanged(
                container(envVar("JVM_ARGS", USER_FLAG), valueFromEnvVar("JVM_ARGS")),
                new Configuration(),
                2.0,
                4.0);
    }

    @Test
    void testLimitAppendedToLastJvmArgsEntry() {
        final Container result =
                limit(
                        container(
                                envVar("JVM_ARGS", "-XX:+UseZGC"),
                                envVar("JVM_ARGS", "-XX:+UseCompressedOops")),
                        new Configuration(),
                        2.0,
                        4.0);
        assertThat(result.getEnv())
                .extracting(EnvVar::getValue)
                .containsExactly("-XX:+UseZGC", "-XX:+UseCompressedOops " + LIMIT_FLAG);
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "   "})
    void testLimitReplacesEmptyJvmArgs(String jvmArgs) {
        assertLimited(container(envVar("JVM_ARGS", jvmArgs)), LIMIT_FLAG);
    }

    @Test
    void testLimitIsIdempotent() {
        final Container once = limit(container(), new Configuration(), 2.0, 4.0);
        assertThat(limit(once, new Configuration(), 2.0, 4.0)).isSameAs(once);
    }

    @Test
    void testFlagNameInUnrelatedOptionDoesNotDisableLimit() {
        final Configuration cfg = new Configuration();
        cfg.set(CoreOptions.FLINK_JVM_OPTIONS, "-Dfoo=ActiveProcessorCount");
        assertLimited(JvmComponent.JOB_MANAGER, container(), cfg, LIMIT_FLAG);
    }

    private static void assertLimited(Container container, String expectedJvmArgs) {
        assertLimited(JvmComponent.JOB_MANAGER, container, new Configuration(), expectedJvmArgs);
    }

    private static void assertLimited(
            JvmComponent component, Container container, Configuration cfg, String expected) {
        final List<EnvVar> inputEnv = List.copyOf(container.getEnv());
        final Container result =
                ActiveProcessorCountLimiter.limit(container, cfg, component, 2.0, 4.0);
        final boolean hasJvmArgs =
                container.getEnv().stream().anyMatch(e -> "JVM_ARGS".equals(e.getName()));
        assertThat(result.getEnv())
                .as("A JVM_ARGS entry may only be added if none exists for %s", component)
                .hasSize(container.getEnv().size() + (hasJvmArgs ? 0 : 1));
        assertThat(jvmArgs(result)).as("JVM_ARGS of %s", component).isEqualTo(expected);
        assertThat(container.getEnv())
                .as("The input container of %s must not be modified", component)
                .isEqualTo(inputEnv);
    }

    private static void assertUnchanged(
            JvmComponent component, Container container, Configuration cfg) {
        assertThat(ActiveProcessorCountLimiter.limit(container, cfg, component, 2.0, 4.0))
                .as("Container of %s must not be modified", component)
                .isSameAs(container);
    }

    private static void assertUnchanged(
            Container container, Configuration cfg, double cpu, double factor) {
        assertThat(limit(container, cfg, cpu, factor)).isSameAs(container);
    }

    private static Container limit(
            Container container, Configuration cfg, double cpu, double factor) {
        return ActiveProcessorCountLimiter.limit(
                container, cfg, JvmComponent.JOB_MANAGER, cpu, factor);
    }

    private static String jvmArgs(Container container) {
        return KubernetesTestUtils.getEnvVarValue(container, "JVM_ARGS");
    }

    private static Container container(EnvVar... envVars) {
        return new ContainerBuilder().addToEnv(envVars).build();
    }

    private static EnvVar envVar(String name, String value) {
        return new EnvVar(name, value, null);
    }

    private static EnvVar valueFromEnvVar(String name) {
        return new EnvVarBuilder()
                .withName(name)
                .withNewValueFrom()
                .withNewSecretKeyRef("key", "secret", false)
                .endValueFrom()
                .build();
    }

    /** Spells out the expected settings per component instead of reading them from the enum. */
    private static ComponentSettings settingsOf(JvmComponent component) {
        switch (component) {
            case JOB_MANAGER:
                return new ComponentSettings(
                        KubernetesConfigOptions
                                .JOB_MANAGER_CPU_LIMIT_ACTIVE_PROCESSOR_COUNT_ENABLED,
                        CoreOptions.FLINK_JM_JVM_OPTIONS,
                        CoreOptions.FLINK_DEFAULT_JM_JVM_OPTIONS,
                        "FLINK_ENV_JAVA_OPTS_JM");
            case TASK_MANAGER:
                return new ComponentSettings(
                        KubernetesConfigOptions
                                .TASK_MANAGER_CPU_LIMIT_ACTIVE_PROCESSOR_COUNT_ENABLED,
                        CoreOptions.FLINK_TM_JVM_OPTIONS,
                        CoreOptions.FLINK_DEFAULT_TM_JVM_OPTIONS,
                        "FLINK_ENV_JAVA_OPTS_TM");
            default:
                throw new IllegalArgumentException("Unknown component " + component);
        }
    }

    private static final class ComponentSettings {
        private final ConfigOption<Boolean> limitEnabled;
        private final ConfigOption<String> jvmOptions;
        private final ConfigOption<String> defaultJvmOptions;
        private final String javaOptsEnvVar;

        private ComponentSettings(
                ConfigOption<Boolean> limitEnabled,
                ConfigOption<String> jvmOptions,
                ConfigOption<String> defaultJvmOptions,
                String javaOptsEnvVar) {
            this.limitEnabled = limitEnabled;
            this.jvmOptions = jvmOptions;
            this.defaultJvmOptions = defaultJvmOptions;
            this.javaOptsEnvVar = javaOptsEnvVar;
        }
    }
}
