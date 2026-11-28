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

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.util.StringUtils;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.EnvVarBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * Limits the processors seen by the JobManager/TaskManager JVM to the CPU request if a CPU limit
 * factor greater than 1 is configured. The container-aware JVM derives {@code
 * Runtime.availableProcessors()} from the cgroup CPU quota, i.e. from the inflated CPU limit, while
 * the memory budget does not grow with the limit factor.
 */
@Internal
public final class ActiveProcessorCountLimiter {

    private static final Logger LOG = LoggerFactory.getLogger(ActiveProcessorCountLimiter.class);

    private static final String ACTIVE_PROCESSOR_COUNT_FLAG = "-XX:ActiveProcessorCount";

    /**
     * Matches the flag as a standalone JVM argument, so that a value merely mentioning it (e.g.
     * {@code -Dfoo=-XX:ActiveProcessorCount=4}) does not count as configured.
     */
    private static final Pattern ACTIVE_PROCESSOR_COUNT_PATTERN =
            Pattern.compile("(?:^|\\s)" + ACTIVE_PROCESSOR_COUNT_FLAG + "(?:=|\\s|$)");

    /**
     * Env vars whose JVM options precede the injected flag, so that the injected flag would
     * override a user-configured {@code -XX:ActiveProcessorCount} in them. {@code _JAVA_OPTIONS} is
     * missing on purpose: it is applied after the command line, so a user value there wins anyway.
     */
    private static final List<String> OVERRIDDEN_ENV_VARS =
            List.of(
                    Constants.ENV_JVM_ARGS,
                    Constants.ENV_JAVA_TOOL_OPTIONS,
                    Constants.ENV_JDK_JAVA_OPTIONS);

    /**
     * Env vars checked for a user-configured flag. {@code FLINK_ENV_JAVA_OPTS*} follow the injected
     * flag on the java command line, so they are not in {@link #OVERRIDDEN_ENV_VARS}, but they are
     * inspected to avoid injecting a flag that would be overridden anyway.
     */
    private static final List<String> DETECTED_ENV_VARS =
            List.of(
                    Constants.ENV_FLINK_ENV_JAVA_OPTS,
                    Constants.ENV_JAVA_TOOL_OPTIONS,
                    Constants.ENV_JVM_ARGS,
                    Constants.ENV_JDK_JAVA_OPTIONS,
                    Constants.ENV_UNDERSCORE_JAVA_OPTIONS);

    private ActiveProcessorCountLimiter() {}

    /**
     * Appends {@code -XX:ActiveProcessorCount=<ceil(cpuCores)>} to the {@code JVM_ARGS} env var of
     * the main container. The flag goes into {@code JVM_ARGS} because that variable precedes all
     * options derived from the configuration on the java command line, so an explicit user setting
     * still wins. The injection is skipped if the limit is disabled, if the CPU request cannot be
     * represented, if the user already configured the flag, or if the JVM options of the container
     * cannot be inspected.
     *
     * @return the given main container if unchanged, or a copy with the flag appended
     */
    public static Container limit(
            Container mainContainer,
            Configuration configuration,
            JvmComponent component,
            double cpuCores,
            double cpuLimitFactor) {
        if (!(cpuLimitFactor > 1.0)) {
            return mainContainer;
        }
        if (!configuration.get(component.limitEnabledOption)) {
            LOG.info(
                    "The -XX:ActiveProcessorCount limit for the {} is disabled via {}.",
                    component.displayName,
                    component.limitEnabledOption.key());
            return mainContainer;
        }
        if (!Double.isFinite(cpuCores) || cpuCores <= 0 || cpuCores > Integer.MAX_VALUE) {
            LOG.warn(
                    "Skipping the -XX:ActiveProcessorCount limit for the {} because the CPU "
                            + "request cannot be represented as a processor count: {}",
                    component.displayName,
                    cpuCores);
            return mainContainer;
        }
        final Map<String, EnvVar> effectiveEnv = getEffectiveEnv(mainContainer);
        final Optional<String> configuredSource =
                findActiveProcessorCountSource(configuration, effectiveEnv, component);
        if (configuredSource.isPresent()) {
            LOG.info(
                    "Skipping the -XX:ActiveProcessorCount limit for the {} because "
                            + "-XX:ActiveProcessorCount is already configured via {}.",
                    component.displayName,
                    configuredSource.get());
            return mainContainer;
        }
        if (hasOpaqueJvmOptions(mainContainer, effectiveEnv)) {
            LOG.info(
                    "Skipping the -XX:ActiveProcessorCount limit for the {} because its "
                            + "environment uses envFrom or sets one of {} via valueFrom, which may "
                            + "contain JVM options that cannot be inspected at pod creation time. "
                            + "Set -XX:ActiveProcessorCount via {} to limit the processors, or "
                            + "disable this check via {}.",
                    component.displayName,
                    String.join(", ", OVERRIDDEN_ENV_VARS),
                    component.jvmOptions.key(),
                    component.limitEnabledOption.key());
            return mainContainer;
        }
        // Round up: a request below one CPU must not be reported as zero processors.
        final String activeProcessorCountJvmArg =
                ACTIVE_PROCESSOR_COUNT_FLAG + "=" + (int) Math.ceil(cpuCores);
        LOG.info(
                "CPU limit factor {} is greater than 1, limiting the {} JVM to the CPU request "
                        + "with {}. This can be disabled via {}.",
                cpuLimitFactor,
                component.displayName,
                activeProcessorCountJvmArg,
                component.limitEnabledOption.key());
        return appendJvmArgToEnv(mainContainer, activeProcessorCountJvmArg);
    }

    /** Kubernetes applies the last duplicate env var, so only that one is effective. */
    private static Map<String, EnvVar> getEffectiveEnv(Container mainContainer) {
        final Map<String, EnvVar> effectiveEnv = new HashMap<>();
        for (EnvVar envVar : getEnvVars(mainContainer)) {
            effectiveEnv.put(envVar.getName(), envVar);
        }
        return effectiveEnv;
    }

    /** Returns a description of the source that already sets the flag, if any. */
    private static Optional<String> findActiveProcessorCountSource(
            Configuration configuration, Map<String, EnvVar> effectiveEnv, JvmComponent component) {
        // config.sh ignores the env.java.* options if the env var that replaces them is set.
        final List<ConfigOption<String>> effectiveOptions = new ArrayList<>();
        if (!isSetInEnv(effectiveEnv, Constants.ENV_FLINK_ENV_JAVA_OPTS)) {
            effectiveOptions.add(CoreOptions.FLINK_JVM_OPTIONS);
            effectiveOptions.add(CoreOptions.FLINK_DEFAULT_JVM_OPTIONS);
        }
        if (!isSetInEnv(effectiveEnv, component.javaOptsEnvVar)) {
            effectiveOptions.add(component.jvmOptions);
            effectiveOptions.add(component.defaultJvmOptions);
        }
        final Stream<String> configSources =
                effectiveOptions.stream()
                        .filter(option -> containsActiveProcessorCount(configuration.get(option)))
                        .map(ConfigOption::key);
        final Stream<String> envSources =
                Stream.concat(Stream.of(component.javaOptsEnvVar), DETECTED_ENV_VARS.stream())
                        .filter(
                                name ->
                                        effectiveEnv.containsKey(name)
                                                && containsActiveProcessorCount(
                                                        effectiveEnv.get(name).getValue()))
                        .map(name -> "the " + name + " environment variable");
        return Stream.concat(configSources, envSources).findFirst();
    }

    /**
     * Mirrors config.sh, which treats an unset and an empty env var alike. A {@code valueFrom} env
     * var counts as set, so that a config option it may replace is not trusted; injecting is safe
     * then, because any option from config.sh follows the injected flag on the command line.
     */
    private static boolean isSetInEnv(Map<String, EnvVar> effectiveEnv, String name) {
        final EnvVar envVar = effectiveEnv.get(name);
        return envVar != null
                && (envVar.getValueFrom() != null
                        || (envVar.getValue() != null && !envVar.getValue().isEmpty()));
    }

    /**
     * Returns whether a JVM option source that the injected flag would override cannot be
     * inspected, i.e. any {@code envFrom} entry or such an env var declared with {@code valueFrom}.
     */
    private static boolean hasOpaqueJvmOptions(
            Container mainContainer, Map<String, EnvVar> effectiveEnv) {
        return (mainContainer.getEnvFrom() != null && !mainContainer.getEnvFrom().isEmpty())
                || OVERRIDDEN_ENV_VARS.stream()
                        .map(effectiveEnv::get)
                        .anyMatch(envVar -> envVar != null && envVar.getValueFrom() != null);
    }

    private static boolean containsActiveProcessorCount(String jvmOpts) {
        return jvmOpts != null && ACTIVE_PROCESSOR_COUNT_PATTERN.matcher(jvmOpts).find();
    }

    private static List<EnvVar> getEnvVars(Container mainContainer) {
        return mainContainer.getEnv() == null ? Collections.emptyList() : mainContainer.getEnv();
    }

    private static Container appendJvmArgToEnv(Container mainContainer, String jvmArg) {
        final List<EnvVar> envVars = new ArrayList<>(getEnvVars(mainContainer));
        // Kubernetes applies the last duplicate env var, so the flag must be appended to the last.
        int jvmArgsIndex = -1;
        for (int i = 0; i < envVars.size(); ++i) {
            if (Constants.ENV_JVM_ARGS.equals(envVars.get(i).getName())) {
                jvmArgsIndex = i;
            }
        }
        if (jvmArgsIndex >= 0) {
            final EnvVar jvmArgs = envVars.get(jvmArgsIndex);
            envVars.set(
                    jvmArgsIndex,
                    new EnvVarBuilder(jvmArgs)
                            .withValue(
                                    StringUtils.isNullOrWhitespaceOnly(jvmArgs.getValue())
                                            ? jvmArg
                                            : jvmArgs.getValue() + " " + jvmArg)
                            .build());
        } else {
            envVars.add(
                    new EnvVarBuilder().withName(Constants.ENV_JVM_ARGS).withValue(jvmArg).build());
        }
        return new ContainerBuilder(mainContainer).withEnv(envVars).build();
    }

    /** JVM component whose processor count is limited to the CPU request. */
    @Internal
    public enum JvmComponent {
        JOB_MANAGER(
                "JobManager",
                KubernetesConfigOptions.JOB_MANAGER_CPU_LIMIT_ACTIVE_PROCESSOR_COUNT_ENABLED,
                CoreOptions.FLINK_JM_JVM_OPTIONS,
                CoreOptions.FLINK_DEFAULT_JM_JVM_OPTIONS,
                Constants.ENV_FLINK_ENV_JAVA_OPTS_JM),
        TASK_MANAGER(
                "TaskManager",
                KubernetesConfigOptions.TASK_MANAGER_CPU_LIMIT_ACTIVE_PROCESSOR_COUNT_ENABLED,
                CoreOptions.FLINK_TM_JVM_OPTIONS,
                CoreOptions.FLINK_DEFAULT_TM_JVM_OPTIONS,
                Constants.ENV_FLINK_ENV_JAVA_OPTS_TM);

        private final String displayName;
        private final ConfigOption<Boolean> limitEnabledOption;
        private final ConfigOption<String> jvmOptions;
        private final ConfigOption<String> defaultJvmOptions;
        private final String javaOptsEnvVar;

        JvmComponent(
                String displayName,
                ConfigOption<Boolean> limitEnabledOption,
                ConfigOption<String> jvmOptions,
                ConfigOption<String> defaultJvmOptions,
                String javaOptsEnvVar) {
            this.displayName = displayName;
            this.limitEnabledOption = limitEnabledOption;
            this.jvmOptions = jvmOptions;
            this.defaultJvmOptions = defaultJvmOptions;
            this.javaOptsEnvVar = javaOptsEnvVar;
        }
    }
}
