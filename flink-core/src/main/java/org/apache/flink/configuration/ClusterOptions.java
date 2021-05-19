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

package org.apache.flink.configuration;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.docs.Documentation;
import org.apache.flink.configuration.description.Description;
import org.apache.flink.configuration.description.TextElement;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.configuration.ConfigOptions.key;
import static org.apache.flink.configuration.description.LinkElement.link;
import static org.apache.flink.configuration.description.TextElement.code;
import static org.apache.flink.configuration.description.TextElement.text;

/** Options which control the cluster behaviour. */
@PublicEvolving
public class ClusterOptions {

    @Documentation.Section(Documentation.Sections.EXPERT_FAULT_TOLERANCE)
    public static final ConfigOption<Long> INITIAL_REGISTRATION_TIMEOUT =
            ConfigOptions.key("cluster.registration.initial-timeout")
                    .defaultValue(100L)
                    .withDescription(
                            "Initial registration timeout between cluster components in milliseconds.");

    @Documentation.Section(Documentation.Sections.EXPERT_FAULT_TOLERANCE)
    public static final ConfigOption<Long> MAX_REGISTRATION_TIMEOUT =
            ConfigOptions.key("cluster.registration.max-timeout")
                    .defaultValue(30000L)
                    .withDescription(
                            "Maximum registration timeout between cluster components in milliseconds.");

    @Documentation.Section(Documentation.Sections.EXPERT_FAULT_TOLERANCE)
    public static final ConfigOption<Long> ERROR_REGISTRATION_DELAY =
            ConfigOptions.key("cluster.registration.error-delay")
                    .defaultValue(10000L)
                    .withDescription(
                            "The pause made after an registration attempt caused an exception (other than timeout) in milliseconds.");

    @Documentation.Section(Documentation.Sections.EXPERT_FAULT_TOLERANCE)
    public static final ConfigOption<Long> REFUSED_REGISTRATION_DELAY =
            ConfigOptions.key("cluster.registration.refused-registration-delay")
                    .defaultValue(30000L)
                    .withDescription(
                            "The pause made after the registration attempt was refused in milliseconds.");

    @Documentation.Section(Documentation.Sections.EXPERT_FAULT_TOLERANCE)
    public static final ConfigOption<Long> CLUSTER_SERVICES_SHUTDOWN_TIMEOUT =
            ConfigOptions.key("cluster.services.shutdown-timeout")
                    .defaultValue(30000L)
                    .withDescription(
                            "The shutdown timeout for cluster services like executors in milliseconds.");

    @Documentation.Section(Documentation.Sections.EXPERT_FAULT_TOLERANCE)
    public static final ConfigOption<Integer> CLUSTER_IO_EXECUTOR_POOL_SIZE =
            ConfigOptions.key("cluster.io-pool.size")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "The size of the IO executor pool used by the cluster to execute blocking IO operations (Master as well as TaskManager processes). "
                                    + "By default it will use 4 * the number of CPU cores (hardware contexts) that the cluster process has access to. "
                                    + "Increasing the pool size allows to run more IO operations concurrently.");

    @Documentation.Section(Documentation.Sections.EXPERT_SCHEDULING)
    public static final ConfigOption<Boolean> EVENLY_SPREAD_OUT_SLOTS_STRATEGY =
            ConfigOptions.key("cluster.evenly-spread-out-slots")
                    .defaultValue(false)
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Enable the slot spread out allocation strategy. This strategy tries to spread out "
                                                    + "the slots evenly across all available %s.",
                                            code("TaskExecutors"))
                                    .build());

    @Documentation.Section(Documentation.Sections.EXPERT_CLUSTER)
    public static final ConfigOption<Boolean> HALT_ON_FATAL_ERROR =
            key("cluster.processes.halt-on-fatal-error")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Whether processes should halt on fatal errors instead of performing a graceful shutdown. "
                                                    + "In some environments (e.g. Java 8 with the G1 garbage collector), a regular graceful shutdown can lead "
                                                    + "to a JVM deadlock. See %s for details.",
                                            link(
                                                    "https://issues.apache.org/jira/browse/FLINK-16510",
                                                    "FLINK-16510"))
                                    .build());

    @Documentation.Section(Documentation.Sections.EXPERT_CLUSTER)
    public static final ConfigOption<UserSystemExitMode> INTERCEPT_USER_SYSTEM_EXIT =
            key("cluster.intercept-user-system-exit")
                    .enumType(UserSystemExitMode.class)
                    .defaultValue(UserSystemExitMode.DISABLED)
                    .withDescription(UserSystemExitMode.getConfigDescription());

    @Documentation.ExcludeFromDocumentation
    public static final ConfigOption<Boolean> ENABLE_FINE_GRAINED_RESOURCE_MANAGEMENT =
            ConfigOptions.key("cluster.fine-grained-resource-management.enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Defines whether the cluster uses fine-grained resource management.");

    public static JobManagerOptions.SchedulerType getSchedulerType(Configuration configuration) {
        if (isAdaptiveSchedulerEnabled(configuration) || isReactiveModeEnabled(configuration)) {
            return JobManagerOptions.SchedulerType.Adaptive;
        } else {
            return configuration.get(JobManagerOptions.SCHEDULER);
        }
    }

    private static boolean isReactiveModeEnabled(Configuration configuration) {
        return configuration.get(JobManagerOptions.SCHEDULER_MODE)
                == SchedulerExecutionMode.REACTIVE;
    }

    public static boolean isAdaptiveSchedulerEnabled(Configuration configuration) {
        if (configuration.contains(JobManagerOptions.SCHEDULER)) {
            return configuration.get(JobManagerOptions.SCHEDULER)
                    == JobManagerOptions.SchedulerType.Adaptive;
        } else {
            return System.getProperties().containsKey("flink.tests.enable-adaptive-scheduler");
        }
    }

    public static boolean isFineGrainedResourceManagementEnabled(Configuration configuration) {
        if (configuration.contains(ENABLE_FINE_GRAINED_RESOURCE_MANAGEMENT)) {
            return configuration.get(ENABLE_FINE_GRAINED_RESOURCE_MANAGEMENT);
        } else {
            return System.getProperties().containsKey("flink.tests.enable-fine-grained");
        }
    }

    /** The mode of how to handle user code attempting to exit JVM. */
    public enum UserSystemExitMode {
        DISABLED("Flink is not monitoring or intercepting calls to System.exit()"),
        LOG("Log exit attempt with stack trace but still allowing exit to be performed"),
        THROW("Throw exception when exit is attempted disallowing JVM termination");

        private final String description;

        UserSystemExitMode(String description) {
            this.description = description;
        }

        public static Description getConfigDescription() {
            Description.DescriptionBuilder builder = Description.builder();
            List<TextElement> modeDescriptions =
                    new ArrayList<>(UserSystemExitMode.values().length);
            builder.text(
                    "Flag to check user code exiting system by terminating JVM (e.g., System.exit())");
            for (UserSystemExitMode mode : UserSystemExitMode.values()) {
                modeDescriptions.add(
                        text(String.format("%s - %s", mode.name(), mode.getDescription())));
            }
            builder.list(modeDescriptions.toArray(new TextElement[modeDescriptions.size()]));
            builder.linebreak();
            builder.text(
                    "Note that this configuration option can interfere with %s: "
                            + "In intercepted user-code, a call to System.exit() will not cause the JVM to halt, when %s is configured.",
                    code(HALT_ON_FATAL_ERROR.key()), code(THROW.name()));
            return builder.build();
        }

        public String getDescription() {
            return description;
        }
    }
}
