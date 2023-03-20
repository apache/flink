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
import org.apache.flink.configuration.description.InlineElement;
import org.apache.flink.configuration.description.TextElement;

import static org.apache.flink.configuration.ClusterOptions.UserSystemExitMode.THROW;
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
                    .longType()
                    .defaultValue(100L)
                    .withDescription(
                            "Initial registration timeout between cluster components in milliseconds.");

    @Documentation.Section(Documentation.Sections.EXPERT_FAULT_TOLERANCE)
    public static final ConfigOption<Long> MAX_REGISTRATION_TIMEOUT =
            ConfigOptions.key("cluster.registration.max-timeout")
                    .longType()
                    .defaultValue(30000L)
                    .withDescription(
                            "Maximum registration timeout between cluster components in milliseconds.");

    @Documentation.Section(Documentation.Sections.EXPERT_FAULT_TOLERANCE)
    public static final ConfigOption<Long> ERROR_REGISTRATION_DELAY =
            ConfigOptions.key("cluster.registration.error-delay")
                    .longType()
                    .defaultValue(10000L)
                    .withDescription(
                            "The pause made after an registration attempt caused an exception (other than timeout) in milliseconds.");

    @Documentation.Section(Documentation.Sections.EXPERT_FAULT_TOLERANCE)
    public static final ConfigOption<Long> REFUSED_REGISTRATION_DELAY =
            ConfigOptions.key("cluster.registration.refused-registration-delay")
                    .longType()
                    .defaultValue(30000L)
                    .withDescription(
                            "The pause made after the registration attempt was refused in milliseconds.");

    @Documentation.Section(Documentation.Sections.EXPERT_FAULT_TOLERANCE)
    public static final ConfigOption<Long> CLUSTER_SERVICES_SHUTDOWN_TIMEOUT =
            ConfigOptions.key("cluster.services.shutdown-timeout")
                    .longType()
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
                    .booleanType()
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
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Flag to check user code exiting system by terminating JVM (e.g., System.exit()). ")
                                    .text(
                                            "Note that this configuration option can interfere with %s: "
                                                    + "In intercepted user-code, a call to System.exit() will not cause the JVM to halt, when %s is configured.",
                                            code(HALT_ON_FATAL_ERROR.key()), code(THROW.name()))
                                    .build());

    @Documentation.Section(Documentation.Sections.EXPERT_CLUSTER)
    public static final ConfigOption<Integer> THREAD_DUMP_STACKTRACE_MAX_DEPTH =
            key("cluster.thread-dump.stacktrace-max-depth")
                    .intType()
                    .defaultValue(50)
                    .withDescription(
                            "The maximum stacktrace depth of TaskManager and JobManager's thread dump web-frontend displayed.");

    @Documentation.Section(Documentation.Sections.EXPERT_SCHEDULING)
    @Documentation.ExcludeFromDocumentation("Hidden for deprecated")
    @Deprecated
    public static final ConfigOption<Boolean> ENABLE_FINE_GRAINED_RESOURCE_MANAGEMENT =
            ConfigOptions.key("cluster.fine-grained-resource-management.enabled")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "Defines whether the cluster uses fine-grained resource management.");

    @Documentation.Section(Documentation.Sections.EXPERT_SCHEDULING)
    public static final ConfigOption<Boolean> FINE_GRAINED_SHUFFLE_MODE_ALL_BLOCKING =
            ConfigOptions.key("fine-grained.shuffle-mode.all-blocking")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Whether to convert all PIPELINE edges to BLOCKING when apply fine-grained resource management in batch jobs.");

    @Documentation.Section(Documentation.Sections.EXPERT_CLUSTER)
    public static final ConfigOption<UncaughtExceptionHandleMode> UNCAUGHT_EXCEPTION_HANDLING =
            ConfigOptions.key("cluster.uncaught-exception-handling")
                    .enumType(UncaughtExceptionHandleMode.class)
                    .defaultValue(UncaughtExceptionHandleMode.LOG)
                    .withDescription(
                            String.format(
                                    "Defines whether cluster will handle any uncaught exceptions "
                                            + "by just logging them (%s mode), or by failing job (%s mode)",
                                    UncaughtExceptionHandleMode.LOG.name(),
                                    UncaughtExceptionHandleMode.FAIL.name()));

    @Documentation.OverrideDefault("io.tmp.dirs")
    @Documentation.Section(Documentation.Sections.EXPERT_CLUSTER)
    public static final ConfigOption<String> PROCESS_WORKING_DIR_BASE =
            ConfigOptions.key("process.working-dir")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Local working directory for Flink processes. "
                                                    + "The working directory can be used to store information that can be used upon process recovery. "
                                                    + "If not configured, then it will default to a randomly picked temporary directory defined via %s.",
                                            TextElement.code(CoreOptions.TMP_DIRS.key()))
                                    .build());

    @Documentation.Section(Documentation.Sections.EXPERT_CLUSTER)
    public static final ConfigOption<String> JOB_MANAGER_PROCESS_WORKING_DIR_BASE =
            ConfigOptions.key("process.jobmanager.working-dir")
                    .stringType()
                    .noDefaultValue()
                    .withFallbackKeys(PROCESS_WORKING_DIR_BASE.key())
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Working directory for Flink JobManager processes. The working directory can be used to store information that can be used upon process recovery. If not configured, then it will default to %s.",
                                            TextElement.code(PROCESS_WORKING_DIR_BASE.key()))
                                    .build());

    @Documentation.Section(Documentation.Sections.EXPERT_CLUSTER)
    public static final ConfigOption<String> TASK_MANAGER_PROCESS_WORKING_DIR_BASE =
            ConfigOptions.key("process.taskmanager.working-dir")
                    .stringType()
                    .noDefaultValue()
                    .withFallbackKeys(PROCESS_WORKING_DIR_BASE.key())
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Working directory for Flink TaskManager processes. The working directory can be used to store information that can be used upon process recovery. If not configured, then it will default to %s.",
                                            TextElement.code(PROCESS_WORKING_DIR_BASE.key()))
                                    .build());

    public static JobManagerOptions.SchedulerType getSchedulerType(Configuration configuration) {
        if (isAdaptiveSchedulerEnabled(configuration) || isReactiveModeEnabled(configuration)) {
            return JobManagerOptions.SchedulerType.Adaptive;
        } else {
            return configuration.get(JobManagerOptions.SCHEDULER);
        }
    }

    public static boolean isReactiveModeEnabled(Configuration configuration) {
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

    /** The mode of how to handle user code attempting to exit JVM. */
    public enum UserSystemExitMode implements DescribedEnum {
        DISABLED(text("Flink is not monitoring or intercepting calls to System.exit()")),
        LOG(text("Log exit attempt with stack trace but still allowing exit to be performed")),
        THROW(text("Throw exception when exit is attempted disallowing JVM termination"));

        private final InlineElement description;

        UserSystemExitMode(InlineElement description) {
            this.description = description;
        }

        @Override
        public InlineElement getDescription() {
            return description;
        }
    }

    /** @see ClusterOptions#UNCAUGHT_EXCEPTION_HANDLING */
    public enum UncaughtExceptionHandleMode {
        LOG,
        FAIL
    }
}
