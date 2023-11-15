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

import java.time.Duration;

import static org.apache.flink.configuration.ConfigOptions.key;
import static org.apache.flink.configuration.JobManagerOptions.HybridPartitionDataConsumeConstraint.ALL_PRODUCERS_FINISHED;
import static org.apache.flink.configuration.JobManagerOptions.HybridPartitionDataConsumeConstraint.ONLY_FINISHED_PRODUCERS;
import static org.apache.flink.configuration.JobManagerOptions.HybridPartitionDataConsumeConstraint.UNFINISHED_PRODUCERS;
import static org.apache.flink.configuration.description.LinkElement.link;
import static org.apache.flink.configuration.description.TextElement.code;
import static org.apache.flink.configuration.description.TextElement.text;

/** Configuration options for the JobManager. */
@PublicEvolving
public class JobManagerOptions {

    public static final MemorySize MIN_JVM_HEAP_SIZE = MemorySize.ofMebiBytes(128);

    /**
     * The config parameter defining the network address to connect to for communication with the
     * job manager.
     *
     * <p>This value is only interpreted in setups where a single JobManager with static name or
     * address exists (simple standalone setups, or container setups with dynamic service name
     * resolution). It is not used in many high-availability setups, when a leader-election service
     * (like ZooKeeper) is used to elect and discover the JobManager leader from potentially
     * multiple standby JobManagers.
     */
    @Documentation.Section({
        Documentation.Sections.COMMON_HOST_PORT,
        Documentation.Sections.ALL_JOB_MANAGER
    })
    public static final ConfigOption<String> ADDRESS =
            key("jobmanager.rpc.address")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The config parameter defining the network address to connect to"
                                    + " for communication with the job manager."
                                    + " This value is only interpreted in setups where a single JobManager with static"
                                    + " name or address exists (simple standalone setups, or container setups with dynamic"
                                    + " service name resolution). It is not used in many high-availability setups, when a"
                                    + " leader-election service (like ZooKeeper) is used to elect and discover the JobManager"
                                    + " leader from potentially multiple standby JobManagers.");

    /** The local address of the network interface that the job manager binds to. */
    @Documentation.Section({
        Documentation.Sections.COMMON_HOST_PORT,
        Documentation.Sections.ALL_JOB_MANAGER
    })
    public static final ConfigOption<String> BIND_HOST =
            key("jobmanager.bind-host")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The local address of the network interface that the job manager binds to. If not"
                                    + " configured, '0.0.0.0' will be used.");

    /**
     * The config parameter defining the network port to connect to for communication with the job
     * manager.
     *
     * <p>Like {@link JobManagerOptions#ADDRESS}, this value is only interpreted in setups where a
     * single JobManager with static name/address and port exists (simple standalone setups, or
     * container setups with dynamic service name resolution). This config option is not used in
     * many high-availability setups, when a leader-election service (like ZooKeeper) is used to
     * elect and discover the JobManager leader from potentially multiple standby JobManagers.
     */
    @Documentation.Section({
        Documentation.Sections.COMMON_HOST_PORT,
        Documentation.Sections.ALL_JOB_MANAGER
    })
    public static final ConfigOption<Integer> PORT =
            key("jobmanager.rpc.port")
                    .intType()
                    .defaultValue(6123)
                    .withDescription(
                            "The config parameter defining the network port to connect to"
                                    + " for communication with the job manager."
                                    + " Like "
                                    + ADDRESS.key()
                                    + ", this value is only interpreted in setups where"
                                    + " a single JobManager with static name/address and port exists (simple standalone setups,"
                                    + " or container setups with dynamic service name resolution)."
                                    + " This config option is not used in many high-availability setups, when a"
                                    + " leader-election service (like ZooKeeper) is used to elect and discover the JobManager"
                                    + " leader from potentially multiple standby JobManagers.");

    /** The local port that the job manager binds to. */
    @Documentation.Section({
        Documentation.Sections.COMMON_HOST_PORT,
        Documentation.Sections.ALL_JOB_MANAGER
    })
    public static final ConfigOption<Integer> RPC_BIND_PORT =
            key("jobmanager.rpc.bind-port")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "The local RPC port that the JobManager binds to. If not configured, the external port"
                                    + " (configured by '"
                                    + PORT.key()
                                    + "') will be used.");

    /**
     * JVM heap size for the JobManager with memory size.
     *
     * @deprecated use {@link #TOTAL_FLINK_MEMORY} for standalone setups and {@link
     *     #TOTAL_PROCESS_MEMORY} for containerized setups.
     */
    @Deprecated
    @Documentation.Section(Documentation.Sections.ALL_JOB_MANAGER)
    public static final ConfigOption<MemorySize> JOB_MANAGER_HEAP_MEMORY =
            key("jobmanager.heap.size")
                    .memoryType()
                    .noDefaultValue()
                    .withDescription("JVM heap size for the JobManager.");

    /**
     * JVM heap size (in megabytes) for the JobManager.
     *
     * @deprecated use {@link #TOTAL_FLINK_MEMORY} for standalone setups and {@link
     *     #TOTAL_PROCESS_MEMORY} for containerized setups.
     */
    @Deprecated
    public static final ConfigOption<Integer> JOB_MANAGER_HEAP_MEMORY_MB =
            key("jobmanager.heap.mb")
                    .intType()
                    .noDefaultValue()
                    .withDescription("JVM heap size (in megabytes) for the JobManager.");

    /** Total Process Memory size for the JobManager. */
    @Documentation.Section(Documentation.Sections.COMMON_MEMORY)
    public static final ConfigOption<MemorySize> TOTAL_PROCESS_MEMORY =
            key("jobmanager.memory.process.size")
                    .memoryType()
                    .noDefaultValue()
                    .withDescription(
                            "Total Process Memory size for the JobManager. This includes all the memory that a "
                                    + "JobManager JVM process consumes, consisting of Total Flink Memory, JVM Metaspace, and JVM Overhead. "
                                    + "In containerized setups, this should be set to the container memory. See also "
                                    + "'jobmanager.memory.flink.size' for Total Flink Memory size configuration.");

    /** Total Flink Memory size for the JobManager. */
    @Documentation.Section(Documentation.Sections.COMMON_MEMORY)
    public static final ConfigOption<MemorySize> TOTAL_FLINK_MEMORY =
            key("jobmanager.memory.flink.size")
                    .memoryType()
                    .noDefaultValue()
                    .withDescription(
                            String.format(
                                    "Total Flink Memory size for the JobManager. This includes all the "
                                            + "memory that a JobManager consumes, except for JVM Metaspace and JVM Overhead. It consists of "
                                            + "JVM Heap Memory and Off-heap Memory. See also '%s' for total process memory size configuration.",
                                    TOTAL_PROCESS_MEMORY.key()));

    /** JVM Heap Memory size for the JobManager. */
    @Documentation.Section(Documentation.Sections.COMMON_MEMORY)
    public static final ConfigOption<MemorySize> JVM_HEAP_MEMORY =
            key("jobmanager.memory.heap.size")
                    .memoryType()
                    .noDefaultValue()
                    .withDescription(
                            "JVM Heap Memory size for JobManager. The minimum recommended JVM Heap size is "
                                    + MIN_JVM_HEAP_SIZE.toHumanReadableString()
                                    + '.');

    /** Off-heap Memory size for the JobManager. */
    @Documentation.Section(Documentation.Sections.COMMON_MEMORY)
    public static final ConfigOption<MemorySize> OFF_HEAP_MEMORY =
            key("jobmanager.memory.off-heap.size")
                    .memoryType()
                    .defaultValue(MemorySize.ofMebiBytes(128))
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Off-heap Memory size for JobManager. This option covers all off-heap memory usage including "
                                                    + "direct and native memory allocation. The JVM direct memory limit of the JobManager process "
                                                    + "(-XX:MaxDirectMemorySize) will be set to this value if the limit is enabled by "
                                                    + "'jobmanager.memory.enable-jvm-direct-memory-limit'. ")
                                    .build());

    /** Off-heap Memory size for the JobManager. */
    @Documentation.Section(Documentation.Sections.COMMON_MEMORY)
    public static final ConfigOption<Boolean> JVM_DIRECT_MEMORY_LIMIT_ENABLED =
            key("jobmanager.memory.enable-jvm-direct-memory-limit")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Whether to enable the JVM direct memory limit of the JobManager process "
                                                    + "(-XX:MaxDirectMemorySize). The limit will be set to the value of '%s' option. ",
                                            text(OFF_HEAP_MEMORY.key()))
                                    .build());

    /** JVM Metaspace Size for the JobManager. */
    @Documentation.Section(Documentation.Sections.COMMON_MEMORY)
    public static final ConfigOption<MemorySize> JVM_METASPACE =
            key("jobmanager.memory.jvm-metaspace.size")
                    .memoryType()
                    .defaultValue(MemorySize.ofMebiBytes(256))
                    .withDescription("JVM Metaspace Size for the JobManager.");

    private static final String JVM_OVERHEAD_DESCRIPTION =
            "This is off-heap memory reserved for JVM "
                    + "overhead, such as thread stack space, compile cache, etc. This includes native memory but not direct "
                    + "memory, and will not be counted when Flink calculates JVM max direct memory size parameter. The size "
                    + "of JVM Overhead is derived to make up the configured fraction of the Total Process Memory. If the "
                    + "derived size is less or greater than the configured min or max size, the min or max size will be used. The "
                    + "exact size of JVM Overhead can be explicitly specified by setting the min and max size to the same value.";

    /** Min JVM Overhead size for the JobManager. */
    @Documentation.Section(Documentation.Sections.COMMON_MEMORY)
    public static final ConfigOption<MemorySize> JVM_OVERHEAD_MIN =
            key("jobmanager.memory.jvm-overhead.min")
                    .memoryType()
                    .defaultValue(MemorySize.ofMebiBytes(192))
                    .withDescription(
                            "Min JVM Overhead size for the JobManager. "
                                    + JVM_OVERHEAD_DESCRIPTION);

    /** Max JVM Overhead size for the TaskExecutors. */
    @Documentation.Section(Documentation.Sections.COMMON_MEMORY)
    public static final ConfigOption<MemorySize> JVM_OVERHEAD_MAX =
            key("jobmanager.memory.jvm-overhead.max")
                    .memoryType()
                    .defaultValue(MemorySize.parse("1g"))
                    .withDescription(
                            "Max JVM Overhead size for the JobManager. "
                                    + JVM_OVERHEAD_DESCRIPTION);

    /** Fraction of Total Process Memory to be reserved for JVM Overhead. */
    @Documentation.Section(Documentation.Sections.COMMON_MEMORY)
    public static final ConfigOption<Float> JVM_OVERHEAD_FRACTION =
            key("jobmanager.memory.jvm-overhead.fraction")
                    .floatType()
                    .defaultValue(0.1f)
                    .withDescription(
                            "Fraction of Total Process Memory to be reserved for JVM Overhead. "
                                    + JVM_OVERHEAD_DESCRIPTION);

    /** The maximum number of historical execution attempts kept in history. */
    @Documentation.Section(Documentation.Sections.ALL_JOB_MANAGER)
    public static final ConfigOption<Integer> MAX_ATTEMPTS_HISTORY_SIZE =
            key("jobmanager.execution.attempts-history-size")
                    .intType()
                    .defaultValue(16)
                    .withDeprecatedKeys("job-manager.max-attempts-history-size")
                    .withDescription(
                            "The maximum number of historical execution attempts kept in history.");

    /**
     * Flag indicating whether JobManager should load available Failure Enricher plugins at startup.
     * An optional list of Failure Enricher names. If empty, NO enrichers will be started. If
     * configured, only enrichers whose name (as returned by class.getName()) matches any of the
     * names in the list will be started.
     *
     * <p>Example:
     *
     * <pre>{@code
     * jobmanager.failure-enrichers = org.apache.flink.test.plugin.jar.failure.TypeFailureEnricher, org.apache.flink.runtime.failure.FailureEnricherUtilsTest$TestEnricher
     *
     * }</pre>
     */
    @Documentation.Section(Documentation.Sections.ALL_JOB_MANAGER)
    public static final ConfigOption<String> FAILURE_ENRICHERS_LIST =
            key("jobmanager.failure-enrichers")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "An optional list of failure enricher names."
                                    + " If empty, NO failure enrichers will be started."
                                    + " If configured, only enrichers whose name matches"
                                    + " any of the names in the list will be started.");

    /**
     * This option specifies the failover strategy, i.e. how the job computation recovers from task
     * failures.
     */
    @Documentation.Section({
        Documentation.Sections.ALL_JOB_MANAGER,
        Documentation.Sections.EXPERT_FAULT_TOLERANCE
    })
    public static final ConfigOption<String> EXECUTION_FAILOVER_STRATEGY =
            key("jobmanager.execution.failover-strategy")
                    .stringType()
                    .defaultValue("region")
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "This option specifies how the job computation recovers from task failures. "
                                                    + "Accepted values are:")
                                    .list(
                                            text("'full': Restarts all tasks to recover the job."),
                                            text(
                                                    "'region': Restarts all tasks that could be affected by the task failure. "
                                                            + "More details can be found %s.",
                                                    link(
                                                            "{{.Site.BaseURL}}{{.Site.LanguagePrefix}}/docs/ops/state/task_failure_recovery/#restart-pipelined-region-failover-strategy",
                                                            "here")))
                                    .build());

    /** The location where the JobManager stores the archives of completed jobs. */
    @Documentation.Section(Documentation.Sections.ALL_JOB_MANAGER)
    public static final ConfigOption<String> ARCHIVE_DIR =
            key("jobmanager.archive.fs.dir")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Directory for JobManager to store the archives of completed jobs.");

    /** The job store cache size in bytes which is used to keep completed jobs in memory. */
    @Documentation.Section(Documentation.Sections.ALL_JOB_MANAGER)
    public static final ConfigOption<Long> JOB_STORE_CACHE_SIZE =
            key("jobstore.cache-size")
                    .longType()
                    .defaultValue(50L * 1024L * 1024L)
                    .withDescription(
                            "The job store cache size in bytes which is used to keep completed jobs in memory.");

    /** The time in seconds after which a completed job expires and is purged from the job store. */
    @Documentation.Section(Documentation.Sections.ALL_JOB_MANAGER)
    public static final ConfigOption<Long> JOB_STORE_EXPIRATION_TIME =
            key("jobstore.expiration-time")
                    .longType()
                    .defaultValue(60L * 60L)
                    .withDescription(
                            "The time in seconds after which a completed job expires and is purged from the job store.");

    /** The max number of completed jobs that can be kept in the job store. */
    @Documentation.Section(Documentation.Sections.ALL_JOB_MANAGER)
    @Documentation.OverrideDefault("infinite")
    public static final ConfigOption<Integer> JOB_STORE_MAX_CAPACITY =
            key("jobstore.max-capacity")
                    .intType()
                    .defaultValue(Integer.MAX_VALUE)
                    .withDescription(
                            "The max number of completed jobs that can be kept in the job store. "
                                    + "NOTICE: if memory store keeps too many jobs in session cluster, it may cause FullGC or OOM in jm.");

    /** Config parameter determining the job store implementation in session cluster. */
    @Documentation.Section(Documentation.Sections.ALL_JOB_MANAGER)
    public static final ConfigOption<JobStoreType> JOB_STORE_TYPE =
            key("jobstore.type")
                    .enumType(JobStoreType.class)
                    .defaultValue(JobStoreType.File)
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Determines which job store implementation is used in session cluster. Accepted values are:")
                                    .list(
                                            text(
                                                    "'File': the file job store keeps the archived execution graphs in files"),
                                            text(
                                                    "'Memory': the memory job store keeps the archived execution graphs in memory. You"
                                                            + " may need to limit the %s to mitigate FullGC or OOM when there are too many graphs",
                                                    code(JOB_STORE_MAX_CAPACITY.key())))
                                    .build());

    /** Type of job store implementation. */
    public enum JobStoreType {
        File,
        Memory
    }

    /**
     * Flag indicating whether JobManager would retrieve canonical host name of TaskManager during
     * registration.
     */
    @Documentation.Section(Documentation.Sections.ALL_JOB_MANAGER)
    public static final ConfigOption<Boolean> RETRIEVE_TASK_MANAGER_HOSTNAME =
            key("jobmanager.retrieve-taskmanager-hostname")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "Flag indicating whether JobManager would retrieve canonical "
                                    + "host name of TaskManager during registration. "
                                    + "If the option is set to \"false\", TaskManager registration with "
                                    + "JobManager could be faster, since no reverse DNS lookup is performed. "
                                    + "However, local input split assignment (such as for HDFS files) may be impacted.");

    @Documentation.Section({
        Documentation.Sections.EXPERT_JOB_MANAGER,
        Documentation.Sections.ALL_JOB_MANAGER
    })
    public static final ConfigOption<Integer> JOB_MANAGER_FUTURE_POOL_SIZE =
            key("jobmanager.future-pool.size")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "The size of the future thread pool to execute future callbacks for all spawned JobMasters. "
                                    + "If no value is specified, then Flink defaults to the number of available CPU cores.");

    @Documentation.Section({
        Documentation.Sections.EXPERT_JOB_MANAGER,
        Documentation.Sections.ALL_JOB_MANAGER
    })
    public static final ConfigOption<Integer> JOB_MANAGER_IO_POOL_SIZE =
            key("jobmanager.io-pool.size")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "The size of the IO thread pool to run blocking operations for all spawned JobMasters. "
                                    + "This includes recovery and completion of checkpoints. "
                                    + "Increase this value if you experience slow checkpoint operations when running many jobs. "
                                    + "If no value is specified, then Flink defaults to the number of available CPU cores.");

    /** The timeout in milliseconds for requesting a slot from Slot Pool. */
    @Documentation.Section(Documentation.Sections.EXPERT_SCHEDULING)
    public static final ConfigOption<Long> SLOT_REQUEST_TIMEOUT =
            key("slot.request.timeout")
                    .longType()
                    .defaultValue(5L * 60L * 1000L)
                    .withDescription(
                            "The timeout in milliseconds for requesting a slot from Slot Pool.");

    /** The timeout in milliseconds for a idle slot in Slot Pool. */
    @Documentation.Section(Documentation.Sections.EXPERT_SCHEDULING)
    public static final ConfigOption<Long> SLOT_IDLE_TIMEOUT =
            key("slot.idle.timeout")
                    .longType()
                    // default matches heartbeat.timeout so that sticky allocation is not lost on
                    // timeouts for local recovery
                    .defaultValue(HeartbeatManagerOptions.HEARTBEAT_TIMEOUT.defaultValue())
                    .withDescription("The timeout in milliseconds for a idle slot in Slot Pool.");

    /** Config parameter determining the scheduler implementation. */
    @Documentation.Section({
        Documentation.Sections.EXPERT_SCHEDULING,
        Documentation.Sections.ALL_JOB_MANAGER
    })
    public static final ConfigOption<SchedulerType> SCHEDULER =
            key("jobmanager.scheduler")
                    .enumType(SchedulerType.class)
                    .defaultValue(SchedulerType.Default)
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Determines which scheduler implementation is used to schedule tasks. Accepted values are:")
                                    .list(
                                            text("'Default': Default scheduler"),
                                            text(
                                                    "'Adaptive': Adaptive scheduler. More details can be found %s.",
                                                    link(
                                                            "{{.Site.BaseURL}}{{.Site.LanguagePrefix}}/docs/deployment/elastic_scaling#adaptive-scheduler",
                                                            "here")),
                                            text(
                                                    "'AdaptiveBatch': Adaptive batch scheduler. More details can be found %s.",
                                                    link(
                                                            "{{.Site.BaseURL}}{{.Site.LanguagePrefix}}/docs/deployment/elastic_scaling#adaptive-batch-scheduler",
                                                            "here")))
                                    .build());

    /** Type of scheduler implementation. */
    public enum SchedulerType {
        /** @deprecated Use {@link SchedulerType#Default} instead. */
        @Deprecated
        Ng,
        Default,
        Adaptive,
        AdaptiveBatch
    }

    @Documentation.Section(Documentation.Sections.EXPERT_SCHEDULING)
    public static final ConfigOption<SchedulerExecutionMode> SCHEDULER_MODE =
            key("scheduler-mode")
                    .enumType(SchedulerExecutionMode.class)
                    .defaultValue(null)
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Determines the mode of the scheduler. Note that %s=%s is only supported by standalone application deployments, not by active resource managers (YARN, Kubernetes) or session clusters.",
                                            code("scheduler-mode"),
                                            code(SchedulerExecutionMode.REACTIVE.name()))
                                    .build());

    @Documentation.Section({
        Documentation.Sections.EXPERT_SCHEDULING,
        Documentation.Sections.ALL_JOB_MANAGER
    })
    public static final ConfigOption<Integer> MIN_PARALLELISM_INCREASE =
            key("jobmanager.adaptive-scheduler.min-parallelism-increase")
                    .intType()
                    .defaultValue(1)
                    .withDescription(
                            "Configure the minimum increase in parallelism for a job to scale up.");

    @Documentation.Section({
        Documentation.Sections.EXPERT_SCHEDULING,
        Documentation.Sections.ALL_JOB_MANAGER
    })
    public static final ConfigOption<Duration> SCHEDULER_SCALING_INTERVAL_MIN =
            key("jobmanager.adaptive-scheduler.scaling-interval.min")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(30))
                    // rescaling and let the user increase the value for high workloads
                    .withDescription("Determines the minimum time between scaling operations.");

    @Documentation.Section({
        Documentation.Sections.EXPERT_SCHEDULING,
        Documentation.Sections.ALL_JOB_MANAGER
    })
    public static final ConfigOption<Duration> SCHEDULER_SCALING_INTERVAL_MAX =
            key("jobmanager.adaptive-scheduler.scaling-interval.max")
                    .durationType()
                    .noDefaultValue()
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Determines the maximum interval time after which a scaling operation is forced even if the %s aren't met. The scaling operation will be ignored when the resource hasn't changed. This option is disabled by default.",
                                            code(MIN_PARALLELISM_INCREASE.key()))
                                    .build());

    @Documentation.Section({
        Documentation.Sections.EXPERT_SCHEDULING,
        Documentation.Sections.ALL_JOB_MANAGER
    })
    public static final ConfigOption<Duration> RESOURCE_WAIT_TIMEOUT =
            key("jobmanager.adaptive-scheduler.resource-wait-timeout")
                    .durationType()
                    .defaultValue(Duration.ofMinutes(5))
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "The maximum time the JobManager will wait to acquire all required resources after a job submission or restart. "
                                                    + "Once elapsed it will try to run the job with a lower parallelism, or fail if the minimum amount of resources could not be acquired.")
                                    .linebreak()
                                    .text(
                                            "Increasing this value will make the cluster more resilient against temporary resources shortages (e.g., there is more time for a failed TaskManager to be restarted).")
                                    .linebreak()
                                    .text(
                                            "Setting a negative duration will disable the resource timeout: The JobManager will wait indefinitely for resources to appear.")
                                    .linebreak()
                                    .text(
                                            "If %s is configured to %s, this configuration value will default to a negative value to disable the resource timeout.",
                                            code(SCHEDULER_MODE.key()),
                                            code(SchedulerExecutionMode.REACTIVE.name()))
                                    .build());

    @Documentation.Section({
        Documentation.Sections.EXPERT_SCHEDULING,
        Documentation.Sections.ALL_JOB_MANAGER
    })
    public static final ConfigOption<Duration> RESOURCE_STABILIZATION_TIMEOUT =
            key("jobmanager.adaptive-scheduler.resource-stabilization-timeout")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(10L))
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "The resource stabilization timeout defines the time the JobManager will wait if fewer than the desired but sufficient resources are available. "
                                                    + "The timeout starts once sufficient resources for running the job are available. "
                                                    + "Once this timeout has passed, the job will start executing with the available resources.")
                                    .linebreak()
                                    .text(
                                            "If %s is configured to %s, this configuration value will default to 0, so that jobs are starting immediately with the available resources.",
                                            code(SCHEDULER_MODE.key()),
                                            code(SchedulerExecutionMode.REACTIVE.name()))
                                    .build());

    /**
     * Config parameter controlling whether partitions should already be released during the job
     * execution.
     */
    @Documentation.ExcludeFromDocumentation(
            "User normally should not be expected to deactivate this feature. "
                    + "We aim at removing this flag eventually.")
    public static final ConfigOption<Boolean> PARTITION_RELEASE_DURING_JOB_EXECUTION =
            key("jobmanager.partition.release-during-job-execution")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "Controls whether partitions should already be released during the job execution.");

    /** @deprecated Use {@link BatchExecutionOptions#ADAPTIVE_AUTO_PARALLELISM_MIN_PARALLELISM}. */
    @Deprecated
    @Documentation.ExcludeFromDocumentation("Hidden for deprecated")
    public static final ConfigOption<Integer> ADAPTIVE_BATCH_SCHEDULER_MIN_PARALLELISM =
            key("jobmanager.adaptive-batch-scheduler.min-parallelism")
                    .intType()
                    .defaultValue(1)
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "The lower bound of allowed parallelism to set adaptively if %s has been set to %s",
                                            code(SCHEDULER.key()),
                                            code(SchedulerType.AdaptiveBatch.name()))
                                    .build());

    /** @deprecated Use {@link BatchExecutionOptions#ADAPTIVE_AUTO_PARALLELISM_MAX_PARALLELISM}. */
    @Deprecated
    @Documentation.ExcludeFromDocumentation("Hidden for deprecated")
    public static final ConfigOption<Integer> ADAPTIVE_BATCH_SCHEDULER_MAX_PARALLELISM =
            key("jobmanager.adaptive-batch-scheduler.max-parallelism")
                    .intType()
                    .defaultValue(128)
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "The upper bound of allowed parallelism to set adaptively if %s has been set to %s",
                                            code(SCHEDULER.key()),
                                            code(SchedulerType.AdaptiveBatch.name()))
                                    .build());

    /**
     * @deprecated Use {@link
     *     BatchExecutionOptions#ADAPTIVE_AUTO_PARALLELISM_AVG_DATA_VOLUME_PER_TASK}.
     */
    @Deprecated
    @Documentation.ExcludeFromDocumentation("Hidden for deprecated")
    public static final ConfigOption<MemorySize> ADAPTIVE_BATCH_SCHEDULER_AVG_DATA_VOLUME_PER_TASK =
            key("jobmanager.adaptive-batch-scheduler.avg-data-volume-per-task")
                    .memoryType()
                    .defaultValue(MemorySize.ofMebiBytes(1024))
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "The average size of data volume to expect each task instance to process if %s has been set to %s. "
                                                    + "Note that when data skew occurs or the decided parallelism reaches the %s (due to too much data), "
                                                    + "the data actually processed by some tasks may far exceed this value.",
                                            code(SCHEDULER.key()),
                                            code(SchedulerType.AdaptiveBatch.name()),
                                            code(ADAPTIVE_BATCH_SCHEDULER_MAX_PARALLELISM.key()))
                                    .build());

    /**
     * @deprecated Use {@link
     *     BatchExecutionOptions#ADAPTIVE_AUTO_PARALLELISM_DEFAULT_SOURCE_PARALLELISM}.
     */
    @Deprecated
    @Documentation.ExcludeFromDocumentation("Hidden for deprecated")
    public static final ConfigOption<Integer> ADAPTIVE_BATCH_SCHEDULER_DEFAULT_SOURCE_PARALLELISM =
            key("jobmanager.adaptive-batch-scheduler.default-source-parallelism")
                    .intType()
                    .defaultValue(1)
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "The default parallelism of source vertices if %s has been set to %s",
                                            code(SCHEDULER.key()),
                                            code(SchedulerType.AdaptiveBatch.name()))
                                    .build());

    /** @deprecated Use {@link BatchExecutionOptions#SPECULATIVE_ENABLED}. */
    @Deprecated
    @Documentation.ExcludeFromDocumentation("Hidden for deprecated")
    public static final ConfigOption<Boolean> SPECULATIVE_ENABLED =
            key("jobmanager.adaptive-batch-scheduler.speculative.enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Controls whether to enable speculative execution.");

    /** @deprecated Use {@link BatchExecutionOptions#SPECULATIVE_MAX_CONCURRENT_EXECUTIONS}. */
    @Deprecated
    @Documentation.ExcludeFromDocumentation("Hidden for deprecated")
    public static final ConfigOption<Integer> SPECULATIVE_MAX_CONCURRENT_EXECUTIONS =
            key("jobmanager.adaptive-batch-scheduler.speculative.max-concurrent-executions")
                    .intType()
                    .defaultValue(2)
                    .withDescription(
                            "Controls the maximum number of execution attempts of each operator "
                                    + "that can execute concurrently, including the original one "
                                    + "and speculative ones.");

    /** @deprecated Use {@link BatchExecutionOptions#BLOCK_SLOW_NODE_DURATION}. */
    @Deprecated
    @Documentation.ExcludeFromDocumentation("Hidden for deprecated")
    public static final ConfigOption<Duration> BLOCK_SLOW_NODE_DURATION =
            key("jobmanager.adaptive-batch-scheduler.speculative.block-slow-node-duration")
                    .durationType()
                    .defaultValue(Duration.ofMinutes(1))
                    .withDescription(
                            "Controls how long an detected slow node should be blocked for.");

    /**
     * The JobManager's ResourceID. If not configured, the ResourceID will be generated randomly.
     */
    @Documentation.Section(Documentation.Sections.ALL_JOB_MANAGER)
    public static final ConfigOption<String> JOB_MANAGER_RESOURCE_ID =
            key("jobmanager.resource-id")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The JobManager's ResourceID. If not configured, the ResourceID will be generated randomly.");

    /** Constraints of upstream hybrid partition data consumption by downstream. */
    public enum HybridPartitionDataConsumeConstraint {
        ALL_PRODUCERS_FINISHED(true),
        ONLY_FINISHED_PRODUCERS(true),
        UNFINISHED_PRODUCERS(false);

        private final boolean onlyConsumeFinishedPartition;

        HybridPartitionDataConsumeConstraint(boolean onlyConsumeFinishedPartition) {
            this.onlyConsumeFinishedPartition = onlyConsumeFinishedPartition;
        }

        public boolean isOnlyConsumeFinishedPartition() {
            return onlyConsumeFinishedPartition;
        }
    }

    @Documentation.Section({
        Documentation.Sections.EXPERT_SCHEDULING,
        Documentation.Sections.ALL_JOB_MANAGER
    })
    public static final ConfigOption<HybridPartitionDataConsumeConstraint>
            HYBRID_PARTITION_DATA_CONSUME_CONSTRAINT =
                    key("jobmanager.partition.hybrid.partition-data-consume-constraint")
                            .enumType(HybridPartitionDataConsumeConstraint.class)
                            .noDefaultValue()
                            .withDescription(
                                    Description.builder()
                                            .text(
                                                    "Controls the constraint that hybrid partition data can be consumed. "
                                                            + "Note that this option is allowed only when %s has been set to %s. "
                                                            + "Accepted values are:",
                                                    code(SCHEDULER.key()),
                                                    code(SchedulerType.AdaptiveBatch.name()))
                                            .list(
                                                    text(
                                                            "'%s': hybrid partition data can be consumed only when all producers are finished.",
                                                            code(ALL_PRODUCERS_FINISHED.name())),
                                                    text(
                                                            "'%s': hybrid partition data can be consumed when its producer is finished.",
                                                            code(ONLY_FINISHED_PRODUCERS.name())),
                                                    text(
                                                            "'%s': hybrid partition data can be consumed even if its producer is un-finished.",
                                                            code(UNFINISHED_PRODUCERS.name())))
                                            .build());

    // ---------------------------------------------------------------------------------------------

    private JobManagerOptions() {
        throw new IllegalAccessError();
    }
}
