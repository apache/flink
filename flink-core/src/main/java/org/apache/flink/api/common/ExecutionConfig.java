/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.api.common;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.Public;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.serialization.SerializerConfig;
import org.apache.flink.api.common.serialization.SerializerConfigImpl;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.DescribedEnum;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.JobManagerOptions.SchedulerType;
import org.apache.flink.configuration.MetricOptions;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.configuration.StateChangelogOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.configuration.description.InlineElement;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static org.apache.flink.configuration.description.TextElement.text;
import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * A config to define the behavior of the program execution. It allows to define (among other
 * options) the following settings:
 *
 * <ul>
 *   <li>The default parallelism of the program, i.e., how many parallel tasks to use for all
 *       functions that do not define a specific value directly.
 *   <li>The number of retries in the case of failed executions.
 *   <li>The delay between execution retries.
 *   <li>Enabling or disabling the "closure cleaner". The closure cleaner pre-processes the
 *       implementations of functions. In case they are (anonymous) inner classes, it removes unused
 *       references to the enclosing class to fix certain serialization-related problems and to
 *       reduce the size of the closure.
 *   <li>The config allows to register types and serializers to increase the efficiency of handling
 *       <i>generic types</i> and <i>POJOs</i>. This is usually only needed when the functions
 *       return not only the types declared in their signature, but also subclasses of those types.
 * </ul>
 */
@Public
public class ExecutionConfig implements Serializable, Archiveable<ArchivedExecutionConfig> {

    // NOTE TO IMPLEMENTERS:
    // Please do not add further fields to this class. Use the ConfigOption stack instead!
    // It is currently very tricky to keep this kind of POJO classes in sync with instances of
    // org.apache.flink.configuration.Configuration. Instances of Configuration are way easier to
    // pass, layer, merge, restrict, copy, filter, etc.
    // See ExecutionOptions.RUNTIME_MODE for a reference implementation. If the option is very
    // crucial for the API, we can add a dedicated setter to StreamExecutionEnvironment. Otherwise,
    // introducing a ConfigOption should be enough.

    private static final long serialVersionUID = 1L;

    /**
     * The flag value indicating use of the default parallelism. This value can be used to reset the
     * parallelism back to the default state.
     */
    public static final int PARALLELISM_DEFAULT = -1;

    /**
     * The flag value indicating an unknown or unset parallelism. This value is not a valid
     * parallelism and indicates that the parallelism should remain unchanged.
     */
    public static final int PARALLELISM_UNKNOWN = -2;

    // --------------------------------------------------------------------------------------------

    /**
     * In the long run, this field should be somehow merged with the {@link Configuration} from
     * StreamExecutionEnvironment.
     */
    private final Configuration configuration;

    private final SerializerConfig serializerConfig;

    @Internal
    public SerializerConfig getSerializerConfig() {
        return serializerConfig;
    }

    public ExecutionConfig() {
        this(new Configuration());
    }

    @Internal
    public ExecutionConfig(Configuration configuration) {
        this.configuration = configuration;
        this.serializerConfig = new SerializerConfigImpl(configuration);
    }

    /**
     * Enables the ClosureCleaner. This analyzes user code functions and sets fields to null that
     * are not used. This will in most cases make closures or anonymous inner classes serializable
     * that where not serializable due to some Scala or Java implementation artifact. User code must
     * be serializable because it needs to be sent to worker nodes.
     */
    public ExecutionConfig enableClosureCleaner() {
        return setClosureCleanerLevel(ClosureCleanerLevel.RECURSIVE);
    }

    /**
     * Disables the ClosureCleaner.
     *
     * @see #enableClosureCleaner()
     */
    public ExecutionConfig disableClosureCleaner() {
        return setClosureCleanerLevel(ClosureCleanerLevel.NONE);
    }

    /**
     * Returns whether the ClosureCleaner is enabled.
     *
     * @see #enableClosureCleaner()
     */
    public boolean isClosureCleanerEnabled() {
        return !(getClosureCleanerLevel() == ClosureCleanerLevel.NONE);
    }

    /**
     * Configures the closure cleaner. Please see {@link ClosureCleanerLevel} for details on the
     * different settings.
     */
    public ExecutionConfig setClosureCleanerLevel(ClosureCleanerLevel level) {
        configuration.set(PipelineOptions.CLOSURE_CLEANER_LEVEL, level);
        return this;
    }

    /** Returns the configured {@link ClosureCleanerLevel}. */
    public ClosureCleanerLevel getClosureCleanerLevel() {
        return configuration.get(PipelineOptions.CLOSURE_CLEANER_LEVEL);
    }

    /**
     * Sets the interval of the automatic watermark emission. Watermarks are used throughout the
     * streaming system to keep track of the progress of time. They are used, for example, for time
     * based windowing.
     *
     * <p>Setting an interval of {@code 0} will disable periodic watermark emission.
     *
     * @param interval The interval between watermarks in milliseconds.
     */
    @PublicEvolving
    public ExecutionConfig setAutoWatermarkInterval(long interval) {
        Preconditions.checkArgument(interval >= 0, "Auto watermark interval must not be negative.");
        return setAutoWatermarkInterval(Duration.ofMillis(interval));
    }

    private ExecutionConfig setAutoWatermarkInterval(Duration autoWatermarkInterval) {
        configuration.set(PipelineOptions.AUTO_WATERMARK_INTERVAL, autoWatermarkInterval);
        return this;
    }

    /**
     * Returns the interval of the automatic watermark emission.
     *
     * @see #setAutoWatermarkInterval(long)
     */
    @PublicEvolving
    public long getAutoWatermarkInterval() {
        return configuration.get(PipelineOptions.AUTO_WATERMARK_INTERVAL).toMillis();
    }

    /**
     * Interval for sending latency tracking marks from the sources to the sinks. Flink will send
     * latency tracking marks from the sources at the specified interval.
     *
     * <p>Setting a tracking interval <= 0 disables the latency tracking.
     *
     * @param interval Interval in milliseconds.
     */
    @PublicEvolving
    public ExecutionConfig setLatencyTrackingInterval(long interval) {
        configuration.set(MetricOptions.LATENCY_INTERVAL, Duration.ofMillis(interval));
        return this;
    }

    /**
     * Returns the latency tracking interval.
     *
     * @return The latency tracking interval in milliseconds
     */
    @PublicEvolving
    public long getLatencyTrackingInterval() {
        return configuration.get(MetricOptions.LATENCY_INTERVAL).toMillis();
    }

    @Internal
    public boolean isLatencyTrackingConfigured() {
        return configuration.getOptional(MetricOptions.LATENCY_INTERVAL).isPresent();
    }

    @Internal
    public boolean isPeriodicMaterializeEnabled() {
        return configuration.get(StateChangelogOptions.PERIODIC_MATERIALIZATION_ENABLED);
    }

    @Internal
    public void enablePeriodicMaterialize(boolean enabled) {
        configuration.set(StateChangelogOptions.PERIODIC_MATERIALIZATION_ENABLED, enabled);
    }

    @Internal
    public long getPeriodicMaterializeIntervalMillis() {
        return configuration
                .get(StateChangelogOptions.PERIODIC_MATERIALIZATION_INTERVAL)
                .toMillis();
    }

    @Internal
    public void setPeriodicMaterializeIntervalMillis(Duration periodicMaterializeInterval) {
        configuration.set(
                StateChangelogOptions.PERIODIC_MATERIALIZATION_INTERVAL,
                periodicMaterializeInterval);
    }

    @Internal
    public int getMaterializationMaxAllowedFailures() {
        return configuration.get(StateChangelogOptions.MATERIALIZATION_MAX_FAILURES_ALLOWED);
    }

    @Internal
    public void setMaterializationMaxAllowedFailures(int materializationMaxAllowedFailures) {
        configuration.set(
                StateChangelogOptions.MATERIALIZATION_MAX_FAILURES_ALLOWED,
                materializationMaxAllowedFailures);
    }

    /**
     * Gets the parallelism with which operation are executed by default. Operations can
     * individually override this value to use a specific parallelism.
     *
     * <p>Other operations may need to run with a different parallelism - for example calling a
     * reduce operation over the entire data set will involve an operation that runs with a
     * parallelism of one (the final reduce to the single result value).
     *
     * @return The parallelism used by operations, unless they override that value. This method
     *     returns {@link #PARALLELISM_DEFAULT} if the environment's default parallelism should be
     *     used.
     */
    public int getParallelism() {
        return configuration.get(CoreOptions.DEFAULT_PARALLELISM);
    }

    /**
     * Sets the parallelism for operations executed through this environment. Setting a parallelism
     * of x here will cause all operators (such as join, map, reduce) to run with x parallel
     * instances.
     *
     * <p>This method overrides the default parallelism for this environment. The local execution
     * environment uses by default a value equal to the number of hardware contexts (CPU cores /
     * threads). When executing the program via the command line client from a JAR file, the default
     * parallelism is the one configured for that setup.
     *
     * @param parallelism The parallelism to use
     */
    public ExecutionConfig setParallelism(int parallelism) {
        if (parallelism != PARALLELISM_UNKNOWN) {
            if (parallelism < 1 && parallelism != PARALLELISM_DEFAULT) {
                throw new IllegalArgumentException(
                        "Parallelism must be at least one, or ExecutionConfig.PARALLELISM_DEFAULT (use system default).");
            }
            configuration.set(CoreOptions.DEFAULT_PARALLELISM, parallelism);
        }
        return this;
    }

    @Internal
    public void resetParallelism() {
        configuration.removeConfig(CoreOptions.DEFAULT_PARALLELISM);
    }

    /**
     * Gets the maximum degree of parallelism defined for the program.
     *
     * <p>The maximum degree of parallelism specifies the upper limit for dynamic scaling. It also
     * defines the number of key groups used for partitioned state.
     *
     * @return Maximum degree of parallelism
     */
    @PublicEvolving
    public int getMaxParallelism() {
        return configuration.get(PipelineOptions.MAX_PARALLELISM);
    }

    /**
     * Sets the maximum degree of parallelism defined for the program.
     *
     * <p>The maximum degree of parallelism specifies the upper limit for dynamic scaling. It also
     * defines the number of key groups used for partitioned state.
     *
     * @param maxParallelism Maximum degree of parallelism to be used for the program.
     */
    @PublicEvolving
    public void setMaxParallelism(int maxParallelism) {
        checkArgument(maxParallelism > 0, "The maximum parallelism must be greater than 0.");
        configuration.set(PipelineOptions.MAX_PARALLELISM, maxParallelism);
    }

    /**
     * Gets the interval (in milliseconds) between consecutive attempts to cancel a running task.
     */
    public long getTaskCancellationInterval() {
        return configuration.get(TaskManagerOptions.TASK_CANCELLATION_INTERVAL).toMillis();
    }

    /**
     * Sets the configuration parameter specifying the interval (in milliseconds) between
     * consecutive attempts to cancel a running task.
     *
     * @param interval the interval (in milliseconds).
     */
    public ExecutionConfig setTaskCancellationInterval(long interval) {
        configuration.set(
                TaskManagerOptions.TASK_CANCELLATION_INTERVAL, Duration.ofMillis(interval));
        return this;
    }

    /**
     * Returns the timeout (in milliseconds) after which an ongoing task cancellation leads to a
     * fatal TaskManager error.
     *
     * <p>The value <code>0</code> means that the timeout is disabled. In this case a stuck
     * cancellation will not lead to a fatal error.
     */
    @PublicEvolving
    public long getTaskCancellationTimeout() {
        return configuration.get(TaskManagerOptions.TASK_CANCELLATION_TIMEOUT).toMillis();
    }

    /**
     * Sets the timeout (in milliseconds) after which an ongoing task cancellation is considered
     * failed, leading to a fatal TaskManager error.
     *
     * <p>The cluster default is configured via {@link
     * TaskManagerOptions#TASK_CANCELLATION_TIMEOUT}.
     *
     * <p>The value <code>0</code> disables the timeout. In this case a stuck cancellation will not
     * lead to a fatal error.
     *
     * @param timeout The task cancellation timeout (in milliseconds).
     */
    @PublicEvolving
    public ExecutionConfig setTaskCancellationTimeout(long timeout) {
        checkArgument(timeout >= 0, "Timeout needs to be >= 0.");
        configuration.set(TaskManagerOptions.TASK_CANCELLATION_TIMEOUT, Duration.ofMillis(timeout));
        return this;
    }

    @Internal
    public Optional<SchedulerType> getSchedulerType() {
        return configuration.getOptional(JobManagerOptions.SCHEDULER);
    }

    /**
     * Enables the Flink runtime to auto-generate UID's for operators.
     *
     * @see #disableAutoGeneratedUIDs()
     */
    public void enableAutoGeneratedUIDs() {
        setAutoGeneratedUids(true);
    }

    /**
     * Disables auto-generated UIDs. Forces users to manually specify UIDs on DataStream
     * applications.
     *
     * <p>It is highly recommended that users specify UIDs before deploying to production since they
     * are used to match state in savepoints to operators in a job. Because auto-generated ID's are
     * likely to change when modifying a job, specifying custom IDs allow an application to evolve
     * overtime without discarding state.
     */
    public void disableAutoGeneratedUIDs() {
        setAutoGeneratedUids(false);
    }

    private void setAutoGeneratedUids(boolean autoGeneratedUids) {
        configuration.set(PipelineOptions.AUTO_GENERATE_UIDS, autoGeneratedUids);
    }

    /**
     * Checks whether auto generated UIDs are supported.
     *
     * <p>Auto generated UIDs are enabled by default.
     *
     * @see #enableAutoGeneratedUIDs()
     * @see #disableAutoGeneratedUIDs()
     */
    public boolean hasAutoGeneratedUIDsEnabled() {
        return configuration.get(PipelineOptions.AUTO_GENERATE_UIDS);
    }

    /**
     * Enables reusing objects that Flink internally uses for deserialization and passing data to
     * user-code functions. Keep in mind that this can lead to bugs when the user-code function of
     * an operation is not aware of this behaviour.
     */
    public ExecutionConfig enableObjectReuse() {
        return setObjectReuse(true);
    }

    /**
     * Disables reusing objects that Flink internally uses for deserialization and passing data to
     * user-code functions. @see #enableObjectReuse()
     */
    public ExecutionConfig disableObjectReuse() {
        return setObjectReuse(false);
    }

    private ExecutionConfig setObjectReuse(boolean objectReuse) {
        configuration.set(PipelineOptions.OBJECT_REUSE, objectReuse);
        return this;
    }

    /** Returns whether object reuse has been enabled or disabled. @see #enableObjectReuse() */
    public boolean isObjectReuseEnabled() {
        return configuration.get(PipelineOptions.OBJECT_REUSE);
    }

    public GlobalJobParameters getGlobalJobParameters() {
        return configuration
                .getOptional(PipelineOptions.GLOBAL_JOB_PARAMETERS)
                .map(MapBasedJobParameters::new)
                .orElse(new MapBasedJobParameters(Collections.emptyMap()));
    }

    /**
     * Register a custom, serializable user configuration object.
     *
     * @param globalJobParameters Custom user configuration object
     */
    public void setGlobalJobParameters(GlobalJobParameters globalJobParameters) {
        Preconditions.checkNotNull(globalJobParameters, "globalJobParameters shouldn't be null");
        setGlobalJobParameters(globalJobParameters.toMap());
    }

    private void setGlobalJobParameters(Map<String, String> parameters) {
        configuration.set(PipelineOptions.GLOBAL_JOB_PARAMETERS, parameters);
    }

    public boolean isUseSnapshotCompression() {
        return configuration.get(ExecutionOptions.SNAPSHOT_COMPRESSION);
    }

    public void setUseSnapshotCompression(boolean useSnapshotCompression) {
        configuration.set(ExecutionOptions.SNAPSHOT_COMPRESSION, useSnapshotCompression);
    }

    // --------------------------------------------------------------------------------------------
    //  Asynchronous execution configurations
    // --------------------------------------------------------------------------------------------

    @Experimental
    public int getAsyncInflightRecordsLimit() {
        return configuration.get(ExecutionOptions.ASYNC_INFLIGHT_RECORDS_LIMIT);
    }

    @Experimental
    public ExecutionConfig setAsyncInflightRecordsLimit(int limit) {
        configuration.set(ExecutionOptions.ASYNC_INFLIGHT_RECORDS_LIMIT, limit);
        return this;
    }

    @Experimental
    public int getAsyncStateBufferSize() {
        return configuration.get(ExecutionOptions.ASYNC_STATE_BUFFER_SIZE);
    }

    @Experimental
    public ExecutionConfig setAsyncStateBufferSize(int bufferSize) {
        configuration.set(ExecutionOptions.ASYNC_STATE_BUFFER_SIZE, bufferSize);
        return this;
    }

    @Experimental
    public long getAsyncStateBufferTimeout() {
        return configuration.get(ExecutionOptions.ASYNC_STATE_BUFFER_TIMEOUT);
    }

    @Experimental
    public ExecutionConfig setAsyncStateBufferTimeout(long timeout) {
        configuration.set(ExecutionOptions.ASYNC_STATE_BUFFER_TIMEOUT, timeout);
        return this;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof ExecutionConfig) {
            ExecutionConfig other = (ExecutionConfig) obj;

            return Objects.equals(configuration, other.configuration)
                    && Objects.equals(serializerConfig, other.serializerConfig);
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(configuration, serializerConfig);
    }

    @Override
    public String toString() {
        return "ExecutionConfig{"
                + "configuration="
                + configuration
                + ", serializerConfig="
                + serializerConfig
                + '}';
    }

    @Override
    @Internal
    public ArchivedExecutionConfig archive() {
        return new ArchivedExecutionConfig(this);
    }

    // ------------------------------ Utilities  ----------------------------------

    /**
     * Abstract class for a custom user configuration object registered at the execution config.
     *
     * <p>This user config is accessible at runtime through
     * getRuntimeContext().getExecutionConfig().GlobalJobParameters()
     */
    public static class GlobalJobParameters implements Serializable {
        private static final long serialVersionUID = 1L;

        /**
         * Convert UserConfig into a {@code Map<String, String>} representation. This can be used by
         * the runtime, for example for presenting the user config in the web frontend.
         *
         * @return Key/Value representation of the UserConfig
         */
        public Map<String, String> toMap() {
            return Collections.emptyMap();
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null || this.getClass() != obj.getClass()) {
                return false;
            }

            return true;
        }

        @Override
        public int hashCode() {
            return Objects.hash();
        }
    }

    /** Configuration settings for the closure cleaner. */
    public enum ClosureCleanerLevel implements DescribedEnum {
        NONE(text("Disables the closure cleaner completely.")),

        TOP_LEVEL(text("Cleans only the top-level class without recursing into fields.")),

        RECURSIVE(text("Cleans all fields recursively."));

        private final InlineElement description;

        ClosureCleanerLevel(InlineElement description) {
            this.description = description;
        }

        @Override
        public InlineElement getDescription() {
            return description;
        }
    }

    /**
     * Sets all relevant options contained in the {@link ReadableConfig} such as e.g. {@link
     * PipelineOptions#CLOSURE_CLEANER_LEVEL}.
     *
     * <p>It will change the value of a setting only if a corresponding option was set in the {@code
     * configuration}. If a key is not present, the current value of a field will remain untouched.
     *
     * @param configuration a configuration to read the values from
     * @param classLoader a class loader to use when loading classes
     */
    public void configure(ReadableConfig configuration, ClassLoader classLoader) {
        configuration
                .getOptional(PipelineOptions.AUTO_GENERATE_UIDS)
                .ifPresent(this::setAutoGeneratedUids);
        configuration
                .getOptional(PipelineOptions.AUTO_WATERMARK_INTERVAL)
                .ifPresent(this::setAutoWatermarkInterval);
        configuration
                .getOptional(PipelineOptions.CLOSURE_CLEANER_LEVEL)
                .ifPresent(this::setClosureCleanerLevel);
        configuration
                .getOptional(PipelineOptions.GLOBAL_JOB_PARAMETERS)
                .ifPresent(this::setGlobalJobParameters);

        configuration
                .getOptional(MetricOptions.LATENCY_INTERVAL)
                .ifPresent(interval -> setLatencyTrackingInterval(interval.toMillis()));

        configuration
                .getOptional(StateChangelogOptions.PERIODIC_MATERIALIZATION_ENABLED)
                .ifPresent(this::enablePeriodicMaterialize);
        configuration
                .getOptional(StateChangelogOptions.PERIODIC_MATERIALIZATION_INTERVAL)
                .ifPresent(this::setPeriodicMaterializeIntervalMillis);
        configuration
                .getOptional(StateChangelogOptions.MATERIALIZATION_MAX_FAILURES_ALLOWED)
                .ifPresent(this::setMaterializationMaxAllowedFailures);

        configuration
                .getOptional(PipelineOptions.MAX_PARALLELISM)
                .ifPresent(this::setMaxParallelism);
        configuration.getOptional(CoreOptions.DEFAULT_PARALLELISM).ifPresent(this::setParallelism);
        configuration.getOptional(PipelineOptions.OBJECT_REUSE).ifPresent(this::setObjectReuse);
        configuration
                .getOptional(TaskManagerOptions.TASK_CANCELLATION_INTERVAL)
                .ifPresent(interval -> setTaskCancellationInterval(interval.toMillis()));
        configuration
                .getOptional(TaskManagerOptions.TASK_CANCELLATION_TIMEOUT)
                .ifPresent(timeout -> setTaskCancellationTimeout(timeout.toMillis()));
        configuration
                .getOptional(ExecutionOptions.SNAPSHOT_COMPRESSION)
                .ifPresent(this::setUseSnapshotCompression);
        configuration
                .getOptional(RestartStrategyOptions.RESTART_STRATEGY)
                .ifPresent(s -> this.setRestartStrategy(configuration));

        configuration
                .getOptional(JobManagerOptions.SCHEDULER)
                .ifPresent(t -> this.configuration.set(JobManagerOptions.SCHEDULER, t));

        serializerConfig.configure(configuration, classLoader);
    }

    private void setRestartStrategy(ReadableConfig configuration) {
        Map<String, String> map = configuration.toMap();
        Map<String, String> restartStrategyEntries = new HashMap<>();
        for (Map.Entry<String, String> entry : map.entrySet()) {
            if (entry.getKey().startsWith(RestartStrategyOptions.RESTART_STRATEGY_CONFIG_PREFIX)) {
                restartStrategyEntries.put(entry.getKey(), entry.getValue());
            }
        }
        this.configuration.addAll(Configuration.fromMap(restartStrategyEntries));
    }

    /**
     * @return A copy of internal {@link #configuration}. Note it is missing all options that are
     *     stored as plain java fields in {@link ExecutionConfig}.
     */
    @Internal
    public Configuration toConfiguration() {
        return new Configuration(configuration);
    }

    private static class MapBasedJobParameters extends GlobalJobParameters {
        private final Map<String, String> properties;

        private MapBasedJobParameters(Map<String, String> properties) {
            this.properties = properties;
        }

        @Override
        public Map<String, String> toMap() {
            return properties;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof GlobalJobParameters)) {
                return false;
            }
            GlobalJobParameters that = (GlobalJobParameters) o;
            return Objects.equals(properties, that.toMap());
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), properties);
        }
    }
}
