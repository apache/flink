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

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.Public;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.DescribedEnum;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.JobManagerOptions.SchedulerType;
import org.apache.flink.configuration.MetricOptions;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.configuration.StateChangelogOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.configuration.description.InlineElement;
import org.apache.flink.util.Preconditions;

import com.esotericsoftware.kryo.Serializer;

import java.io.Serializable;
import java.time.Duration;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.flink.configuration.ConfigOptions.key;
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
 *   <li>The {@link ExecutionMode} of the program: Batch or Pipelined. The default execution mode is
 *       {@link ExecutionMode#PIPELINED}
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
     * The constant to use for the parallelism, if the system should use the number of currently
     * available slots.
     */
    @Deprecated public static final int PARALLELISM_AUTO_MAX = Integer.MAX_VALUE;

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

    private static final long DEFAULT_RESTART_DELAY = 10000L;

    /**
     * Internal {@link ConfigOption}s, that are not exposed and it's not possible to configure them
     * via config files. We are defining them here, so that we can store them in the {@link
     * #configuration}.
     *
     * <p>If you decide to expose any of those {@link ConfigOption}s, please double-check if the
     * key, type and descriptions are sensible, as the initial values are arbitrary.
     */
    // --------------------------------------------------------------------------------------------

    private static final ConfigOption<ExecutionMode> EXECUTION_MODE =
            key("hidden.execution.mode")
                    .enumType(ExecutionMode.class)
                    .defaultValue(ExecutionMode.PIPELINED)
                    .withDescription("Defines how data exchange happens - batch or pipelined");

    /**
     * Use {@link
     * org.apache.flink.api.common.restartstrategy.RestartStrategies.RestartStrategyConfiguration}
     */
    @Deprecated
    private static final ConfigOption<Integer> EXECUTION_RETRIES =
            key("hidden.execution.retries")
                    .intType()
                    .defaultValue(-1)
                    .withDescription(
                            "Should no longer be used because it is subsumed by RestartStrategyConfiguration");
    // --------------------------------------------------------------------------------------------

    /**
     * In the long run, this field should be somehow merged with the {@link Configuration} from
     * StreamExecutionEnvironment.
     */
    private final Configuration configuration = new Configuration();

    /**
     * @deprecated Should no longer be used because it is subsumed by RestartStrategyConfiguration
     */
    @Deprecated private long executionRetryDelay = DEFAULT_RESTART_DELAY;

    private RestartStrategies.RestartStrategyConfiguration restartStrategyConfiguration =
            new RestartStrategies.FallbackRestartStrategyConfiguration();

    // ------------------------------- User code values --------------------------------------------

    // Serializers and types registered with Kryo and the PojoSerializer
    // we store them in linked maps/sets to ensure they are registered in order in all kryo
    // instances.

    private LinkedHashMap<Class<?>, SerializableSerializer<?>> registeredTypesWithKryoSerializers =
            new LinkedHashMap<>();

    private LinkedHashMap<Class<?>, Class<? extends Serializer<?>>>
            registeredTypesWithKryoSerializerClasses = new LinkedHashMap<>();

    private LinkedHashMap<Class<?>, SerializableSerializer<?>> defaultKryoSerializers =
            new LinkedHashMap<>();

    private LinkedHashMap<Class<?>, Class<? extends Serializer<?>>> defaultKryoSerializerClasses =
            new LinkedHashMap<>();

    private LinkedHashSet<Class<?>> registeredKryoTypes = new LinkedHashSet<>();

    private LinkedHashSet<Class<?>> registeredPojoTypes = new LinkedHashSet<>();

    // --------------------------------------------------------------------------------------------

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
        configuration.set(MetricOptions.LATENCY_INTERVAL, interval);
        return this;
    }

    /**
     * Returns the latency tracking interval.
     *
     * @return The latency tracking interval in milliseconds
     */
    @PublicEvolving
    public long getLatencyTrackingInterval() {
        return configuration.get(MetricOptions.LATENCY_INTERVAL);
    }

    @Internal
    public boolean isLatencyTrackingConfigured() {
        return configuration.getOptional(MetricOptions.LATENCY_INTERVAL).isPresent();
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
        return configuration.get(TaskManagerOptions.TASK_CANCELLATION_INTERVAL);
    }

    /**
     * Sets the configuration parameter specifying the interval (in milliseconds) between
     * consecutive attempts to cancel a running task.
     *
     * @param interval the interval (in milliseconds).
     */
    public ExecutionConfig setTaskCancellationInterval(long interval) {
        configuration.set(TaskManagerOptions.TASK_CANCELLATION_INTERVAL, interval);
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
        return configuration.get(TaskManagerOptions.TASK_CANCELLATION_TIMEOUT);
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
        configuration.set(TaskManagerOptions.TASK_CANCELLATION_TIMEOUT, timeout);
        return this;
    }

    /**
     * Sets the restart strategy to be used for recovery.
     *
     * <pre>{@code
     * ExecutionConfig config = env.getConfig();
     *
     * config.setRestartStrategy(RestartStrategies.fixedDelayRestart(
     * 	10,  // number of retries
     * 	1000 // delay between retries));
     * }</pre>
     *
     * @param restartStrategyConfiguration Configuration defining the restart strategy to use
     */
    @PublicEvolving
    public void setRestartStrategy(
            RestartStrategies.RestartStrategyConfiguration restartStrategyConfiguration) {
        this.restartStrategyConfiguration =
                Preconditions.checkNotNull(restartStrategyConfiguration);
    }

    /**
     * Returns the restart strategy which has been set for the current job.
     *
     * @return The specified restart configuration
     */
    @PublicEvolving
    @SuppressWarnings("deprecation")
    public RestartStrategies.RestartStrategyConfiguration getRestartStrategy() {
        if (restartStrategyConfiguration
                instanceof RestartStrategies.FallbackRestartStrategyConfiguration) {
            // support the old API calls by creating a restart strategy from them
            if (getNumberOfExecutionRetries() > 0 && getExecutionRetryDelay() >= 0) {
                return RestartStrategies.fixedDelayRestart(
                        getNumberOfExecutionRetries(), getExecutionRetryDelay());
            } else if (getNumberOfExecutionRetries() == 0) {
                return RestartStrategies.noRestart();
            } else {
                return restartStrategyConfiguration;
            }
        } else {
            return restartStrategyConfiguration;
        }
    }

    @Internal
    public Optional<SchedulerType> getSchedulerType() {
        return configuration.getOptional(JobManagerOptions.SCHEDULER);
    }

    /**
     * Gets the number of times the system will try to re-execute failed tasks. A value of {@code
     * -1} indicates that the system default value (as defined in the configuration) should be used.
     *
     * @return The number of times the system will try to re-execute failed tasks.
     * @deprecated Should no longer be used because it is subsumed by RestartStrategyConfiguration
     */
    @Deprecated
    public int getNumberOfExecutionRetries() {
        return configuration.get(EXECUTION_RETRIES);
    }

    /**
     * Returns the delay between execution retries.
     *
     * @return The delay between successive execution retries in milliseconds.
     * @deprecated Should no longer be used because it is subsumed by RestartStrategyConfiguration
     */
    @Deprecated
    public long getExecutionRetryDelay() {
        return executionRetryDelay;
    }

    /**
     * Sets the number of times that failed tasks are re-executed. A value of zero effectively
     * disables fault tolerance. A value of {@code -1} indicates that the system default value (as
     * defined in the configuration) should be used.
     *
     * @param numberOfExecutionRetries The number of times the system will try to re-execute failed
     *     tasks.
     * @return The current execution configuration
     * @deprecated This method will be replaced by {@link #setRestartStrategy}. The {@link
     *     RestartStrategies.FixedDelayRestartStrategyConfiguration} contains the number of
     *     execution retries.
     */
    @Deprecated
    public ExecutionConfig setNumberOfExecutionRetries(int numberOfExecutionRetries) {
        if (numberOfExecutionRetries < -1) {
            throw new IllegalArgumentException(
                    "The number of execution retries must be non-negative, or -1 (use system default)");
        }
        configuration.set(EXECUTION_RETRIES, numberOfExecutionRetries);
        return this;
    }

    /**
     * Sets the delay between executions.
     *
     * @param executionRetryDelay The number of milliseconds the system will wait to retry.
     * @return The current execution configuration
     * @deprecated This method will be replaced by {@link #setRestartStrategy}. The {@link
     *     RestartStrategies.FixedDelayRestartStrategyConfiguration} contains the delay between
     *     successive execution attempts.
     */
    @Deprecated
    public ExecutionConfig setExecutionRetryDelay(long executionRetryDelay) {
        if (executionRetryDelay < 0) {
            throw new IllegalArgumentException("The delay between retries must be non-negative.");
        }
        this.executionRetryDelay = executionRetryDelay;
        return this;
    }

    /**
     * Sets the execution mode to execute the program. The execution mode defines whether data
     * exchanges are performed in a batch or on a pipelined manner.
     *
     * <p>The default execution mode is {@link ExecutionMode#PIPELINED}.
     *
     * @param executionMode The execution mode to use.
     * @deprecated The {@link ExecutionMode} is deprecated because it's only used in DataSet APIs.
     *     All Flink DataSet APIs are deprecated since Flink 1.18 and will be removed in a future
     *     Flink major version. You can still build your application in DataSet, but you should move
     *     to either the DataStream and/or Table API.
     * @see <a href="https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=158866741">
     *     FLIP-131: Consolidate the user-facing Dataflow SDKs/APIs (and deprecate the DataSet
     *     API</a>
     */
    @Deprecated
    public void setExecutionMode(ExecutionMode executionMode) {
        configuration.set(EXECUTION_MODE, executionMode);
    }

    /**
     * Gets the execution mode used to execute the program. The execution mode defines whether data
     * exchanges are performed in a batch or on a pipelined manner.
     *
     * <p>The default execution mode is {@link ExecutionMode#PIPELINED}.
     *
     * @return The execution mode for the program.
     * @deprecated The {@link ExecutionMode} is deprecated because it's only used in DataSet APIs.
     *     All Flink DataSet APIs are deprecated since Flink 1.18 and will be removed in a future
     *     Flink major version. You can still build your application in DataSet, but you should move
     *     to either the DataStream and/or Table API.
     * @see <a href="https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=158866741">
     *     FLIP-131: Consolidate the user-facing Dataflow SDKs/APIs (and deprecate the DataSet
     *     API</a>
     */
    @Deprecated
    public ExecutionMode getExecutionMode() {
        return configuration.get(EXECUTION_MODE);
    }

    /**
     * This method is deprecated. It was used to set the {@link InputDependencyConstraint} utilized
     * by the old scheduler implementations which got removed as part of FLINK-20589. The current
     * implementation has no effect.
     *
     * @param ignored Ignored parameter.
     * @deprecated due to the deprecation of {@code InputDependencyConstraint}.
     */
    @PublicEvolving
    @Deprecated
    public void setDefaultInputDependencyConstraint(InputDependencyConstraint ignored) {}

    /**
     * This method is deprecated. It was used to return the {@link InputDependencyConstraint}
     * utilized by the old scheduler implementations. These implementations were removed as part of
     * FLINK-20589.
     *
     * @return The previous default constraint {@link InputDependencyConstraint#ANY}.
     * @deprecated due to the deprecation of {@code InputDependencyConstraint}.
     */
    @PublicEvolving
    @Deprecated
    public InputDependencyConstraint getDefaultInputDependencyConstraint() {
        return InputDependencyConstraint.ANY;
    }

    /**
     * Force TypeExtractor to use Kryo serializer for POJOS even though we could analyze as POJO. In
     * some cases this might be preferable. For example, when using interfaces with subclasses that
     * cannot be analyzed as POJO.
     */
    public void enableForceKryo() {
        setForceKryo(true);
    }

    /** Disable use of Kryo serializer for all POJOs. */
    public void disableForceKryo() {
        setForceKryo(false);
    }

    private void setForceKryo(boolean forceKryo) {
        configuration.set(PipelineOptions.FORCE_KRYO, forceKryo);
    }

    public boolean isForceKryoEnabled() {
        return configuration.get(PipelineOptions.FORCE_KRYO);
    }

    /**
     * Enables the use generic types which are serialized via Kryo.
     *
     * <p>Generic types are enabled by default.
     *
     * @see #disableGenericTypes()
     */
    public void enableGenericTypes() {
        setGenericTypes(true);
    }

    /**
     * Disables the use of generic types (types that would be serialized via Kryo). If this option
     * is used, Flink will throw an {@code UnsupportedOperationException} whenever it encounters a
     * data type that would go through Kryo for serialization.
     *
     * <p>Disabling generic types can be helpful to eagerly find and eliminate the use of types that
     * would go through Kryo serialization during runtime. Rather than checking types individually,
     * using this option will throw exceptions eagerly in the places where generic types are used.
     *
     * <p><b>Important:</b> We recommend to use this option only during development and
     * pre-production phases, not during actual production use. The application program and/or the
     * input data may be such that new, previously unseen, types occur at some point. In that case,
     * setting this option would cause the program to fail.
     *
     * @see #enableGenericTypes()
     */
    public void disableGenericTypes() {
        setGenericTypes(false);
    }

    private void setGenericTypes(boolean genericTypes) {
        configuration.set(PipelineOptions.GENERIC_TYPES, genericTypes);
    }

    /**
     * Checks whether generic types are supported. Generic types are types that go through Kryo
     * during serialization.
     *
     * <p>Generic types are enabled by default.
     *
     * @see #enableGenericTypes()
     * @see #disableGenericTypes()
     */
    public boolean hasGenericTypesDisabled() {
        return !configuration.get(PipelineOptions.GENERIC_TYPES);
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
     * Forces Flink to use the Apache Avro serializer for POJOs.
     *
     * <p><b>Important:</b> Make sure to include the <i>flink-avro</i> module.
     */
    public void enableForceAvro() {
        setForceAvro(true);
    }

    /** Disables the Apache Avro serializer as the forced serializer for POJOs. */
    public void disableForceAvro() {
        setForceAvro(false);
    }

    private void setForceAvro(boolean forceAvro) {
        configuration.set(PipelineOptions.FORCE_AVRO, forceAvro);
    }

    /** Returns whether the Apache Avro is the default serializer for POJOs. */
    public boolean isForceAvroEnabled() {
        return configuration.get(PipelineOptions.FORCE_AVRO);
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

    // --------------------------------------------------------------------------------------------
    //  Registry for types and serializers
    // --------------------------------------------------------------------------------------------

    /**
     * Adds a new Kryo default serializer to the Runtime.
     *
     * <p>Note that the serializer instance must be serializable (as defined by
     * java.io.Serializable), because it may be distributed to the worker nodes by java
     * serialization.
     *
     * @param type The class of the types serialized with the given serializer.
     * @param serializer The serializer to use.
     */
    public <T extends Serializer<?> & Serializable> void addDefaultKryoSerializer(
            Class<?> type, T serializer) {
        if (type == null || serializer == null) {
            throw new NullPointerException("Cannot register null class or serializer.");
        }

        defaultKryoSerializers.put(type, new SerializableSerializer<>(serializer));
    }

    /**
     * Adds a new Kryo default serializer to the Runtime.
     *
     * @param type The class of the types serialized with the given serializer.
     * @param serializerClass The class of the serializer to use.
     */
    public void addDefaultKryoSerializer(
            Class<?> type, Class<? extends Serializer<?>> serializerClass) {
        if (type == null || serializerClass == null) {
            throw new NullPointerException("Cannot register null class or serializer.");
        }
        defaultKryoSerializerClasses.put(type, serializerClass);
    }

    /**
     * Registers the given type with a Kryo Serializer.
     *
     * <p>Note that the serializer instance must be serializable (as defined by
     * java.io.Serializable), because it may be distributed to the worker nodes by java
     * serialization.
     *
     * @param type The class of the types serialized with the given serializer.
     * @param serializer The serializer to use.
     */
    public <T extends Serializer<?> & Serializable> void registerTypeWithKryoSerializer(
            Class<?> type, T serializer) {
        if (type == null || serializer == null) {
            throw new NullPointerException("Cannot register null class or serializer.");
        }

        registeredTypesWithKryoSerializers.put(type, new SerializableSerializer<>(serializer));
    }

    /**
     * Registers the given Serializer via its class as a serializer for the given type at the
     * KryoSerializer
     *
     * @param type The class of the types serialized with the given serializer.
     * @param serializerClass The class of the serializer to use.
     */
    @SuppressWarnings("rawtypes")
    public void registerTypeWithKryoSerializer(
            Class<?> type, Class<? extends Serializer> serializerClass) {
        if (type == null || serializerClass == null) {
            throw new NullPointerException("Cannot register null class or serializer.");
        }

        @SuppressWarnings("unchecked")
        Class<? extends Serializer<?>> castedSerializerClass =
                (Class<? extends Serializer<?>>) serializerClass;
        registeredTypesWithKryoSerializerClasses.put(type, castedSerializerClass);
    }

    /**
     * Registers the given type with the serialization stack. If the type is eventually serialized
     * as a POJO, then the type is registered with the POJO serializer. If the type ends up being
     * serialized with Kryo, then it will be registered at Kryo to make sure that only tags are
     * written.
     *
     * @param type The class of the type to register.
     */
    public void registerPojoType(Class<?> type) {
        if (type == null) {
            throw new NullPointerException("Cannot register null type class.");
        }
        if (!registeredPojoTypes.contains(type)) {
            registeredPojoTypes.add(type);
        }
    }

    /**
     * Registers the given type with the serialization stack. If the type is eventually serialized
     * as a POJO, then the type is registered with the POJO serializer. If the type ends up being
     * serialized with Kryo, then it will be registered at Kryo to make sure that only tags are
     * written.
     *
     * @param type The class of the type to register.
     */
    public void registerKryoType(Class<?> type) {
        if (type == null) {
            throw new NullPointerException("Cannot register null type class.");
        }
        registeredKryoTypes.add(type);
    }

    /** Returns the registered types with Kryo Serializers. */
    public LinkedHashMap<Class<?>, SerializableSerializer<?>>
            getRegisteredTypesWithKryoSerializers() {
        return registeredTypesWithKryoSerializers;
    }

    /** Returns the registered types with their Kryo Serializer classes. */
    public LinkedHashMap<Class<?>, Class<? extends Serializer<?>>>
            getRegisteredTypesWithKryoSerializerClasses() {
        return registeredTypesWithKryoSerializerClasses;
    }

    /** Returns the registered default Kryo Serializers. */
    public LinkedHashMap<Class<?>, SerializableSerializer<?>> getDefaultKryoSerializers() {
        return defaultKryoSerializers;
    }

    /** Returns the registered default Kryo Serializer classes. */
    public LinkedHashMap<Class<?>, Class<? extends Serializer<?>>>
            getDefaultKryoSerializerClasses() {
        return defaultKryoSerializerClasses;
    }

    /** Returns the registered Kryo types. */
    public LinkedHashSet<Class<?>> getRegisteredKryoTypes() {
        if (isForceKryoEnabled()) {
            // if we force kryo, we must also return all the types that
            // were previously only registered as POJO
            LinkedHashSet<Class<?>> result = new LinkedHashSet<>();
            result.addAll(registeredKryoTypes);
            for (Class<?> t : registeredPojoTypes) {
                if (!result.contains(t)) {
                    result.add(t);
                }
            }
            return result;
        } else {
            return registeredKryoTypes;
        }
    }

    /** Returns the registered POJO types. */
    public LinkedHashSet<Class<?>> getRegisteredPojoTypes() {
        return registeredPojoTypes;
    }

    /**
     * Get if the auto type registration is disabled.
     *
     * @return if the auto type registration is disabled.
     * @deprecated The method is deprecated because it's only used in DataSet API. All Flink DataSet
     *     APIs are deprecated since Flink 1.18 and will be removed in a future Flink major version.
     *     You can still build your application in DataSet, but you should move to either the
     *     DataStream and/or Table API.
     * @see <a href="https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=158866741">
     *     FLIP-131: Consolidate the user-facing Dataflow SDKs/APIs (and deprecate the DataSet
     *     API</a>
     */
    @Deprecated
    public boolean isAutoTypeRegistrationDisabled() {
        return !configuration.get(PipelineOptions.AUTO_TYPE_REGISTRATION);
    }

    /**
     * Control whether Flink is automatically registering all types in the user programs with Kryo.
     *
     * @deprecated The method is deprecated because it's only used in DataSet API. All Flink DataSet
     *     APIs are deprecated since Flink 1.18 and will be removed in a future Flink major version.
     *     You can still build your application in DataSet, but you should move to either the
     *     DataStream and/or Table API.
     * @see <a href="https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=158866741">
     *     FLIP-131: Consolidate the user-facing Dataflow SDKs/APIs (and deprecate the DataSet
     *     API</a>
     */
    @Deprecated
    public void disableAutoTypeRegistration() {
        setAutoTypeRegistration(false);
    }

    private void setAutoTypeRegistration(Boolean autoTypeRegistration) {
        configuration.set(PipelineOptions.AUTO_TYPE_REGISTRATION, autoTypeRegistration);
    }

    public boolean isUseSnapshotCompression() {
        return configuration.get(ExecutionOptions.SNAPSHOT_COMPRESSION);
    }

    public void setUseSnapshotCompression(boolean useSnapshotCompression) {
        configuration.set(ExecutionOptions.SNAPSHOT_COMPRESSION, useSnapshotCompression);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof ExecutionConfig) {
            ExecutionConfig other = (ExecutionConfig) obj;

            return Objects.equals(configuration, other.configuration)
                    && ((restartStrategyConfiguration == null
                                    && other.restartStrategyConfiguration == null)
                            || (null != restartStrategyConfiguration
                                    && restartStrategyConfiguration.equals(
                                            other.restartStrategyConfiguration)))
                    && registeredTypesWithKryoSerializerClasses.equals(
                            other.registeredTypesWithKryoSerializerClasses)
                    && defaultKryoSerializerClasses.equals(other.defaultKryoSerializerClasses)
                    && registeredKryoTypes.equals(other.registeredKryoTypes)
                    && registeredPojoTypes.equals(other.registeredPojoTypes);

        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                configuration,
                restartStrategyConfiguration,
                registeredTypesWithKryoSerializerClasses,
                defaultKryoSerializerClasses,
                registeredKryoTypes,
                registeredPojoTypes);
    }

    @Override
    public String toString() {
        return "ExecutionConfig{"
                + "configuration="
                + configuration
                + ", executionRetryDelay="
                + executionRetryDelay
                + ", restartStrategyConfiguration="
                + restartStrategyConfiguration
                + ", registeredTypesWithKryoSerializers="
                + registeredTypesWithKryoSerializers
                + ", registeredTypesWithKryoSerializerClasses="
                + registeredTypesWithKryoSerializerClasses
                + ", defaultKryoSerializers="
                + defaultKryoSerializers
                + ", defaultKryoSerializerClasses="
                + defaultKryoSerializerClasses
                + ", registeredKryoTypes="
                + registeredKryoTypes
                + ", registeredPojoTypes="
                + registeredPojoTypes
                + '}';
    }

    /**
     * This method simply checks whether the object is an {@link ExecutionConfig} instance.
     *
     * @deprecated It is not intended to be used by users.
     */
    @Deprecated
    public boolean canEqual(Object obj) {
        return obj instanceof ExecutionConfig;
    }

    @Override
    @Internal
    public ArchivedExecutionConfig archive() {
        return new ArchivedExecutionConfig(this);
    }

    // ------------------------------ Utilities  ----------------------------------

    public static class SerializableSerializer<T extends Serializer<?> & Serializable>
            implements Serializable {
        private static final long serialVersionUID = 4687893502781067189L;

        private T serializer;

        public SerializableSerializer(T serializer) {
            this.serializer = serializer;
        }

        public T getSerializer() {
            return serializer;
        }
    }

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
                .getOptional(PipelineOptions.AUTO_TYPE_REGISTRATION)
                .ifPresent(this::setAutoTypeRegistration);
        configuration
                .getOptional(PipelineOptions.AUTO_GENERATE_UIDS)
                .ifPresent(this::setAutoGeneratedUids);
        configuration
                .getOptional(PipelineOptions.AUTO_WATERMARK_INTERVAL)
                .ifPresent(this::setAutoWatermarkInterval);
        configuration
                .getOptional(PipelineOptions.CLOSURE_CLEANER_LEVEL)
                .ifPresent(this::setClosureCleanerLevel);
        configuration.getOptional(PipelineOptions.FORCE_AVRO).ifPresent(this::setForceAvro);
        configuration.getOptional(PipelineOptions.GENERIC_TYPES).ifPresent(this::setGenericTypes);
        configuration.getOptional(PipelineOptions.FORCE_KRYO).ifPresent(this::setForceKryo);
        configuration
                .getOptional(PipelineOptions.GLOBAL_JOB_PARAMETERS)
                .ifPresent(this::setGlobalJobParameters);

        configuration
                .getOptional(MetricOptions.LATENCY_INTERVAL)
                .ifPresent(this::setLatencyTrackingInterval);

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
                .ifPresent(this::setTaskCancellationInterval);
        configuration
                .getOptional(TaskManagerOptions.TASK_CANCELLATION_TIMEOUT)
                .ifPresent(this::setTaskCancellationTimeout);
        configuration
                .getOptional(ExecutionOptions.SNAPSHOT_COMPRESSION)
                .ifPresent(this::setUseSnapshotCompression);
        RestartStrategies.fromConfiguration(configuration).ifPresent(this::setRestartStrategy);
        configuration
                .getOptional(PipelineOptions.KRYO_DEFAULT_SERIALIZERS)
                .map(s -> parseKryoSerializersWithExceptionHandling(classLoader, s))
                .ifPresent(s -> this.defaultKryoSerializerClasses = s);

        configuration
                .getOptional(PipelineOptions.POJO_REGISTERED_CLASSES)
                .map(c -> loadClasses(c, classLoader, "Could not load pojo type to be registered."))
                .ifPresent(c -> this.registeredPojoTypes = c);

        configuration
                .getOptional(PipelineOptions.KRYO_REGISTERED_CLASSES)
                .map(c -> loadClasses(c, classLoader, "Could not load kryo type to be registered."))
                .ifPresent(c -> this.registeredKryoTypes = c);

        configuration
                .getOptional(JobManagerOptions.SCHEDULER)
                .ifPresent(t -> this.configuration.set(JobManagerOptions.SCHEDULER, t));
    }

    /**
     * @return A copy of internal {@link #configuration}. Note it is missing all options that are
     *     stored as plain java fields in {@link ExecutionConfig}, for example {@link
     *     #registeredKryoTypes}.
     */
    @Internal
    public Configuration toConfiguration() {
        return new Configuration(configuration);
    }

    private LinkedHashSet<Class<?>> loadClasses(
            List<String> classNames, ClassLoader classLoader, String errorMessage) {
        return classNames.stream()
                .map(name -> this.<Class<?>>loadClass(name, classLoader, errorMessage))
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    private LinkedHashMap<Class<?>, Class<? extends Serializer<?>>>
            parseKryoSerializersWithExceptionHandling(
                    ClassLoader classLoader, List<String> kryoSerializers) {
        try {
            return parseKryoSerializers(classLoader, kryoSerializers);
        } catch (Exception e) {
            throw new IllegalArgumentException(
                    String.format(
                            "Could not configure kryo serializers from %s. The expected format is:"
                                    + "'class:<fully qualified class name>,serializer:<fully qualified serializer name>;...",
                            kryoSerializers),
                    e);
        }
    }

    private LinkedHashMap<Class<?>, Class<? extends Serializer<?>>> parseKryoSerializers(
            ClassLoader classLoader, List<String> kryoSerializers) {
        return kryoSerializers.stream()
                .map(ConfigurationUtils::parseMap)
                .collect(
                        Collectors.toMap(
                                m ->
                                        loadClass(
                                                m.get("class"),
                                                classLoader,
                                                "Could not load class for kryo serialization"),
                                m ->
                                        loadClass(
                                                m.get("serializer"),
                                                classLoader,
                                                "Could not load serializer's class"),
                                (m1, m2) -> {
                                    throw new IllegalArgumentException(
                                            "Duplicated serializer for class: " + m1);
                                },
                                LinkedHashMap::new));
    }

    @SuppressWarnings("unchecked")
    private <T extends Class> T loadClass(
            String className, ClassLoader classLoader, String errorMessage) {
        try {
            return (T) Class.forName(className, false, classLoader);
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException(errorMessage, e);
        }
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
