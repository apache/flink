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

package org.apache.flink.table.api;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.configuration.WritableConfig;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.table.delegation.Executor;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.util.Preconditions;

import java.time.Duration;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.table.api.internal.TableConfigValidation.validateTimeZone;

/**
 * Configuration for the current {@link TableEnvironment} session to adjust Table & SQL API
 * programs.
 *
 * <p>This class is a pure API class that abstracts configuration from various sources. Currently,
 * configuration can be set in any of the following layers (in the given order):
 *
 * <ol>
 *   <li>{@code flink-conf.yaml},
 *   <li>CLI parameters,
 *   <li>{@code StreamExecutionEnvironment} when bridging to DataStream API,
 *   <li>{@link EnvironmentSettings.Builder#withConfiguration(Configuration)} / {@link
 *       TableEnvironment#create(Configuration)},
 *   <li>and {@link TableConfig#set(ConfigOption, Object)} / {@link TableConfig#set(String,
 *       String)}.
 * </ol>
 *
 * <p>The latter two represent the application-specific part of the configuration. They initialize
 * and directly modify {@link TableConfig#getConfiguration()}. Other layers represent the
 * configuration of the execution context and are immutable.
 *
 * <p>The getters {@link #get(ConfigOption)} and {@link #getOptional(ConfigOption)} give read-only
 * access to the full configuration. However, application-specific configuration has precedence.
 * Configuration of outer layers is used for defaults and fallbacks. The setters {@link
 * #set(ConfigOption, Object)} and {@link #set(String, String)} will only affect
 * application-specific configuration.
 *
 * <p>For common or important configuration options, this class provides getters and setters methods
 * with detailed inline documentation.
 *
 * <p>For more advanced configuration, users can directly access the underlying key-value map via
 * {@link #getConfiguration()}. Users can configure also underlying execution parameters via this
 * object.
 *
 * <p>For example:
 *
 * <pre>{@code
 * tEnv.getConfig().addConfiguration(
 *          new Configuration()
 *              .set(CoreOptions.DEFAULT_PARALLELISM, 128)
 *              .set(PipelineOptions.AUTO_WATERMARK_INTERVAL, Duration.ofMillis(800))
 *              .set(ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofSeconds(30))
 *      );
 * }</pre>
 *
 * <p>Note: Because options are read at different point in time when performing operations, it is
 * recommended to set configuration options early after instantiating a table environment.
 *
 * @see TableConfigOptions
 * @see ExecutionConfigOptions
 * @see OptimizerConfigOptions
 */
@PublicEvolving
public final class TableConfig implements WritableConfig, ReadableConfig {

    /** Please use {@link TableConfig#getDefault()} instead. */
    @Deprecated
    public TableConfig() {}

    // Note to implementers:
    // TableConfig is a ReadableConfig which is built once the TableEnvironment is created and
    // contains both the configuration defined in the execution context (flink-conf.yaml + CLI
    // params), stored in rootConfiguration, but also any extra configuration defined by the user in
    // the application, which has precedence over the execution configuration.
    //
    // This way, any consumer of TableConfig can get the complete view of the configuration
    // (environment + user-defined/application-specific) by calling the get() and getOptional()
    // methods.
    //
    // The set() methods only impact the application-specific configuration.

    /** Defines the configuration of Planner for Table API and SQL queries. */
    private PlannerConfig plannerConfig = PlannerConfig.EMPTY_CONFIG;

    /**
     * A configuration object to hold all configuration that has been set specifically in the Table
     * API. It does not contain configuration from outer layers.
     */
    private final Configuration configuration = new Configuration();

    /** Configuration adopted from the outer layer (i.e. the {@link Executor}). */
    private ReadableConfig rootConfiguration = new Configuration();

    /**
     * Sets an application-specific value for the given {@link ConfigOption}.
     *
     * <p>This method should be preferred over {@link #set(String, String)} as it is type-safe,
     * avoids unnecessary parsing of the value, and provides inline documentation.
     *
     * <p>Note: Scala users might need to convert the value into a boxed type. E.g. by using {@code
     * Int.box(1)} or {@code Boolean.box(false)}.
     *
     * @see TableConfigOptions
     * @see ExecutionConfigOptions
     * @see OptimizerConfigOptions
     */
    @Override
    public <T> TableConfig set(ConfigOption<T> option, T value) {
        configuration.set(option, value);
        return this;
    }

    /**
     * Sets an application-specific string-based value for the given string-based key.
     *
     * <p>The value will be parsed by the framework on access.
     *
     * <p>This method exists for convenience when configuring a session with string-based
     * properties. Use {@link #set(ConfigOption, Object)} for more type-safety and inline
     * documentation.
     *
     * @see TableConfigOptions
     * @see ExecutionConfigOptions
     * @see OptimizerConfigOptions
     */
    public TableConfig set(String key, String value) {
        configuration.setString(key, value);
        return this;
    }

    /**
     * {@inheritDoc}
     *
     * <p>This method gives read-only access to the full configuration. However,
     * application-specific configuration has precedence. Configuration of outer layers is used for
     * defaults and fallbacks. See the docs of {@link TableConfig} for more information.
     *
     * @param option metadata of the option to read
     * @param <T> type of the value to read
     * @return read value or {@link ConfigOption#defaultValue()} if not found
     */
    @Override
    public <T> T get(ConfigOption<T> option) {
        return configuration.getOptional(option).orElseGet(() -> rootConfiguration.get(option));
    }

    /**
     * {@inheritDoc}
     *
     * <p>This method gives read-only access to the full configuration. However,
     * application-specific configuration has precedence. Configuration of outer layers is used for
     * defaults and fallbacks. See the docs of {@link TableConfig} for more information.
     *
     * @param option metadata of the option to read
     * @param <T> type of the value to read
     * @return read value or {@link Optional#empty()} if not found
     */
    @Override
    public <T> Optional<T> getOptional(ConfigOption<T> option) {
        final Optional<T> tableValue = configuration.getOptional(option);
        if (tableValue.isPresent()) {
            return tableValue;
        }
        return rootConfiguration.getOptional(option);
    }

    /**
     * Gives direct access to the underlying application-specific key-value map for advanced
     * configuration.
     */
    public Configuration getConfiguration() {
        return configuration;
    }

    /**
     * Gives direct access to the underlying environment-specific key-value map for advanced
     * configuration.
     */
    @Internal
    public ReadableConfig getRootConfiguration() {
        return rootConfiguration;
    }

    /**
     * Adds the given key-value configuration to the underlying application-specific configuration.
     * It overwrites existing keys.
     *
     * @param configuration key-value configuration to be added
     */
    public void addConfiguration(Configuration configuration) {
        Preconditions.checkNotNull(configuration);
        this.configuration.addAll(configuration);
    }

    /** Returns the current SQL dialect. */
    public SqlDialect getSqlDialect() {
        return SqlDialect.valueOf(get(TableConfigOptions.TABLE_SQL_DIALECT).toUpperCase());
    }

    /** Sets the current SQL dialect to parse a SQL query. Flink's SQL behavior by default. */
    public void setSqlDialect(SqlDialect sqlDialect) {
        set(TableConfigOptions.TABLE_SQL_DIALECT, sqlDialect.name().toLowerCase());
    }

    /**
     * Returns the current session time zone id. It is used when converting to/from {@code TIMESTAMP
     * WITH LOCAL TIME ZONE}. See {@link #setLocalTimeZone(ZoneId)} for more details.
     *
     * @see org.apache.flink.table.types.logical.LocalZonedTimestampType
     */
    public ZoneId getLocalTimeZone() {
        final String zone = configuration.getString(TableConfigOptions.LOCAL_TIME_ZONE);
        if (TableConfigOptions.LOCAL_TIME_ZONE.defaultValue().equals(zone)) {
            return ZoneId.systemDefault();
        }
        validateTimeZone(zone);
        return ZoneId.of(zone);
    }

    /**
     * Sets the current session time zone id. It is used when converting to/from {@link
     * DataTypes#TIMESTAMP_WITH_LOCAL_TIME_ZONE()}. Internally, timestamps with local time zone are
     * always represented in the UTC time zone. However, when converting to data types that don't
     * include a time zone (e.g. TIMESTAMP, TIME, or simply STRING), the session time zone is used
     * during conversion.
     *
     * <p>Example:
     *
     * <pre>{@code
     * TableConfig config = tEnv.getConfig();
     * config.setLocalTimeZone(ZoneOffset.ofHours(2));
     * tEnv.executeSql("CREATE TABLE testTable (id BIGINT, tmstmp TIMESTAMP WITH LOCAL TIME ZONE)");
     * tEnv.executeSql("INSERT INTO testTable VALUES ((1, '2000-01-01 2:00:00'), (2, TIMESTAMP '2000-01-01 2:00:00'))");
     * tEnv.executeSql("SELECT * FROM testTable"); // query with local time zone set to UTC+2
     * }</pre>
     *
     * <p>should produce:
     *
     * <pre>
     * =============================
     *    id   |       tmstmp
     * =============================
     *    1    | 2000-01-01 2:00:00'
     *    2    | 2000-01-01 2:00:00'
     * </pre>
     *
     * <p>If we change the local time zone and query the same table:
     *
     * <pre>{@code
     * config.setLocalTimeZone(ZoneOffset.ofHours(0));
     * tEnv.executeSql("SELECT * FROM testTable"); // query with local time zone set to UTC+0
     * }</pre>
     *
     * <p>we should get:
     *
     * <pre>
     * =============================
     *    id   |       tmstmp
     * =============================
     *    1    | 2000-01-01 0:00:00'
     *    2    | 2000-01-01 0:00:00'
     * </pre>
     *
     * @see org.apache.flink.table.types.logical.LocalZonedTimestampType
     */
    public void setLocalTimeZone(ZoneId zoneId) {
        final String zone;
        if (zoneId instanceof ZoneOffset) {
            // Give ZoneOffset a timezone for backwards compatibility reasons.
            // In general, advertising either TZDB ID, GMT+xx:xx, or UTC is the best we can do.
            zone = ZoneId.ofOffset("GMT", (ZoneOffset) zoneId).toString();
        } else {
            zone = zoneId.toString();
        }
        validateTimeZone(zone);

        configuration.setString(TableConfigOptions.LOCAL_TIME_ZONE, zone);
    }

    /** Returns the current configuration of Planner for Table API and SQL queries. */
    public PlannerConfig getPlannerConfig() {
        return plannerConfig;
    }

    /**
     * Sets the configuration of Planner for Table API and SQL queries. Changing the configuration
     * has no effect after the first query has been defined.
     */
    public void setPlannerConfig(PlannerConfig plannerConfig) {
        this.plannerConfig = Preconditions.checkNotNull(plannerConfig);
    }

    /**
     * Returns the current threshold where generated code will be split into sub-function calls.
     * Java has a maximum method length of 64 KB. This setting allows for finer granularity if
     * necessary.
     *
     * <p>Default value is 4000 instead of 64KB as by default JIT refuses to work on methods with
     * more than 8K byte code.
     */
    public Integer getMaxGeneratedCodeLength() {
        return this.configuration.getInteger(TableConfigOptions.MAX_LENGTH_GENERATED_CODE);
    }

    /**
     * Sets current threshold where generated code will be split into sub-function calls. Java has a
     * maximum method length of 64 KB. This setting allows for finer granularity if necessary.
     *
     * <p>Default value is 4000 instead of 64KB as by default JIT refuses to work on methods with
     * more than 8K byte code.
     */
    public void setMaxGeneratedCodeLength(Integer maxGeneratedCodeLength) {
        this.configuration.setInteger(
                TableConfigOptions.MAX_LENGTH_GENERATED_CODE, maxGeneratedCodeLength);
    }

    /**
     * Specifies a minimum and a maximum time interval for how long idle state, i.e., state which
     * was not updated, will be retained. State will never be cleared until it was idle for less
     * than the minimum time and will never be kept if it was idle for more than the maximum time.
     *
     * <p>When new data arrives for previously cleaned-up state, the new data will be handled as if
     * it was the first data. This can result in previous results being overwritten.
     *
     * <p>Set to 0 (zero) to never clean-up the state.
     *
     * <p>NOTE: Cleaning up state requires additional bookkeeping which becomes less expensive for
     * larger differences of minTime and maxTime. The difference between minTime and maxTime must be
     * at least 5 minutes.
     *
     * <p>NOTE: Currently maxTime will be ignored and it will automatically derived from minTime as
     * 1.5 x minTime.
     *
     * @param minTime The minimum time interval for which idle state is retained. Set to 0 (zero) to
     *     never clean-up the state.
     * @param maxTime The maximum time interval for which idle state is retained. Must be at least 5
     *     minutes greater than minTime. Set to 0 (zero) to never clean-up the state.
     * @deprecated use {@link #setIdleStateRetention(Duration)} instead.
     */
    @Deprecated
    public void setIdleStateRetentionTime(Time minTime, Time maxTime) {
        if (maxTime.toMilliseconds() - minTime.toMilliseconds() < 300000
                && !(maxTime.toMilliseconds() == 0 && minTime.toMilliseconds() == 0)) {
            throw new IllegalArgumentException(
                    "Difference between minTime: "
                            + minTime
                            + " and maxTime: "
                            + maxTime
                            + " should be at least 5 minutes.");
        }
        setIdleStateRetention(Duration.ofMillis(minTime.toMilliseconds()));
    }

    /**
     * Specifies a retention time interval for how long idle state, i.e., state which was not
     * updated, will be retained. State will never be cleared until it was idle for less than the
     * retention time and will be cleared on a best effort basis after the retention time.
     *
     * <p>When new data arrives for previously cleaned-up state, the new data will be handled as if
     * it was the first data. This can result in previous results being overwritten.
     *
     * <p>Set to 0 (zero) to never clean-up the state.
     *
     * @param duration The retention time interval for which idle state is retained. Set to 0 (zero)
     *     to never clean-up the state.
     * @see org.apache.flink.api.common.state.StateTtlConfig
     */
    public void setIdleStateRetention(Duration duration) {
        configuration.set(ExecutionConfigOptions.IDLE_STATE_RETENTION, duration);
    }

    /**
     * NOTE: Currently the concept of min/max idle state retention has been deprecated and only idle
     * state retention time is supported. The min idle state retention is regarded as idle state
     * retention and the max idle state retention is derived from idle state retention as 1.5 x idle
     * state retention.
     *
     * @return The minimum time until state which was not updated will be retained.
     * @deprecated use{@link getIdleStateRetention} instead.
     */
    @Deprecated
    public long getMinIdleStateRetentionTime() {
        return configuration.get(ExecutionConfigOptions.IDLE_STATE_RETENTION).toMillis();
    }

    /**
     * NOTE: Currently the concept of min/max idle state retention has been deprecated and only idle
     * state retention time is supported. The min idle state retention is regarded as idle state
     * retention and the max idle state retention is derived from idle state retention as 1.5 x idle
     * state retention.
     *
     * @return The maximum time until state which was not updated will be retained.
     * @deprecated use{@link getIdleStateRetention} instead.
     */
    @Deprecated
    public long getMaxIdleStateRetentionTime() {
        return getMinIdleStateRetentionTime() * 3 / 2;
    }

    /** @return The duration until state which was not updated will be retained. */
    public Duration getIdleStateRetention() {
        return configuration.get(ExecutionConfigOptions.IDLE_STATE_RETENTION);
    }

    /**
     * Sets a custom user parameter that can be accessed via {@link
     * FunctionContext#getJobParameter(String, String)}.
     *
     * <p>This will add an entry to the current value of {@link
     * PipelineOptions#GLOBAL_JOB_PARAMETERS}.
     *
     * <p>It is also possible to set multiple parameters at once, which will override any previously
     * set parameters:
     *
     * <pre>{@code
     * Map<String, String> params = ...
     * TableConfig config = tEnv.getConfig();
     * config.set(PipelineOptions.GLOBAL_JOB_PARAMETERS, params);
     * }</pre>
     */
    public void addJobParameter(String key, String value) {
        final Map<String, String> params =
                getOptional(PipelineOptions.GLOBAL_JOB_PARAMETERS)
                        .map(HashMap::new)
                        .orElseGet(HashMap::new);
        params.put(key, value);
        set(PipelineOptions.GLOBAL_JOB_PARAMETERS, params);
    }

    /**
     * Sets the given configuration as {@link #rootConfiguration}, which contains any configuration
     * set in the execution context. See the docs of {@link TableConfig} for more information.
     *
     * @param rootConfiguration root configuration to be set
     */
    @Internal
    public void setRootConfiguration(ReadableConfig rootConfiguration) {
        this.rootConfiguration = rootConfiguration;
    }

    public static TableConfig getDefault() {
        return new TableConfig();
    }
}
