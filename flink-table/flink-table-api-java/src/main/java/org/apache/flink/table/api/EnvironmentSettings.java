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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.delegation.Executor;
import org.apache.flink.table.delegation.Planner;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.sinks.TableSink;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.api.common.RuntimeExecutionMode.BATCH;
import static org.apache.flink.api.common.RuntimeExecutionMode.STREAMING;
import static org.apache.flink.configuration.ExecutionOptions.RUNTIME_MODE;
import static org.apache.flink.table.api.config.TableConfigOptions.TABLE_PLANNER;

/**
 * Defines all parameters that initialize a table environment. Those parameters are used only during
 * instantiation of a {@link TableEnvironment} and cannot be changed afterwards.
 *
 * <p>Example:
 *
 * <pre>{@code
 * EnvironmentSettings.newInstance()
 *   .useBlinkPlanner()
 *   .inStreamingMode()
 *   .withBuiltInCatalogName("default_catalog")
 *   .withBuiltInDatabaseName("default_database")
 *   .build()
 * }</pre>
 */
@PublicEvolving
public class EnvironmentSettings {

    public static final String STREAMING_MODE = "streaming-mode";
    public static final String CLASS_NAME = "class-name";
    public static final String DEFAULT_BUILTIN_CATALOG = "default_catalog";
    public static final String DEFAULT_BUILTIN_DATABASE = "default_database";

    /** Canonical name of the {@link Planner} class to use. */
    private final String plannerClass;

    /** Canonical name of the {@link Executor} class to use. */
    private final String executorClass;

    /**
     * Specifies the name of the initial catalog to be created when instantiating {@link
     * TableEnvironment}.
     */
    private final String builtInCatalogName;

    /**
     * Specifies the name of the default database in the initial catalog to be created when
     * instantiating {@link TableEnvironment}.
     */
    private final String builtInDatabaseName;

    /**
     * Determines if the table environment should work in a batch ({@code false}) or streaming
     * ({@code true}) mode.
     */
    private final boolean isStreamingMode;

    private EnvironmentSettings(
            @Nullable String plannerClass,
            @Nullable String executorClass,
            String builtInCatalogName,
            String builtInDatabaseName,
            boolean isStreamingMode) {
        this.plannerClass = plannerClass;
        this.executorClass = executorClass;
        this.builtInCatalogName = builtInCatalogName;
        this.builtInDatabaseName = builtInDatabaseName;
        this.isStreamingMode = isStreamingMode;
    }

    /**
     * Creates a builder for creating an instance of {@link EnvironmentSettings}.
     *
     * <p>By default, it does not specify a required planner and will use the one that is available
     * on the classpath via discovery.
     */
    public static Builder newInstance() {
        return new Builder();
    }

    /** Creates an instance of {@link EnvironmentSettings} from {@link Configuration}. */
    public static EnvironmentSettings fromConfiguration(Configuration configuration) {
        Builder builder = new Builder();
        if (configuration.get(RUNTIME_MODE).equals(STREAMING)) {
            builder.inStreamingMode();
        } else {
            builder.inBatchMode();
        }

        switch (configuration.get(RUNTIME_MODE)) {
            case STREAMING:
                builder.inStreamingMode();
                break;
            case BATCH:
                builder.inBatchMode();
                break;
            case AUTOMATIC:
                throw new UnsupportedOperationException(
                        "TableEnvironment currently doesn't support `AUTOMATIC` mode.");
            default:
                throw new IllegalArgumentException(
                        String.format(
                                "Unrecognized value '%s' for option '%s'.",
                                configuration.get(RUNTIME_MODE), RUNTIME_MODE.key()));
        }

        switch (configuration.get(TABLE_PLANNER)) {
            case BLINK:
                builder.useBlinkPlanner();
                break;
            case OLD:
                builder.useOldPlanner();
                break;
            default:
                throw new IllegalArgumentException(
                        String.format(
                                "Unrecognized value '%s' for option '%s'.",
                                configuration.get(TABLE_PLANNER), TABLE_PLANNER.key()));
        }
        return builder.build();
    }

    /** Convert the environment setting to the {@link Configuration}. */
    public Configuration toConfiguration() {
        Configuration configuration = new Configuration();
        configuration.set(RUNTIME_MODE, isStreamingMode() ? STREAMING : BATCH);
        configuration.set(TABLE_PLANNER, isBlinkPlanner() ? PlannerType.BLINK : PlannerType.OLD);
        return configuration;
    }

    /**
     * Gets the specified name of the initial catalog to be created when instantiating a {@link
     * TableEnvironment}.
     */
    public String getBuiltInCatalogName() {
        return builtInCatalogName;
    }

    /**
     * Gets the specified name of the default database in the initial catalog to be created when
     * instantiating a {@link TableEnvironment}.
     */
    public String getBuiltInDatabaseName() {
        return builtInDatabaseName;
    }

    /** Tells if the {@link TableEnvironment} should work in a batch or streaming mode. */
    public boolean isStreamingMode() {
        return isStreamingMode;
    }

    /** Tells if the {@link TableEnvironment} should work in the blink planner or old planner. */
    public boolean isBlinkPlanner() {
        return (this.plannerClass == null && this.executorClass == null)
                || (Builder.BLINK_PLANNER_FACTORY.equals(this.plannerClass)
                        && Builder.BLINK_EXECUTOR_FACTORY.equals(this.executorClass));
    }

    @Internal
    public Map<String, String> toPlannerProperties() {
        Map<String, String> properties = new HashMap<>(toCommonProperties());
        if (plannerClass != null) {
            properties.put(CLASS_NAME, plannerClass);
        }
        return properties;
    }

    @Internal
    public Map<String, String> toExecutorProperties() {
        Map<String, String> properties = new HashMap<>(toCommonProperties());
        if (executorClass != null) {
            properties.put(CLASS_NAME, executorClass);
        }
        return properties;
    }

    private Map<String, String> toCommonProperties() {
        Map<String, String> properties = new HashMap<>();
        properties.put(STREAMING_MODE, Boolean.toString(isStreamingMode));
        return properties;
    }

    /** A builder for {@link EnvironmentSettings}. */
    public static class Builder {
        private static final String OLD_PLANNER_FACTORY =
                "org.apache.flink.table.planner.StreamPlannerFactory";
        private static final String OLD_EXECUTOR_FACTORY =
                "org.apache.flink.table.executor.StreamExecutorFactory";
        private static final String BLINK_PLANNER_FACTORY =
                "org.apache.flink.table.planner.delegation.BlinkPlannerFactory";
        private static final String BLINK_EXECUTOR_FACTORY =
                "org.apache.flink.table.planner.delegation.BlinkExecutorFactory";

        private String plannerClass = BLINK_PLANNER_FACTORY;
        private String executorClass = BLINK_EXECUTOR_FACTORY;
        private String builtInCatalogName = DEFAULT_BUILTIN_CATALOG;
        private String builtInDatabaseName = DEFAULT_BUILTIN_DATABASE;
        private boolean isStreamingMode = true;

        /**
         * Sets the old Flink planner as the required module. By default, {@link #useBlinkPlanner()}
         * is enabled.
         *
         * @deprecated The old planner will be dropped in Flink 1.14. Please update to the new
         *     planner (i.e. Blink planner).
         */
        @Deprecated
        public Builder useOldPlanner() {
            this.plannerClass = OLD_PLANNER_FACTORY;
            this.executorClass = OLD_EXECUTOR_FACTORY;
            return this;
        }

        /**
         * Sets the Blink planner as the required module.
         *
         * <p>This is the default behavior.
         */
        public Builder useBlinkPlanner() {
            this.plannerClass = BLINK_PLANNER_FACTORY;
            this.executorClass = BLINK_EXECUTOR_FACTORY;
            return this;
        }

        /**
         * Does not set a planner requirement explicitly.
         *
         * <p>A planner will be discovered automatically, if there is only one planner available.
         *
         * <p>By default, {@link #useBlinkPlanner()} is enabled.
         */
        public Builder useAnyPlanner() {
            this.plannerClass = null;
            this.executorClass = null;
            return this;
        }

        /** Sets that the components should work in a batch mode. Streaming mode by default. */
        public Builder inBatchMode() {
            this.isStreamingMode = false;
            return this;
        }

        /** Sets that the components should work in a streaming mode. Enabled by default. */
        public Builder inStreamingMode() {
            this.isStreamingMode = true;
            return this;
        }

        /**
         * Specifies the name of the initial catalog to be created when instantiating a {@link
         * TableEnvironment}. This catalog will be used to store all non-serializable objects such
         * as tables and functions registered via e.g. {@link
         * TableEnvironment#registerTableSink(String, TableSink)} or {@link
         * TableEnvironment#registerFunction(String, ScalarFunction)}. It will also be the initial
         * value for the current catalog which can be altered via {@link
         * TableEnvironment#useCatalog(String)}.
         *
         * <p>Default: "default_catalog".
         */
        public Builder withBuiltInCatalogName(String builtInCatalogName) {
            this.builtInCatalogName = builtInCatalogName;
            return this;
        }

        /**
         * Specifies the name of the default database in the initial catalog to be created when
         * instantiating a {@link TableEnvironment}. The database will be used to store all
         * non-serializable objects such as tables and functions registered via e.g. {@link
         * TableEnvironment#registerTableSink(String, TableSink)} or {@link
         * TableEnvironment#registerFunction(String, ScalarFunction)}. It will also be the initial
         * value for the current database which can be altered via {@link
         * TableEnvironment#useDatabase(String)}.
         *
         * <p>Default: "default_database".
         */
        public Builder withBuiltInDatabaseName(String builtInDatabaseName) {
            this.builtInDatabaseName = builtInDatabaseName;
            return this;
        }

        /** Returns an immutable instance of {@link EnvironmentSettings}. */
        public EnvironmentSettings build() {
            return new EnvironmentSettings(
                    plannerClass,
                    executorClass,
                    builtInCatalogName,
                    builtInDatabaseName,
                    isStreamingMode);
        }
    }
}
