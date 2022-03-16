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

package org.apache.flink.table.planner.plan.nodes.exec;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.planner.codegen.CodeGeneratorContext;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.delegation.PlannerConfig;

import java.time.ZoneId;
import java.util.Optional;

/**
 * Configuration view which is used combine the {@link PlannerConfig} with the {@link
 * ExecNodeBase#getNodeConfig()} configuration. The persisted configuration of the {@link ExecNode}
 * which is deserialized from the JSON plan has precedence over the {@link PlannerConfig}.
 */
@Internal
public final class ExecNodeConfig implements ReadableConfig {

    private final ReadableConfig plannerConfig;

    // See https://issues.apache.org/jira/browse/FLINK-26190
    // Used only for the deprecated getMaxIdleStateRetentionTime to also satisfy tests which
    // manipulate maxIdleStateRetentionTime, like OverAggregateHarnessTest.
    private final TableConfig originalTableConfig;
    // See https://issues.apache.org/jira/browse/FLINK-26190
    private final TableConfig tableConfig;

    private final ReadableConfig nodeConfig;

    ExecNodeConfig(
            ReadableConfig plannerConfig, TableConfig tableConfig, ReadableConfig nodeConfig) {
        this.plannerConfig = plannerConfig;
        this.nodeConfig = nodeConfig;
        this.originalTableConfig = tableConfig;
        this.tableConfig = TableConfig.getDefault();
        this.tableConfig.addConfiguration(tableConfig.getConfiguration());
        this.tableConfig.addConfiguration((Configuration) nodeConfig);
    }

    /**
     * Return the merged {@link TableConfig} from {@link PlannerBase#getTableConfig()} and {@link
     * ExecNodeBase#getNodeConfig()}.
     *
     * @return the {@link TableConfig}.
     * @deprecated This method is used only for {@link CodeGeneratorContext} and related methods,
     *     which end up passing the {@link TableConfig} to the {@link CodeGeneratorContext}. It
     *     should be removed once {@link CodeGeneratorContext#nullCheck()} is removed, since for all
     *     other usages it's possible to use the {@link ReadableConfig}.
     */
    // See https://issues.apache.org/jira/browse/FLINK-26190
    @Deprecated
    public TableConfig getTableConfig() {
        return tableConfig;
    }

    @Override
    public <T> T get(ConfigOption<T> option) {
        return nodeConfig.getOptional(option).orElseGet(() -> plannerConfig.get(option));
    }

    @Override
    public <T> Optional<T> getOptional(ConfigOption<T> option) {
        final Optional<T> tableValue = nodeConfig.getOptional(option);
        if (tableValue.isPresent()) {
            return tableValue;
        }
        return plannerConfig.getOptional(option);
    }

    /** @return The duration until state which was not updated will be retained. */
    public long getStateRetentionTime() {
        return get(ExecutionConfigOptions.IDLE_STATE_RETENTION).toMillis();
    }

    // See https://issues.apache.org/jira/browse/FLINK-26190
    /**
     * Using {@link #originalTableConfig} to satisify tests like {@code OverAggregateHarnessTest},
     * which use {@code HarnessTestBase#TestTableConfig} to individually manipulate the
     * maxIdleStateRetentionTime. See {@link TableConfig#getMaxIdleStateRetentionTime()}.
     */
    @Deprecated
    public long getMaxIdleStateRetentionTime() {
        return originalTableConfig.getMaxIdleStateRetentionTime();
    }

    // See https://issues.apache.org/jira/browse/FLINK-26190
    /** See {@link TableConfig#getLocalTimeZone()}. */
    public ZoneId getLocalTimeZone() {
        return tableConfig.getLocalTimeZone();
    }
}
