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
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.api.config.ExecutionConfigOptions.UidGeneration;
import org.apache.flink.table.planner.delegation.PlannerBase;

import java.util.Optional;

/**
 * Configuration view which is used combine the {@link PlannerBase#getTableConfig()} with the {@link
 * ExecNodeBase#getPersistedConfig()} configuration. The persisted configuration of the {@link
 * ExecNode} which is deserialized from the JSON plan has precedence over the {@link
 * PlannerBase#getTableConfig()}.
 *
 * <p>This class is intended to contain additional context information for {@link ExecNode}
 * translation such as {@link #shouldSetUid()} or helper methods for accessing configuration such as
 * {@link #getStateRetentionTime()}.
 */
@Internal
public final class ExecNodeConfig implements ReadableConfig {

    private final ReadableConfig tableConfig;

    private final ReadableConfig nodeConfig;

    private final boolean isCompiled;

    private ExecNodeConfig(
            ReadableConfig tableConfig, ReadableConfig nodeConfig, boolean isCompiled) {
        this.nodeConfig = nodeConfig;
        this.tableConfig = tableConfig;
        this.isCompiled = isCompiled;
    }

    static ExecNodeConfig of(
            TableConfig tableConfig, ReadableConfig nodeConfig, boolean isCompiled) {
        return new ExecNodeConfig(tableConfig, nodeConfig, isCompiled);
    }

    public static ExecNodeConfig ofNodeConfig(ReadableConfig nodeConfig, boolean isCompiled) {
        return new ExecNodeConfig(new Configuration(), nodeConfig, isCompiled);
    }

    @VisibleForTesting
    public static ExecNodeConfig ofTableConfig(TableConfig tableConfig, boolean isCompiled) {
        return new ExecNodeConfig(tableConfig, TableConfig.getDefault(), isCompiled);
    }

    @Override
    public <T> T get(ConfigOption<T> option) {
        return nodeConfig.getOptional(option).orElseGet(() -> tableConfig.get(option));
    }

    @Override
    public <T> Optional<T> getOptional(ConfigOption<T> option) {
        final Optional<T> tableValue = nodeConfig.getOptional(option);
        if (tableValue.isPresent()) {
            return tableValue;
        }
        return tableConfig.getOptional(option);
    }

    /** @return The duration until state which was not updated will be retained. */
    public long getStateRetentionTime() {
        return get(ExecutionConfigOptions.IDLE_STATE_RETENTION).toMillis();
    }

    /** @return Whether the {@link ExecNode} translation happens as part of a plan compilation. */
    public boolean isCompiled() {
        return isCompiled;
    }

    /** @return Whether transformations should set a UID. */
    public boolean shouldSetUid() {
        final UidGeneration uidGeneration = get(ExecutionConfigOptions.TABLE_EXEC_UID_GENERATION);
        switch (uidGeneration) {
            case PLAN_ONLY:
                return isCompiled
                        && !get(ExecutionConfigOptions.TABLE_EXEC_LEGACY_TRANSFORMATION_UIDS);
            case ALWAYS:
                return true;
            case DISABLED:
                return false;
            default:
                throw new IllegalArgumentException(
                        "Unknown UID generation strategy: " + uidGeneration);
        }
    }
}
