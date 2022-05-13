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

package org.apache.flink.table.planner.delegation;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.delegation.Planner;
import org.apache.flink.table.delegation.PlannerFactory;

import java.util.Collections;
import java.util.Set;

import static org.apache.flink.configuration.ExecutionOptions.RUNTIME_MODE;

/** Factory for the default {@link Planner}. */
@Internal
public final class DefaultPlannerFactory implements PlannerFactory {

    @Override
    public String factoryIdentifier() {
        return PlannerFactory.DEFAULT_IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return Collections.emptySet();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return Collections.emptySet();
    }

    @Override
    public Planner create(Context context) {
        final RuntimeExecutionMode runtimeExecutionMode =
                context.getTableConfig().get(ExecutionOptions.RUNTIME_MODE);
        switch (runtimeExecutionMode) {
            case STREAMING:
                return new StreamPlanner(
                        context.getExecutor(),
                        context.getTableConfig(),
                        context.getModuleManager(),
                        context.getFunctionCatalog(),
                        context.getCatalogManager());
            case BATCH:
                return new BatchPlanner(
                        context.getExecutor(),
                        context.getTableConfig(),
                        context.getModuleManager(),
                        context.getFunctionCatalog(),
                        context.getCatalogManager());
            default:
                throw new TableException(
                        String.format(
                                "Unsupported mode '%s' for '%s'. Only an explicit BATCH or "
                                        + "STREAMING mode is supported in Table API.",
                                runtimeExecutionMode, RUNTIME_MODE.key()));
        }
    }
}
