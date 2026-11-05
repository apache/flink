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

package org.apache.flink.table.planner.delegation.materializedtable;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.delegation.materializedtable.MaterializedTableExecutor;
import org.apache.flink.table.delegation.materializedtable.MaterializedTableExecutorFactory;
import org.apache.flink.table.delegation.materializedtable.MaterializedTableJobSubmitter;
import org.apache.flink.table.operations.ExecutableOperation;

import java.util.Set;

/** Factory that creates the default {@link DefaultMaterializedTableExecutor}. */
@Internal
public class DefaultMaterializedTableExecutorFactory implements MaterializedTableExecutorFactory {

    @Override
    public MaterializedTableExecutor createExecutor(
            ExecutableOperation.Context operationCtx, MaterializedTableJobSubmitter jobSubmitter) {
        return new DefaultMaterializedTableExecutor(operationCtx, jobSubmitter);
    }

    @Override
    public String factoryIdentifier() {
        return MaterializedTableExecutorFactory.DEFAULT_IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return Set.of();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return Set.of();
    }
}
