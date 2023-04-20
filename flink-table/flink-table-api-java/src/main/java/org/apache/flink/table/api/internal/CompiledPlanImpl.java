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

package org.apache.flink.table.api.internal;

import org.apache.flink.FlinkVersion;
import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.CompiledPlan;
import org.apache.flink.table.api.ExplainDetail;
import org.apache.flink.table.api.ExplainFormat;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.table.delegation.InternalPlan;

import java.io.File;

/** Implementation of {@link CompiledPlan}, wrapping {@link InternalPlan}. */
@Internal
class CompiledPlanImpl implements CompiledPlan {

    private final TableEnvironmentInternal tableEnvironment;
    private final InternalPlan internalPlan;

    CompiledPlanImpl(TableEnvironmentInternal tableEnvironment, InternalPlan internalPlan) {
        this.tableEnvironment = tableEnvironment;
        this.internalPlan = internalPlan;
    }

    InternalPlan unwrap() {
        return internalPlan;
    }

    @Override
    public String asJsonString() {
        return internalPlan.asJsonString();
    }

    @Override
    public void writeToFile(File file, boolean ignoreIfExists) {
        internalPlan.writeToFile(
                file,
                ignoreIfExists,
                !tableEnvironment.getConfig().get(TableConfigOptions.PLAN_FORCE_RECOMPILE));
    }

    @Override
    public FlinkVersion getFlinkVersion() {
        return internalPlan.getFlinkVersion();
    }

    @Override
    public TableResult execute() {
        return tableEnvironment.executePlan(internalPlan);
    }

    @Override
    public String explain(ExplainFormat format, ExplainDetail... extraDetails) {
        return tableEnvironment.explainPlan(internalPlan, extraDetails);
    }

    @Override
    public String toString() {
        return explain();
    }
}
