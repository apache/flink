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

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.table.api.CompiledPlan;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.planner.plan.ExecNodeGraphInternalPlan;

import java.util.List;

/** Utilities to test {@link CompiledPlan}. */
public class CompiledPlanUtils {

    private CompiledPlanUtils() {}

    /** Unwrap {@link ExecNodeGraphInternalPlan} from {@link CompiledPlan}. */
    public static ExecNodeGraphInternalPlan unwrap(CompiledPlan compiledPlan) {
        return (ExecNodeGraphInternalPlan) ((CompiledPlanImpl) compiledPlan).unwrap();
    }

    /** Converts the given plan into {@link Transformation}s. */
    public static List<Transformation<?>> toTransformations(
            TableEnvironment env, CompiledPlan compiledPlan) {
        final ExecNodeGraphInternalPlan internalPlan = unwrap(compiledPlan);
        return ((TableEnvironmentImpl) env).getPlanner().translatePlan(internalPlan);
    }
}
