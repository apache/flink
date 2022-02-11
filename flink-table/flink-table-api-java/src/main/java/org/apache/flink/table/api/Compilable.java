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

import org.apache.flink.annotation.Experimental;

/**
 * Represents an artifact that can be compiled to a {@link CompiledPlan}.
 *
 * @see #compilePlan()
 * @see CompiledPlan
 */
@Experimental
public interface Compilable {

    /**
     * Compiles this object into a {@link CompiledPlan} that can be executed as one job.
     *
     * <p>Compiled plans can be persisted and reloaded across Flink versions. They describe static
     * pipelines to ensure backwards compatibility and enable stateful streaming job upgrades. See
     * {@link CompiledPlan} and the website documentation for more information.
     *
     * <p>Note: The compiled plan feature is not supported in batch mode.
     *
     * @throws TableException if any of the statements is invalid or if the plan cannot be
     *     persisted.
     */
    @Experimental
    CompiledPlan compilePlan() throws TableException;
}
