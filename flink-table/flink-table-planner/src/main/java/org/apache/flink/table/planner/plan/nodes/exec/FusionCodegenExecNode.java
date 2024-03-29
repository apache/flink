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

import org.apache.flink.table.delegation.Planner;
import org.apache.flink.table.planner.codegen.CodeGeneratorContext;
import org.apache.flink.table.planner.plan.fusion.OpFusionCodegenSpecGenerator;

/** A {@link ExecNode} which support operator fusion codegen. */
public interface FusionCodegenExecNode {

    /** Whether this ExecNode supports OFCG or not. */
    boolean supportFusionCodegen();

    /**
     * Translates this node into a {@link OpFusionCodegenSpecGenerator}.
     *
     * <p>NOTE: This method should return same spec generator result if called multiple times.
     *
     * @param planner The {@link Planner} of the translated graph.
     * @param parentCtx Parent CodeGeneratorContext.
     */
    OpFusionCodegenSpecGenerator translateToFusionCodegenSpec(
            Planner planner, CodeGeneratorContext parentCtx);
}
