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

package org.apache.flink.table.planner.plan.fusion;

import org.apache.flink.table.planner.codegen.CodeGeneratorContext;
import org.apache.flink.table.planner.codegen.GeneratedExpression;
import org.apache.flink.table.types.logical.RowType;

import java.util.List;

/**
 * A OpFusionContext contains information about the context in which {@link OpFusionCodegenSpec}
 * needed to do operator fusion codegen.
 */
public interface OpFusionContext {

    /** Return the output type of current {@link OpFusionCodegenSpecGenerator}. */
    RowType getOutputType();

    /**
     * Return the managed memory fraction of this {@link OpFusionCodegenSpecGenerator} needed during
     * all fusion operators.
     */
    double getManagedMemoryFraction();

    /** Return the input {@link OpFusionContext} of this {@link OpFusionCodegenSpecGenerator}. */
    List<OpFusionContext> getInputFusionContexts();

    /**
     * Generate Java source code to process the rows from operator corresponding input, delegate to
     * {@link OpFusionCodegenSpecGenerator#processProduce(CodeGeneratorContext)} method.
     */
    void processProduce(CodeGeneratorContext codegenCtx);

    /**
     * Generate Java source code to do clean work for operator corresponding input, delegate to
     * {@link OpFusionCodegenSpecGenerator#endInputProduce(CodeGeneratorContext)} method.
     */
    void endInputProduce(CodeGeneratorContext codegenCtx);

    default String processConsume(List<GeneratedExpression> outputVars) {
        return processConsume(outputVars, null);
    }

    /**
     * Consume the generated columns or row from current {@link OpFusionCodegenSpec}, delegate to
     * {@link OpFusionCodegenSpecGenerator#processConsume(List, String)} ()} method.
     *
     * <p>Note that `outputVars` and `row` can't both be null.
     */
    String processConsume(List<GeneratedExpression> outputVars, String row);

    /**
     * Generate Java source code to do clean work for {@link OpFusionCodegenSpec} corresponding
     * input, delegate to {@link OpFusionCodegenSpecGenerator#endInputConsume()} method.
     */
    String endInputConsume();
}
