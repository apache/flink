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

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.planner.codegen.CodeGeneratorContext;
import org.apache.flink.table.planner.codegen.GeneratedExpression;
import org.apache.flink.table.types.logical.RowType;

import java.util.List;
import java.util.stream.Collectors;

/**
 * {@link OpFusionCodegenSpecGenerator} is used to operator fusion codegen that generate the fusion
 * code, it has multiple inputs and outputs, then form a DAG. Every OpFusionCodegenSpecGenerator
 * holds an {@link OpFusionCodegenSpec} that used to generate the operator process row code. In
 * addition, it also provides some meta information that codegen needed.
 */
@Internal
public abstract class OpFusionCodegenSpecGenerator {

    private final RowType outputType;
    protected final OpFusionCodegenSpec opFusionCodegenSpec;
    private final OpFusionContext opFusionContext;
    private double managedMemoryFraction = 0;

    public OpFusionCodegenSpecGenerator(
            RowType outputType, OpFusionCodegenSpec opFusionCodegenSpec) {
        this.outputType = outputType;
        this.opFusionCodegenSpec = opFusionCodegenSpec;
        this.opFusionContext = new OpFusionContextImpl(this);
    }

    /**
     * Initializes the operator spec generator needed information. This method must be called before
     * produce and consume related method.
     */
    public void setup(Context context) {
        this.managedMemoryFraction = context.getManagedMemoryFraction();
        this.opFusionCodegenSpec.setup(opFusionContext);
    }

    public RowType getOutputType() {
        return outputType;
    }

    public OpFusionCodegenSpec getOpFusionCodegenSpec() {
        return opFusionCodegenSpec;
    }

    public OpFusionContext getOpFusionContext() {
        return opFusionContext;
    }

    public abstract long getManagedMemory();

    public abstract List<OpFusionCodegenSpecGenerator> getInputs();

    /**
     * Add the specific {@link OpFusionCodegenSpecGenerator} as the output of current operator spec
     * generator, one {@link OpFusionCodegenSpecGenerator} may have multiple outputs that form a
     * DAG.
     *
     * @param inputIdOfOutput This is numbered starting from 1, and `1` indicates the first input of
     *     output {@link OpFusionCodegenSpecGenerator}.
     * @param output The {@link OpFusionCodegenSpecGenerator} as output of current spec generator.
     */
    public abstract void addOutput(int inputIdOfOutput, OpFusionCodegenSpecGenerator output);

    /** Generate Java source code to process the rows from operator corresponding input. */
    public abstract void processProduce(CodeGeneratorContext fusionCtx);

    /**
     * Consume the generated columns or row from current operator, call its output's {@link
     * OpFusionCodegenSpec#doProcessConsume(int, List, GeneratedExpression)} method.
     *
     * <p>Note that `outputVars` and `row` can't both be null.
     */
    public abstract String processConsume(List<GeneratedExpression> outputVars, String row);

    /** Generate Java source code to do clean work for operator corresponding input. */
    public abstract void endInputProduce(CodeGeneratorContext fusionCtx);

    /**
     * Generate code to trigger the clean work of operator, call its output's {@link
     * OpFusionCodegenSpec#doEndInputConsume(int)}. The leaf operator start to call endInputConsume
     * method.
     */
    public abstract String endInputConsume();

    public abstract void addReusableInitCode(CodeGeneratorContext fusionCtx);

    public abstract void addReusableOpenCode(CodeGeneratorContext fusionCtx);

    public abstract void addReusableCloseCode(CodeGeneratorContext fusionCtx);

    /** Implementation of {@link OpFusionContext}. */
    private static class OpFusionContextImpl implements OpFusionContext {

        private final OpFusionCodegenSpecGenerator fusionCodegenSpecGenerator;

        public OpFusionContextImpl(OpFusionCodegenSpecGenerator fusionCodegenSpecGenerator) {
            this.fusionCodegenSpecGenerator = fusionCodegenSpecGenerator;
        }

        @Override
        public RowType getOutputType() {
            return fusionCodegenSpecGenerator.outputType;
        }

        @Override
        public double getManagedMemoryFraction() {
            return fusionCodegenSpecGenerator.managedMemoryFraction;
        }

        @Override
        public List<OpFusionContext> getInputFusionContexts() {
            return fusionCodegenSpecGenerator.getInputs().stream()
                    .map(OpFusionCodegenSpecGenerator::getOpFusionContext)
                    .collect(Collectors.toList());
        }

        @Override
        public void processProduce(CodeGeneratorContext codegenCtx) {
            fusionCodegenSpecGenerator.processProduce(codegenCtx);
        }

        @Override
        public void endInputProduce(CodeGeneratorContext codegenCtx) {
            fusionCodegenSpecGenerator.endInputProduce(codegenCtx);
        }

        @Override
        public String processConsume(List<GeneratedExpression> outputVars, String row) {
            return fusionCodegenSpecGenerator.processConsume(outputVars, row);
        }

        @Override
        public String endInputConsume() {
            return fusionCodegenSpecGenerator.endInputConsume();
        }
    }

    @Internal
    interface Context {
        /** Returns the managed memory fraction of the current operator used. */
        double getManagedMemoryFraction();
    }
}
