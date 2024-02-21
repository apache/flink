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

package org.apache.flink.table.planner.plan.nodes.hive;

import org.apache.flink.table.runtime.script.ScriptTransformIOInfo;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.type.RelDataType;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Copied from Flink code base.
 *
 * <p>LogicalScriptTransform is used to represent the sql semantic "TRANSFORM c1, c2, xx USING
 * 'script'".
 */
public class LogicalScriptTransform extends SingleRel {

    // which fields to be process by the script
    private final int[] fieldIndices;
    // the script to run
    private final String script;
    // the input/out schema for the script
    private final ScriptTransformIOInfo scriptTransformIOInfo;
    private final RelDataType outputRowType;

    private LogicalScriptTransform(
            RelOptCluster cluster,
            RelTraitSet traits,
            RelNode input,
            int[] fieldIndices,
            String script,
            ScriptTransformIOInfo scriptTransformIOInfo,
            RelDataType outputRowType) {
        super(cluster, traits, input);
        this.fieldIndices = fieldIndices;
        this.script = script;
        this.scriptTransformIOInfo = scriptTransformIOInfo;
        this.outputRowType = outputRowType;
    }

    public static LogicalScriptTransform create(
            RelNode input,
            int[] fieldIndices,
            String script,
            ScriptTransformIOInfo scriptTransformIOInfo,
            RelDataType outputRowType) {
        return new LogicalScriptTransform(
                input.getCluster(),
                input.getTraitSet(),
                input,
                fieldIndices,
                script,
                scriptTransformIOInfo,
                outputRowType);
    }

    @Override
    public LogicalScriptTransform copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new LogicalScriptTransform(
                getCluster(),
                traitSet,
                inputs.get(0),
                fieldIndices,
                script,
                scriptTransformIOInfo,
                outputRowType);
    }

    @Override
    public RelNode accept(RelShuttle shuttle) {
        return shuttle.visit(this);
    }

    public String getScript() {
        return script;
    }

    public int[] getFieldIndices() {
        return fieldIndices;
    }

    public ScriptTransformIOInfo getScriptInputOutSchema() {
        return scriptTransformIOInfo;
    }

    @Override
    public RelDataType deriveRowType() {
        return outputRowType;
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        super.explainTerms(pw);
        pw.item("script-inputs", fieldToString(fieldIndices, input.getRowType()))
                .item("script-outputs", String.join(", ", getRowType().getFieldNames()))
                .item("script", script)
                .item("script-io-info", scriptTransformIOInfo);
        return pw;
    }

    private String fieldToString(int[] fieldIndices, RelDataType inputType) {
        List<String> fieldNames = inputType.getFieldNames();
        return Arrays.stream(fieldIndices)
                .mapToObj(fieldNames::get)
                .collect(Collectors.joining(", "));
    }
}
