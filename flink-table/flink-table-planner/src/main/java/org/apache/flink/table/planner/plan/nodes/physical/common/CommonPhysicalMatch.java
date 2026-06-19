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

package org.apache.flink.table.planner.plan.nodes.physical.common;

import org.apache.flink.table.api.TableException;
import org.apache.flink.table.planner.plan.logical.MatchRecognize;
import org.apache.flink.table.planner.plan.nodes.physical.FlinkPhysicalRel;
import org.apache.flink.table.planner.plan.utils.PythonUtil;
import org.apache.flink.table.planner.plan.utils.RelExplainUtil;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.type.RelDataType;

import scala.collection.JavaConverters;
import scala.collection.Seq;

/** Base physical RelNode which matches along with MATCH_RECOGNIZE. */
public abstract class CommonPhysicalMatch extends SingleRel implements FlinkPhysicalRel {

    private final MatchRecognize logicalMatch;
    private final RelDataType outputRowType;

    public CommonPhysicalMatch(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelNode inputNode,
            MatchRecognize logicalMatch,
            RelDataType outputRowType) {
        super(cluster, traitSet, inputNode);
        if (logicalMatch.measures().values().stream()
                        .anyMatch(m -> PythonUtil.containsPythonCall(m, null))
                || logicalMatch.patternDefinitions().values().stream()
                        .anyMatch(p -> PythonUtil.containsPythonCall(p, null))) {
            throw new TableException("Python Function can not be used in MATCH_RECOGNIZE for now.");
        }
        this.logicalMatch = logicalMatch;
        this.outputRowType = outputRowType;
    }

    @Override
    protected RelDataType deriveRowType() {
        return outputRowType;
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        RelDataType inputRowType = getInput().getRowType();
        Seq<String> fieldNames =
                JavaConverters.asScalaBufferConverter(inputRowType.getFieldNames()).asScala();
        return super.explainTerms(pw)
                .itemIf(
                        "partitionBy",
                        RelExplainUtil.fieldToString(
                                logicalMatch.partitionKeys().toArray(), inputRowType),
                        !logicalMatch.partitionKeys().isEmpty())
                .itemIf(
                        "orderBy",
                        RelExplainUtil.collationToString(logicalMatch.orderKeys(), inputRowType),
                        !logicalMatch.orderKeys().getFieldCollations().isEmpty())
                .itemIf(
                        "measures",
                        RelExplainUtil.measuresDefineToString(
                                logicalMatch.measures(),
                                fieldNames.toList(),
                                this::getExpressionString,
                                convertToExpressionDetail(pw.getDetailLevel())),
                        !logicalMatch.measures().isEmpty())
                .item("rowsPerMatch", RelExplainUtil.rowsPerMatchToString(logicalMatch.allRows()))
                .item("after", RelExplainUtil.afterMatchToString(logicalMatch.after(), fieldNames))
                .item("pattern", logicalMatch.pattern().toString())
                .itemIf(
                        "subset",
                        RelExplainUtil.subsetToString(logicalMatch.subsets()),
                        !logicalMatch.subsets().isEmpty())
                .item("define", logicalMatch.patternDefinitions());
    }

    public MatchRecognize getLogicalMatch() {
        return logicalMatch;
    }
}
