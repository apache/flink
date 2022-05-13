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

package org.apache.flink.table.planner.delegation.hive.copy;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/** Counterpart of hive's org.apache.hadoop.hive.ql.parse.TypeCheckCtx. */
public class HiveParserTypeCheckCtx implements NodeProcessorCtx {

    protected static final Logger LOG = LoggerFactory.getLogger(HiveParserTypeCheckCtx.class);

    /**
     * The row resolver of the previous operator. This field is used to generate expression
     * descriptors from the expression ASTs.
     */
    private HiveParserRowResolver inputRR;

    /**
     * HiveParserRowResolver of outer query. This is used to resolve co-rrelated columns in Filter
     * TODO: this currently will only be able to resolve reference to parent query's column this
     * will not work for references to grand-parent column
     */
    private HiveParserRowResolver outerRR;

    /** Map from astnode of a subquery to it's logical plan. */
    private Map<HiveParserASTNode, RelNode> subqueryToRelNode;

    private final boolean useCaching;

    private final boolean foldExpr;

    /** Receives translations which will need to be applied during unparse. */
    private HiveParserUnparseTranslator unparseTranslator;

    /** Potential typecheck error reason. */
    private String error;

    /** The node that generated the potential typecheck error. */
    private HiveParserASTNode errorSrcNode;

    /** Whether to allow stateful UDF invocations. */
    private boolean allowStatefulFunctions;

    private boolean allowDistinctFunctions;

    private final boolean allowGBExprElimination;

    private final boolean allowAllColRef;

    private final boolean allowFunctionStar;

    private final boolean allowWindowing;

    // "[]" : LSQUARE/INDEX Expression
    private final boolean allowIndexExpr;

    private final boolean allowSubQueryExpr;

    // used to get SqlOperators from flink
    private final FrameworkConfig frameworkConfig;
    private final RelOptCluster cluster;

    /**
     * Constructor.
     *
     * @param inputRR The input row resolver of the previous operator.
     */
    public HiveParserTypeCheckCtx(
            HiveParserRowResolver inputRR, FrameworkConfig frameworkConfig, RelOptCluster cluster) {
        this(inputRR, true, false, frameworkConfig, cluster);
    }

    public HiveParserTypeCheckCtx(
            HiveParserRowResolver inputRR,
            boolean useCaching,
            boolean foldExpr,
            FrameworkConfig frameworkConfig,
            RelOptCluster cluster) {
        this(
                inputRR,
                useCaching,
                foldExpr,
                false,
                true,
                true,
                true,
                true,
                true,
                true,
                true,
                frameworkConfig,
                cluster);
    }

    public HiveParserTypeCheckCtx(
            HiveParserRowResolver inputRR,
            boolean useCaching,
            boolean foldExpr,
            boolean allowStatefulFunctions,
            boolean allowDistinctFunctions,
            boolean allowGBExprElimination,
            boolean allowAllColRef,
            boolean allowFunctionStar,
            boolean allowWindowing,
            boolean allowIndexExpr,
            boolean allowSubQueryExpr,
            FrameworkConfig frameworkConfig,
            RelOptCluster cluster) {
        setInputRR(inputRR);
        error = null;
        this.useCaching = useCaching;
        this.foldExpr = foldExpr;
        this.allowStatefulFunctions = allowStatefulFunctions;
        this.allowDistinctFunctions = allowDistinctFunctions;
        this.allowGBExprElimination = allowGBExprElimination;
        this.allowAllColRef = allowAllColRef;
        this.allowFunctionStar = allowFunctionStar;
        this.allowWindowing = allowWindowing;
        this.allowIndexExpr = allowIndexExpr;
        this.allowSubQueryExpr = allowSubQueryExpr;
        this.outerRR = null;
        this.subqueryToRelNode = null;
        this.frameworkConfig = frameworkConfig;
        this.cluster = cluster;
    }

    /** @param inputRR the inputRR to set */
    public void setInputRR(HiveParserRowResolver inputRR) {
        this.inputRR = inputRR;
    }

    /** @return the inputRR */
    public HiveParserRowResolver getInputRR() {
        return inputRR;
    }

    /** @param outerRR the outerRR to set */
    public void setOuterRR(HiveParserRowResolver outerRR) {
        this.outerRR = outerRR;
    }

    /** @return the outerRR */
    public HiveParserRowResolver getOuterRR() {
        return outerRR;
    }

    /** @param subqueryToRelNode the subqueryToRelNode to set */
    public void setSubqueryToRelNode(Map<HiveParserASTNode, RelNode> subqueryToRelNode) {
        this.subqueryToRelNode = subqueryToRelNode;
    }

    /** @return the outerRR */
    public Map<HiveParserASTNode, RelNode> getSubqueryToRelNode() {
        return subqueryToRelNode;
    }

    /** @param unparseTranslator the unparseTranslator to set */
    public void setUnparseTranslator(HiveParserUnparseTranslator unparseTranslator) {
        this.unparseTranslator = unparseTranslator;
    }

    /** @return the unparseTranslator */
    public HiveParserUnparseTranslator getUnparseTranslator() {
        return unparseTranslator;
    }

    /** @param allowStatefulFunctions whether to allow stateful UDF invocations */
    public void setAllowStatefulFunctions(boolean allowStatefulFunctions) {
        this.allowStatefulFunctions = allowStatefulFunctions;
    }

    /** @return whether to allow stateful UDF invocations */
    public boolean getAllowStatefulFunctions() {
        return allowStatefulFunctions;
    }

    /** @param error the error to set */
    public void setError(String error, HiveParserASTNode errorSrcNode) {
        if (LOG.isDebugEnabled()) {
            // Logger the callstack from which the error has been set.
            LOG.debug(
                    "Setting error: ["
                            + error
                            + "] from "
                            + ((errorSrcNode == null) ? "null" : errorSrcNode.toStringTree()),
                    new Exception());
        }
        this.error = error;
        this.errorSrcNode = errorSrcNode;
    }

    /** @return the error */
    public String getError() {
        return error;
    }

    public HiveParserASTNode getErrorSrcNode() {
        return errorSrcNode;
    }

    public void setAllowDistinctFunctions(boolean allowDistinctFunctions) {
        this.allowDistinctFunctions = allowDistinctFunctions;
    }

    public boolean getAllowDistinctFunctions() {
        return allowDistinctFunctions;
    }

    public boolean getAllowGBExprElimination() {
        return allowGBExprElimination;
    }

    public boolean getallowAllColRef() {
        return allowAllColRef;
    }

    public boolean getallowFunctionStar() {
        return allowFunctionStar;
    }

    public boolean getallowWindowing() {
        return allowWindowing;
    }

    public boolean getallowIndexExpr() {
        return allowIndexExpr;
    }

    public boolean getallowSubQueryExpr() {
        return allowSubQueryExpr;
    }

    public boolean isUseCaching() {
        return useCaching;
    }

    public boolean isFoldExpr() {
        return foldExpr;
    }

    public SqlOperatorTable getSqlOperatorTable() {
        return frameworkConfig.getOperatorTable();
    }

    public RelDataTypeFactory getTypeFactory() {
        return cluster.getTypeFactory();
    }

    public RelOptCluster getCluster() {
        return cluster;
    }
}
