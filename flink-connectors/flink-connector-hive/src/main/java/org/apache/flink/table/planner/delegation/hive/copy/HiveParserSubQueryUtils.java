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

import org.apache.flink.table.planner.delegation.hive.parse.HiveASTParser;
import org.apache.flink.table.planner.delegation.hive.parse.HiveParserErrorMsg;

import org.antlr.runtime.tree.CommonTreeAdaptor;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFCount;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFResolver;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

/** Counterpart of hive's org.apache.hadoop.hive.ql.parse.SubQueryUtils. */
public class HiveParserSubQueryUtils {
    static void extractConjuncts(HiveParserASTNode node, List<HiveParserASTNode> conjuncts) {
        if (node.getType() != HiveASTParser.KW_AND) {
            conjuncts.add(node);
            return;
        }
        extractConjuncts((HiveParserASTNode) node.getChild(0), conjuncts);
        extractConjuncts((HiveParserASTNode) node.getChild(1), conjuncts);
    }

    public static List<HiveParserASTNode> findSubQueries(HiveParserASTNode node) {
        List<HiveParserASTNode> subQueries = new ArrayList<>();
        findSubQueries(node, subQueries);
        return subQueries;
    }

    private static void findSubQueries(HiveParserASTNode node, List<HiveParserASTNode> subQueries) {
        Deque<HiveParserASTNode> stack = new ArrayDeque<>();
        stack.push(node);

        while (!stack.isEmpty()) {
            HiveParserASTNode next = stack.pop();

            switch (next.getType()) {
                case HiveASTParser.TOK_SUBQUERY_EXPR:
                    subQueries.add(next);
                    break;
                default:
                    int childCount = next.getChildCount();
                    for (int i = childCount - 1; i >= 0; i--) {
                        stack.push((HiveParserASTNode) next.getChild(i));
                    }
            }
        }
    }

    public static HiveParserQBSubQuery buildSubQuery(
            int sqIdx,
            HiveParserASTNode sqAST,
            HiveParserASTNode originalSQAST,
            HiveParserContext ctx,
            FrameworkConfig frameworkConfig,
            RelOptCluster cluster)
            throws SemanticException {
        HiveParserASTNode sqOp = (HiveParserASTNode) sqAST.getChild(0);
        HiveParserASTNode sq = (HiveParserASTNode) sqAST.getChild(1);
        HiveParserASTNode outerQueryExpr = (HiveParserASTNode) sqAST.getChild(2);

        /*
         * Restriction.8.m :: We allow only 1 SubQuery expression per Query.
         */
        if (outerQueryExpr != null && outerQueryExpr.getType() == HiveASTParser.TOK_SUBQUERY_EXPR) {

            throw new SemanticException(
                    HiveParserErrorMsg.getMsg(
                            ErrorMsg.UNSUPPORTED_SUBQUERY_EXPRESSION,
                            originalSQAST.getChild(1),
                            "Only 1 SubQuery expression is supported."));
        }

        return new HiveParserQBSubQuery(
                sqIdx, sq, buildSQOperator(sqOp), originalSQAST, ctx, frameworkConfig, cluster);
    }

    static HiveParserQBSubQuery.SubQueryTypeDef buildSQOperator(HiveParserASTNode astSQOp)
            throws SemanticException {
        HiveParserASTNode opAST = (HiveParserASTNode) astSQOp.getChild(0);
        HiveParserQBSubQuery.SubQueryType type = HiveParserQBSubQuery.SubQueryType.get(opAST);
        return new HiveParserQBSubQuery.SubQueryTypeDef(opAST, type);
    }

    /*
     * is this expr a UDAF invocation; does it imply windowing
     * @return
     * 0 if implies neither
     * 1 if implies aggregation
     * 2 if implies count
     * 3 if implies windowing
     */
    static int checkAggOrWindowing(HiveParserASTNode expressionTree) throws SemanticException {
        int exprTokenType = expressionTree.getToken().getType();
        if (exprTokenType == HiveASTParser.TOK_FUNCTION
                || exprTokenType == HiveASTParser.TOK_FUNCTIONDI
                || exprTokenType == HiveASTParser.TOK_FUNCTIONSTAR) {
            assert (expressionTree.getChildCount() != 0);
            if (expressionTree.getChild(expressionTree.getChildCount() - 1).getType()
                    == HiveASTParser.TOK_WINDOWSPEC) {
                return 3;
            }
            if (expressionTree.getChild(0).getType() == HiveASTParser.Identifier) {
                String functionName =
                        HiveParserBaseSemanticAnalyzer.unescapeIdentifier(
                                expressionTree.getChild(0).getText());
                GenericUDAFResolver udafResolver =
                        FunctionRegistry.getGenericUDAFResolver(functionName);
                if (udafResolver != null) {
                    // we need to know if it is COUNT since this is specialized for IN/NOT IN
                    // corr subqueries.
                    if (udafResolver instanceof GenericUDAFCount) {
                        return 2;
                    }
                    return 1;
                }
            }
        }
        int r = 0;
        for (int i = 0; i < expressionTree.getChildCount(); i++) {
            int c = checkAggOrWindowing((HiveParserASTNode) expressionTree.getChild(i));
            r = Math.max(r, c);
        }
        return r;
    }

    static HiveParserASTNode subQueryWhere(HiveParserASTNode insertClause) {
        if (insertClause.getChildCount() > 2
                && insertClause.getChild(2).getType() == HiveASTParser.TOK_WHERE) {
            return (HiveParserASTNode) insertClause.getChild(2);
        }
        return null;
    }

    private static void checkForSubqueries(HiveParserASTNode node) throws SemanticException {
        // allow NOT but throw an error for rest
        if (node.getType() == HiveASTParser.TOK_SUBQUERY_EXPR
                && node.getParent().getType() != HiveASTParser.KW_NOT) {
            throw new SemanticException(
                    ErrorMsg.UNSUPPORTED_SUBQUERY_EXPRESSION.getMsg(
                            "Invalid subquery. Subquery in SELECT could only be top-level expression"));
        }
        for (int i = 0; i < node.getChildCount(); i++) {
            checkForSubqueries((HiveParserASTNode) node.getChild(i));
        }
    }

    /*
     * Given a TOK_SELECT this checks IF there is a subquery
     *  it is top level expression, else it throws an error
     */
    public static void checkForTopLevelSubqueries(HiveParserASTNode selExprList)
            throws SemanticException {
        // should be either SELECT or SELECT DISTINCT
        assert (selExprList.getType() == HiveASTParser.TOK_SELECT
                || selExprList.getType() == HiveASTParser.TOK_SELECTDI);
        for (int i = 0; i < selExprList.getChildCount(); i++) {
            HiveParserASTNode selExpr = (HiveParserASTNode) selExprList.getChild(i);
            // could get either query hint or select expr
            assert (selExpr.getType() == HiveASTParser.TOK_SELEXPR
                    || selExpr.getType() == HiveASTParser.QUERY_HINT);

            if (selExpr.getType() == HiveASTParser.QUERY_HINT) {
                // skip query hints
                continue;
            }

            if (selExpr.getChildCount() == 1
                    && selExpr.getChild(0).getType() == HiveASTParser.TOK_SUBQUERY_EXPR) {
                if (selExprList.getType() == HiveASTParser.TOK_SELECTDI) {
                    throw new SemanticException(
                            ErrorMsg.UNSUPPORTED_SUBQUERY_EXPRESSION.getMsg(
                                    "Invalid subquery. Subquery with DISTINCT clause is not supported!"));
                }
                continue; // we are good since subquery is top level expression
            }
            // otherwise we need to make sure that there is no subquery at any level
            for (int j = 0; j < selExpr.getChildCount(); j++) {
                checkForSubqueries((HiveParserASTNode) selExpr.getChild(j));
            }
        }
    }

    /** ISubQueryJoinInfo. */
    public interface ISubQueryJoinInfo {
        String getAlias();

        HiveParserQBSubQuery getSubQuery();
    }

    /*
     * Using CommonTreeAdaptor because the Adaptor in ParseDriver doesn't carry
     * the token indexes when duplicating a Tree.
     */
    public static final CommonTreeAdaptor ADAPTOR = new CommonTreeAdaptor();
}
