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

import org.apache.flink.table.planner.delegation.hive.HiveParserTypeCheckProcFactory;
import org.apache.flink.table.planner.delegation.hive.HiveParserUtils;
import org.apache.flink.table.planner.delegation.hive.parse.HiveASTParser;
import org.apache.flink.table.planner.delegation.hive.parse.HiveParserErrorMsg;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.hadoop.hive.common.ObjectPair;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

import static org.apache.flink.table.planner.delegation.hive.copy.HiveParserBaseSemanticAnalyzer.unescapeIdentifier;

/** Counterpart of hive's org.apache.hadoop.hive.ql.parse.QBSubQuery. */
public class HiveParserQBSubQuery {

    /** SubQueryType. */
    public enum SubQueryType {
        EXISTS,
        NOT_EXISTS,
        IN,
        NOT_IN,
        SCALAR;

        public static HiveParserQBSubQuery.SubQueryType get(HiveParserASTNode opNode)
                throws SemanticException {
            if (opNode == null) {
                return SCALAR;
            }

            switch (opNode.getType()) {
                    // opNode's type is always either KW_EXISTS or KW_IN never NOTEXISTS or NOTIN
                    //  to figure this out we need to check it's grand parent's parent
                case HiveASTParser.KW_EXISTS:
                    if (opNode.getParent().getParent().getParent() != null
                            && opNode.getParent().getParent().getParent().getType()
                                    == HiveASTParser.KW_NOT) {
                        return NOT_EXISTS;
                    }
                    return EXISTS;
                case HiveASTParser.TOK_SUBQUERY_OP_NOTEXISTS:
                    return NOT_EXISTS;
                case HiveASTParser.KW_IN:
                    if (opNode.getParent().getParent().getParent() != null
                            && opNode.getParent().getParent().getParent().getType()
                                    == HiveASTParser.KW_NOT) {
                        return NOT_IN;
                    }
                    return IN;
                case HiveASTParser.TOK_SUBQUERY_OP_NOTIN:
                    return NOT_IN;
                default:
                    throw new SemanticException(
                            HiveParserUtils.generateErrorMessage(
                                    opNode, "Operator not supported in SubQuery use."));
            }
        }
    }

    /** SubQueryTypeDef. */
    public static class SubQueryTypeDef {
        private final HiveParserASTNode ast;
        private final HiveParserQBSubQuery.SubQueryType type;

        public SubQueryTypeDef(HiveParserASTNode ast, HiveParserQBSubQuery.SubQueryType type) {
            super();
            this.ast = ast;
            this.type = type;
        }

        public HiveParserASTNode getAst() {
            return ast;
        }

        public HiveParserQBSubQuery.SubQueryType getType() {
            return type;
        }
    }

    /*
     * An expression is either the left/right side of an Equality predicate in the SubQuery where
     * clause; or it is the entire conjunct. For e.g. if the Where Clause for a SubQuery is:
     * where R1.X = R2.Y and R2.Z > 7
     * Then the expressions analyzed are R1.X, R2.X ( the left and right sides of the Equality
     * predicate); and R2.Z > 7.
     *
     * The ExprType tracks whether the expr:
     * - has a reference to a SubQuery table source
     * - has a reference to Outer(parent) Query table source
     */
    enum ExprType {
        REFERS_NONE(false, false) {
            @Override
            public HiveParserQBSubQuery.ExprType combine(HiveParserQBSubQuery.ExprType other) {
                return other;
            }
        },
        REFERS_PARENT(true, false) {
            @Override
            public HiveParserQBSubQuery.ExprType combine(HiveParserQBSubQuery.ExprType other) {
                switch (other) {
                    case REFERS_SUBQUERY:
                    case REFERS_BOTH:
                        return REFERS_BOTH;
                    default:
                        return this;
                }
            }
        },
        REFERS_SUBQUERY(false, true) {
            @Override
            public HiveParserQBSubQuery.ExprType combine(HiveParserQBSubQuery.ExprType other) {
                switch (other) {
                    case REFERS_PARENT:
                    case REFERS_BOTH:
                        return REFERS_BOTH;
                    default:
                        return this;
                }
            }
        },
        REFERS_BOTH(true, true) {
            @Override
            public HiveParserQBSubQuery.ExprType combine(HiveParserQBSubQuery.ExprType other) {
                return this;
            }
        };

        final boolean refersParent;
        final boolean refersSubQuery;

        ExprType(boolean refersParent, boolean refersSubQuery) {
            this.refersParent = refersParent;
            this.refersSubQuery = refersSubQuery;
        }

        public boolean refersParent() {
            return refersParent;
        }

        public abstract HiveParserQBSubQuery.ExprType combine(HiveParserQBSubQuery.ExprType other);
    }

    /*
     * This class captures the information about a
     * conjunct in the where clause of the SubQuery.
     * For a equality predicate it capture for each side:
     * - the AST
     * - the type of Expression (basically what columns are referenced)
     * - for Expressions that refer the parent it captures the
     *   parent's ColumnInfo. In case of outer Aggregation expressions
     *   we need this to introduce a new mapping in the OuterQuery
     *   HiveParserRowResolver. A join condition must use qualified column references,
     *   so we generate a new name for the aggr expression and use it in the
     *   joining condition.
     *   For e.g.
     *   having exists ( select x from R2 where y = min(R1.z) )
     *   where the expression 'min(R1.z)' is from the outer Query.
     *   We give this expression a new name like 'R1._gby_sq_col_1'
     *   and use the join condition: R1._gby_sq_col_1 = R2.y
     */
    static class Conjunct {
        private final HiveParserASTNode leftExpr;
        private final HiveParserASTNode rightExpr;
        private final HiveParserQBSubQuery.ExprType leftExprType;
        private final HiveParserQBSubQuery.ExprType rightExprType;
        private final ColumnInfo leftOuterColInfo;
        private final ColumnInfo rightOuterColInfo;

        Conjunct(
                HiveParserASTNode leftExpr,
                HiveParserASTNode rightExpr,
                HiveParserQBSubQuery.ExprType leftExprType,
                HiveParserQBSubQuery.ExprType rightExprType,
                ColumnInfo leftOuterColInfo,
                ColumnInfo rightOuterColInfo) {
            super();
            this.leftExpr = leftExpr;
            this.rightExpr = rightExpr;
            this.leftExprType = leftExprType;
            this.rightExprType = rightExprType;
            this.leftOuterColInfo = leftOuterColInfo;
            this.rightOuterColInfo = rightOuterColInfo;
        }

        boolean eitherSideRefersBoth() {
            if (leftExprType == HiveParserQBSubQuery.ExprType.REFERS_BOTH) {
                return true;
            } else if (rightExpr != null) {
                return rightExprType == HiveParserQBSubQuery.ExprType.REFERS_BOTH;
            }
            return false;
        }

        boolean isCorrelated() {
            if (rightExpr != null) {
                return leftExprType.combine(rightExprType)
                        == HiveParserQBSubQuery.ExprType.REFERS_BOTH;
            }
            return false;
        }
    }

    static class ConjunctAnalyzer {
        private final HiveParserRowResolver parentQueryRR;
        boolean forHavingClause;
        String parentQueryNewAlias;
        NodeProcessor defaultExprProcessor;
        Stack<Node> stack;
        private final FrameworkConfig frameworkConfig;
        private final RelOptCluster cluster;

        ConjunctAnalyzer(
                HiveParserRowResolver parentQueryRR,
                boolean forHavingClause,
                String parentQueryNewAlias,
                FrameworkConfig frameworkConfig,
                RelOptCluster cluster) {
            this.parentQueryRR = parentQueryRR;
            defaultExprProcessor = new HiveParserTypeCheckProcFactory.DefaultExprProcessor();
            this.forHavingClause = forHavingClause;
            this.parentQueryNewAlias = parentQueryNewAlias;
            stack = new Stack<>();
            this.frameworkConfig = frameworkConfig;
            this.cluster = cluster;
        }

        /*
         * 1. On encountering a DOT, we attempt to resolve the leftmost name
         *    to the Parent Query.
         * 2. An unqualified name is assumed to be a SubQuery reference.
         *    We don't attempt to resolve this to the Parent; because
         *    we require all Parent column references to be qualified.
         * 3. All other expressions have a Type based on their children.
         *    An Expr w/o children is assumed to refer to neither.
         */
        private ObjectPair<HiveParserQBSubQuery.ExprType, ColumnInfo> analyzeExpr(
                HiveParserASTNode expr) {
            ColumnInfo cInfo = null;
            if (forHavingClause) {
                try {
                    cInfo = parentQueryRR.getExpression(expr);
                    if (cInfo != null) {
                        return ObjectPair.create(
                                HiveParserQBSubQuery.ExprType.REFERS_PARENT, cInfo);
                    }
                } catch (SemanticException se) {
                }
            }
            if (expr.getType() == HiveASTParser.DOT) {
                HiveParserASTNode dot = firstDot(expr);
                cInfo = resolveDot(dot);
                if (cInfo != null) {
                    return ObjectPair.create(HiveParserQBSubQuery.ExprType.REFERS_PARENT, cInfo);
                }
                return ObjectPair.create(HiveParserQBSubQuery.ExprType.REFERS_SUBQUERY, null);
            } else if (expr.getType() == HiveASTParser.TOK_TABLE_OR_COL) {
                return ObjectPair.create(HiveParserQBSubQuery.ExprType.REFERS_SUBQUERY, null);
            } else {
                HiveParserQBSubQuery.ExprType exprType = HiveParserQBSubQuery.ExprType.REFERS_NONE;
                int cnt = expr.getChildCount();
                for (int i = 0; i < cnt; i++) {
                    HiveParserASTNode child = (HiveParserASTNode) expr.getChild(i);
                    exprType = exprType.combine(analyzeExpr(child).getFirst());
                }
                return ObjectPair.create(exprType, null);
            }
        }

        /*
         * 1. The only correlation operator we check for is EQUAL; because that is
         *    the one for which we can do a Algebraic transformation.
         * 2. For expressions that are not an EQUAL predicate, we treat them as conjuncts
         *    having only 1 side. These should only contain references to the SubQuery
         *    table sources.
         * 3. For expressions that are an EQUAL predicate; we analyze each side and let the
         *    left and right exprs in the Conjunct object.
         *
         * @return Conjunct  contains details on the left and right side of the conjunct expression.
         */
        HiveParserQBSubQuery.Conjunct analyzeConjunct(HiveParserASTNode conjunct)
                throws SemanticException {
            int type = conjunct.getType();

            if (type == HiveASTParser.EQUAL) {
                HiveParserASTNode left = (HiveParserASTNode) conjunct.getChild(0);
                HiveParserASTNode right = (HiveParserASTNode) conjunct.getChild(1);
                ObjectPair<HiveParserQBSubQuery.ExprType, ColumnInfo> leftInfo = analyzeExpr(left);
                ObjectPair<HiveParserQBSubQuery.ExprType, ColumnInfo> rightInfo =
                        analyzeExpr(right);

                return new HiveParserQBSubQuery.Conjunct(
                        left,
                        right,
                        leftInfo.getFirst(),
                        rightInfo.getFirst(),
                        leftInfo.getSecond(),
                        rightInfo.getSecond());
            } else {
                ObjectPair<HiveParserQBSubQuery.ExprType, ColumnInfo> sqExprInfo =
                        analyzeExpr(conjunct);
                return new HiveParserQBSubQuery.Conjunct(
                        conjunct,
                        null,
                        sqExprInfo.getFirst(),
                        null,
                        sqExprInfo.getSecond(),
                        sqExprInfo.getSecond());
            }
        }

        /*
         * Try to resolve a qualified name as a column reference on the Parent Query's HiveParserRowResolver.
         * Apply this logic on the leftmost(first) dot in an AST tree.
         */
        protected ColumnInfo resolveDot(HiveParserASTNode node) {
            try {
                HiveParserTypeCheckCtx tcCtx =
                        new HiveParserTypeCheckCtx(parentQueryRR, frameworkConfig, cluster);
                String str = unescapeIdentifier(node.getChild(1).getText());
                ExprNodeDesc idDesc =
                        new ExprNodeConstantDesc(TypeInfoFactory.stringTypeInfo, str.toLowerCase());
                Object desc = defaultExprProcessor.process(node, stack, tcCtx, null, idDesc);
                if (desc instanceof ExprNodeColumnDesc) {
                    ExprNodeColumnDesc colDesc = (ExprNodeColumnDesc) desc;
                    String[] qualName = parentQueryRR.reverseLookup(colDesc.getColumn());
                    return parentQueryRR.get(qualName[0], qualName[1]);
                }
            } catch (SemanticException se) {
            }
            return null;
        }

        /*
         * We want to resolve the leftmost name to the Parent Query's RR.
         * Hence we do a left walk down the AST, until we reach the bottom most DOT.
         */
        protected HiveParserASTNode firstDot(HiveParserASTNode dot) {
            HiveParserASTNode firstChild = (HiveParserASTNode) dot.getChild(0);
            if (firstChild != null && firstChild.getType() == HiveASTParser.DOT) {
                return firstDot(firstChild);
            }
            return dot;
        }
    }

    /*
     * When transforming a Not In SubQuery we need to check for nulls in the
     * Joining expressions of the SubQuery. If there are nulls then the SubQuery always
     * return false. For more details see
     * https://issues.apache.org/jira/secure/attachment/12614003/SubQuerySpec.pdf
     *
     * Basically, SQL semantics say that:
     * - R1.A not in (null, 1, 2, ...)
     *   is always false.
     *   A 'not in' operator is equivalent to a '<> all'. Since a not equal check with null
     *   returns false, a not in predicate against aset with a 'null' value always returns false.
     *
     * So for not in SubQuery predicates:
     * - we join in a null count predicate.
     * - And the joining condition is that the 'Null Count' query has a count of 0.
     *
     */
    class NotInCheck implements HiveParserSubQueryUtils.ISubQueryJoinInfo {

        /*
         * expressions in SubQ that are joined to the Outer Query.
         */
        List<HiveParserASTNode> subQryCorrExprs;

        NotInCheck() {
            subQryCorrExprs = new ArrayList<>();
        }

        public String getAlias() {
            return HiveParserQBSubQuery.this.getAlias() + "_notin_nullcheck";
        }

        public HiveParserQBSubQuery getSubQuery() {
            return HiveParserQBSubQuery.this;
        }
    }

    private final String alias;
    private final HiveParserASTNode subQueryAST;
    private final HiveParserQBSubQuery.SubQueryTypeDef operator;
    private final transient HiveParserASTNodeOrigin originalSQASTOrigin;

    private HiveParserQBSubQuery.NotInCheck notInCheck;

    private final HiveParserSubQueryDiagnostic.QBSubQueryRewrite subQueryDiagnostic;

    private final FrameworkConfig frameworkConfig;
    private final RelOptCluster cluster;

    public HiveParserQBSubQuery(
            int sqIdx,
            HiveParserASTNode subQueryAST,
            HiveParserQBSubQuery.SubQueryTypeDef operator,
            HiveParserASTNode originalSQAST,
            HiveParserContext ctx,
            FrameworkConfig frameworkConfig,
            RelOptCluster cluster) {
        super();
        this.subQueryAST = subQueryAST;
        this.operator = operator;
        this.alias = "sq_" + sqIdx;
        String s =
                ctx.getTokenRewriteStream()
                        .toString(
                                originalSQAST.getTokenStartIndex(),
                                originalSQAST.getTokenStopIndex());
        originalSQASTOrigin =
                new HiveParserASTNodeOrigin("SubQuery", alias, s, alias, originalSQAST);

        if (operator.getType() == HiveParserQBSubQuery.SubQueryType.NOT_IN) {
            notInCheck = new HiveParserQBSubQuery.NotInCheck();
        }

        subQueryDiagnostic =
                HiveParserSubQueryDiagnostic.getRewrite(this, ctx.getTokenRewriteStream(), ctx);

        this.frameworkConfig = frameworkConfig;
        this.cluster = cluster;
    }

    public boolean subqueryRestrictionsCheck(
            HiveParserRowResolver parentQueryRR, boolean forHavingClause, String outerQueryAlias)
            throws SemanticException {
        HiveParserASTNode insertClause =
                getChildFromSubqueryAST("Insert", HiveASTParser.TOK_INSERT);

        HiveParserASTNode selectClause = (HiveParserASTNode) insertClause.getChild(1);

        int selectExprStart = 0;
        if (selectClause.getChild(0).getType() == HiveASTParser.QUERY_HINT) {
            selectExprStart = 1;
        }

        /*
         * Check.5.h :: For In and Not In the SubQuery must implicitly or
         * explicitly only contain one select item.
         */
        if (operator.getType() != HiveParserQBSubQuery.SubQueryType.EXISTS
                && operator.getType() != HiveParserQBSubQuery.SubQueryType.NOT_EXISTS
                && selectClause.getChildCount() - selectExprStart > 1) {
            subQueryAST.setOrigin(originalSQASTOrigin);
            throw new SemanticException(
                    HiveParserErrorMsg.getMsg(
                            ErrorMsg.INVALID_SUBQUERY_EXPRESSION,
                            subQueryAST,
                            "SubQuery can contain only 1 item in Select List."));
        }

        boolean hasAggreateExprs = false;
        boolean hasWindowing = false;

        // we need to know if aggregate is COUNT since IN corr subq with count aggregate
        // is not special cased later in subquery remove rule
        boolean hasCount = false;
        for (int i = selectExprStart; i < selectClause.getChildCount(); i++) {

            HiveParserASTNode selectItem = (HiveParserASTNode) selectClause.getChild(i);
            int r = HiveParserSubQueryUtils.checkAggOrWindowing(selectItem);

            hasWindowing = hasWindowing | (r == 3);
            hasAggreateExprs = hasAggreateExprs | (r == 1 | r == 2);
            hasCount = hasCount | (r == 2);
        }

        HiveParserASTNode whereClause = HiveParserSubQueryUtils.subQueryWhere(insertClause);

        if (whereClause == null) {
            return false;
        }
        HiveParserASTNode searchCond = (HiveParserASTNode) whereClause.getChild(0);
        List<HiveParserASTNode> conjuncts = new ArrayList<>();
        HiveParserSubQueryUtils.extractConjuncts(searchCond, conjuncts);

        HiveParserQBSubQuery.ConjunctAnalyzer conjunctAnalyzer =
                new ConjunctAnalyzer(
                        parentQueryRR, forHavingClause, outerQueryAlias, frameworkConfig, cluster);

        boolean hasCorrelation = false;
        boolean hasNonEquiJoinPred = false;
        for (HiveParserASTNode conjunctAST : conjuncts) {
            HiveParserQBSubQuery.Conjunct conjunct = conjunctAnalyzer.analyzeConjunct(conjunctAST);
            if (conjunct.isCorrelated()) {
                hasCorrelation = true;
            }
            if (conjunct.eitherSideRefersBoth() && conjunctAST.getType() != HiveASTParser.EQUAL) {
                hasNonEquiJoinPred = true;
            }
        }
        boolean noImplicityGby = true;
        if (insertClause.getChild(1).getChildCount() > 3
                && insertClause.getChild(1).getChild(3).getType() == HiveASTParser.TOK_GROUPBY) {
            if (insertClause.getChild(1).getChild(3) != null) {
                noImplicityGby = false;
            }
        }

        /*
         * Restriction.14.h :: Correlated Sub Queries cannot contain Windowing clauses.
         */
        if (hasWindowing && hasCorrelation) {
            throw new SemanticException(
                    HiveParserErrorMsg.getMsg(
                            ErrorMsg.UNSUPPORTED_SUBQUERY_EXPRESSION,
                            subQueryAST,
                            "Correlated Sub Queries cannot contain Windowing clauses."));
        }

        /*
         * Restriction.13.m :: In the case of an implied Group By on a
         * correlated SubQuery, the SubQuery always returns 1 row.
         * An exists on a SubQuery with an implied GBy will always return true.
         * Whereas Algebraically transforming to a Join may not return true. See
         * Specification doc for details.
         * Similarly a not exists on a SubQuery with a implied GBY will always return false.
         */
        // Following is special cases for different type of subqueries which have aggregate and no
        // implicit group by
        // and are correlatd
        // * EXISTS/NOT EXISTS - NOT allowed, throw an error for now. We plan to allow this later
        // * SCALAR - only allow if it has non equi join predicate. This should return true since
        // later in subquery remove
        //              rule we need to know about this case.
        // * IN - always allowed, BUT returns true for cases with aggregate other than COUNT since
        // later in subquery remove
        //        rule we need to know about this case.
        // * NOT IN - always allow, but always return true because later subq remove rule will
        // generate diff plan for this case
        if (hasAggreateExprs && noImplicityGby) {

            if (operator.getType() == HiveParserQBSubQuery.SubQueryType.EXISTS
                    || operator.getType() == HiveParserQBSubQuery.SubQueryType.NOT_EXISTS) {
                if (hasCorrelation) {
                    throw new SemanticException(
                            HiveParserErrorMsg.getMsg(
                                    ErrorMsg.INVALID_SUBQUERY_EXPRESSION,
                                    subQueryAST,
                                    "A predicate on EXISTS/NOT EXISTS SubQuery with implicit Aggregation(no Group By clause) "
                                            + "cannot be rewritten."));
                }
            } else if (operator.getType() == HiveParserQBSubQuery.SubQueryType.SCALAR) {
                if (hasNonEquiJoinPred) {
                    throw new SemanticException(
                            HiveParserErrorMsg.getMsg(
                                    ErrorMsg.INVALID_SUBQUERY_EXPRESSION,
                                    subQueryAST,
                                    "Scalar subqueries with aggregate cannot have non-equi join predicate"));
                }
                return hasCorrelation;
            } else if (operator.getType() == HiveParserQBSubQuery.SubQueryType.IN) {
                return hasCount && hasCorrelation;
            } else if (operator.getType() == HiveParserQBSubQuery.SubQueryType.NOT_IN) {
                return hasCorrelation;
            }
        }
        return false;
    }

    private HiveParserASTNode getChildFromSubqueryAST(String errorMsg, int type)
            throws SemanticException {
        HiveParserASTNode childAST = (HiveParserASTNode) subQueryAST.getFirstChildWithType(type);
        if (childAST == null && errorMsg != null) {
            subQueryAST.setOrigin(originalSQASTOrigin);
            throw new SemanticException(
                    HiveParserErrorMsg.getMsg(
                            ErrorMsg.INVALID_SUBQUERY_EXPRESSION,
                            subQueryAST,
                            errorMsg + " clause is missing in SubQuery."));
        }
        return childAST;
    }

    public String getAlias() {
        return alias;
    }
}
