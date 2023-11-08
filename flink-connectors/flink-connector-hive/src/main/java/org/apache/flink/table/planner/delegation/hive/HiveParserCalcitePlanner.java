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

package org.apache.flink.table.planner.delegation.hive;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.calcite.bridge.CalciteContext;
import org.apache.flink.table.catalog.CatalogRegistry;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.hive.util.HiveTypeUtil;
import org.apache.flink.table.functions.hive.conversion.HiveInspectors;
import org.apache.flink.table.planner.delegation.hive.copy.HiveASTParseDriver;
import org.apache.flink.table.planner.delegation.hive.copy.HiveASTParseUtils;
import org.apache.flink.table.planner.delegation.hive.copy.HiveParserASTBuilder;
import org.apache.flink.table.planner.delegation.hive.copy.HiveParserASTNode;
import org.apache.flink.table.planner.delegation.hive.copy.HiveParserBaseSemanticAnalyzer;
import org.apache.flink.table.planner.delegation.hive.copy.HiveParserBaseSemanticAnalyzer.AggInfo;
import org.apache.flink.table.planner.delegation.hive.copy.HiveParserContext;
import org.apache.flink.table.planner.delegation.hive.copy.HiveParserJoinTypeCheckCtx;
import org.apache.flink.table.planner.delegation.hive.copy.HiveParserNamedJoinInfo;
import org.apache.flink.table.planner.delegation.hive.copy.HiveParserPreCboCtx;
import org.apache.flink.table.planner.delegation.hive.copy.HiveParserQB;
import org.apache.flink.table.planner.delegation.hive.copy.HiveParserQBExpr;
import org.apache.flink.table.planner.delegation.hive.copy.HiveParserQBParseInfo;
import org.apache.flink.table.planner.delegation.hive.copy.HiveParserQBSubQuery;
import org.apache.flink.table.planner.delegation.hive.copy.HiveParserQueryState;
import org.apache.flink.table.planner.delegation.hive.copy.HiveParserRowResolver;
import org.apache.flink.table.planner.delegation.hive.copy.HiveParserSemanticAnalyzer;
import org.apache.flink.table.planner.delegation.hive.copy.HiveParserSqlFunctionConverter;
import org.apache.flink.table.planner.delegation.hive.copy.HiveParserSubQueryUtils;
import org.apache.flink.table.planner.delegation.hive.copy.HiveParserTypeCheckCtx;
import org.apache.flink.table.planner.delegation.hive.copy.HiveParserTypeConverter;
import org.apache.flink.table.planner.delegation.hive.copy.HiveParserWindowingSpec;
import org.apache.flink.table.planner.delegation.hive.parse.HiveASTParser;
import org.apache.flink.table.planner.delegation.hive.parse.HiveParserCreateViewInfo;
import org.apache.flink.table.planner.delegation.hive.parse.HiveParserErrorMsg;
import org.apache.flink.table.planner.plan.nodes.hive.LogicalDistribution;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableList;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.ViewExpanders;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.SetOp;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.rel.logical.LogicalIntersect;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalMinus;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalTableFunctionScan;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexFieldCollation;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexWindowBound;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql2rel.DeduplicateCorrelateVariables;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.util.CompositeList;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;
import org.apache.hadoop.hive.common.ObjectPair;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.metadata.VirtualColumn;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelOptUtil;
import org.apache.hadoop.hive.ql.parse.ColumnAccessInfo;
import org.apache.hadoop.hive.ql.parse.JoinType;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.SplitSample;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.GroupByDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.util.AbstractMap;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.table.planner.delegation.hive.HiveParserUtils.generateErrorMessage;
import static org.apache.flink.table.planner.delegation.hive.HiveParserUtils.rewriteGroupingFunctionAST;
import static org.apache.flink.table.planner.delegation.hive.HiveParserUtils.verifyCanHandleAst;
import static org.apache.flink.table.planner.delegation.hive.copy.HiveParserBaseSemanticAnalyzer.addToGBExpr;
import static org.apache.flink.table.planner.delegation.hive.copy.HiveParserBaseSemanticAnalyzer.buildHiveColNameToInputPosMap;
import static org.apache.flink.table.planner.delegation.hive.copy.HiveParserBaseSemanticAnalyzer.buildHiveToCalciteColumnMap;
import static org.apache.flink.table.planner.delegation.hive.copy.HiveParserBaseSemanticAnalyzer.convert;
import static org.apache.flink.table.planner.delegation.hive.copy.HiveParserBaseSemanticAnalyzer.genValues;
import static org.apache.flink.table.planner.delegation.hive.copy.HiveParserBaseSemanticAnalyzer.getBound;
import static org.apache.flink.table.planner.delegation.hive.copy.HiveParserBaseSemanticAnalyzer.getColumnInternalName;
import static org.apache.flink.table.planner.delegation.hive.copy.HiveParserBaseSemanticAnalyzer.getCorrelationUse;
import static org.apache.flink.table.planner.delegation.hive.copy.HiveParserBaseSemanticAnalyzer.getGroupByForClause;
import static org.apache.flink.table.planner.delegation.hive.copy.HiveParserBaseSemanticAnalyzer.getGroupingSets;
import static org.apache.flink.table.planner.delegation.hive.copy.HiveParserBaseSemanticAnalyzer.getGroupingSetsForCube;
import static org.apache.flink.table.planner.delegation.hive.copy.HiveParserBaseSemanticAnalyzer.getGroupingSetsForRollup;
import static org.apache.flink.table.planner.delegation.hive.copy.HiveParserBaseSemanticAnalyzer.getHiveAggInfo;
import static org.apache.flink.table.planner.delegation.hive.copy.HiveParserBaseSemanticAnalyzer.getOrderKeys;
import static org.apache.flink.table.planner.delegation.hive.copy.HiveParserBaseSemanticAnalyzer.getPartitionKeys;
import static org.apache.flink.table.planner.delegation.hive.copy.HiveParserBaseSemanticAnalyzer.getWindowSpecIndx;
import static org.apache.flink.table.planner.delegation.hive.copy.HiveParserBaseSemanticAnalyzer.initPhase1Ctx;
import static org.apache.flink.table.planner.delegation.hive.copy.HiveParserBaseSemanticAnalyzer.processPositionAlias;
import static org.apache.flink.table.planner.delegation.hive.copy.HiveParserBaseSemanticAnalyzer.removeOBInSubQuery;
import static org.apache.flink.table.planner.delegation.hive.copy.HiveParserBaseSemanticAnalyzer.topLevelConjunctCheck;
import static org.apache.flink.table.planner.delegation.hive.copy.HiveParserBaseSemanticAnalyzer.unescapeIdentifier;
import static org.apache.flink.table.planner.delegation.hive.copy.HiveParserBaseSemanticAnalyzer.validateNoHavingReferenceToAlias;

/** Ported Hive's CalcitePlanner. */
public class HiveParserCalcitePlanner {

    private static final Logger LOG = LoggerFactory.getLogger(HiveParserCalcitePlanner.class);

    private final HiveParserSemanticAnalyzer semanticAnalyzer;
    private final CatalogRegistry catalogRegistry;
    private final CalciteCatalogReader catalogReader;
    private final CalciteContext calciteContext;
    private final FrameworkConfig frameworkConfig;
    private final RelOptCluster cluster;
    private final SqlFunctionConverter funcConverter;
    private final LinkedHashMap<RelNode, HiveParserRowResolver> relToRowResolver =
            new LinkedHashMap<>();
    private final LinkedHashMap<RelNode, Map<String, Integer>> relToHiveColNameCalcitePosMap =
            new LinkedHashMap<>();
    private final HiveConf hiveConf;
    // correlated vars across subqueries within same query needs to have different ID
    // this will be used in HiveParserRexNodeConverter to create cor var
    private int subqueryId = 0;

    private HiveParserCreateViewInfo createViewInfo;
    private List<FieldSchema> ctasCols;

    public HiveParserCalcitePlanner(
            HiveParserQueryState queryState,
            CalciteContext calciteContext,
            CalciteCatalogReader catalogReader,
            FrameworkConfig frameworkConfig,
            CatalogRegistry catalogRegistry)
            throws SemanticException {
        this.catalogRegistry = catalogRegistry;
        this.catalogReader = catalogReader;
        this.calciteContext = calciteContext;
        this.frameworkConfig = frameworkConfig;
        this.hiveConf = queryState.getConf();
        this.semanticAnalyzer =
                new HiveParserSemanticAnalyzer(
                        queryState, frameworkConfig, calciteContext.getCluster(), catalogRegistry);
        this.cluster = calciteContext.getCluster();
        this.funcConverter =
                new SqlFunctionConverter(
                        cluster, frameworkConfig.getOperatorTable(), catalogReader.nameMatcher());
    }

    public void setCtasCols(List<FieldSchema> ctasCols) {
        this.ctasCols = ctasCols;
    }

    public void setCreatViewInfo(HiveParserCreateViewInfo createViewInfo) {
        if (createViewInfo != null) {
            semanticAnalyzer.unparseTranslator.enable();
        }
        this.createViewInfo = createViewInfo;
    }

    public void initCtx(HiveParserContext context) {
        semanticAnalyzer.initCtx(context);
    }

    public void init(boolean clearPartsCache) {
        semanticAnalyzer.init(clearPartsCache);
    }

    public HiveParserQB getQB() {
        return semanticAnalyzer.getQB();
    }

    // Given an AST, generate and return the RelNode plan. Returns null if nothing needs to be done.
    public RelNode genLogicalPlan(HiveParserASTNode ast) throws SemanticException {
        LOG.info("Starting generating logical plan");
        HiveParserPreCboCtx cboCtx = new HiveParserPreCboCtx();
        // change the location of position alias process here
        processPositionAlias(ast, semanticAnalyzer.getConf());
        if (!semanticAnalyzer.genResolvedParseTree(ast, cboCtx)) {
            return null;
        }

        // flink requires orderBy removed from sub-queries, otherwise it can fail to generate the
        // plan
        for (String alias : semanticAnalyzer.getQB().getSubqAliases()) {
            removeOBInSubQuery(semanticAnalyzer.getQB().getSubqForAlias(alias));
        }

        HiveParserASTNode queryForCbo = ast;
        if (cboCtx.type == HiveParserPreCboCtx.Type.CTAS
                || cboCtx.type == HiveParserPreCboCtx.Type.VIEW) {
            queryForCbo = cboCtx.nodeOfInterest; // nodeOfInterest is the query
        }
        verifyCanHandleAst(queryForCbo, getQB(), semanticAnalyzer.getQueryProperties());
        semanticAnalyzer.disableJoinMerge = true;
        return logicalPlan();
    }

    private RelNode logicalPlan() {
        if (semanticAnalyzer.columnAccessInfo == null) {
            semanticAnalyzer.columnAccessInfo = new ColumnAccessInfo();
        }
        subqueryId = 0;
        relToRowResolver.clear();
        relToHiveColNameCalcitePosMap.clear();

        try {
            RelNode plan = genLogicalPlan(getQB(), true, null, null);
            if (createViewInfo != null) {
                semanticAnalyzer.resultSchema =
                        HiveParserUtils.convertRowSchemaToResultSetSchema(
                                relToRowResolver.get(plan), false);
                HiveParserUtils.saveViewDefinition(
                        semanticAnalyzer.resultSchema,
                        createViewInfo,
                        semanticAnalyzer.ctx.getTokenRewriteStream(),
                        semanticAnalyzer.unparseTranslator,
                        semanticAnalyzer.getConf());
            } else if (ctasCols != null) {
                // CTAS doesn't allow specifying col list, so we set it according to result schema
                semanticAnalyzer.resultSchema =
                        HiveParserUtils.convertRowSchemaToResultSetSchema(
                                relToRowResolver.get(plan), false);
                ctasCols.addAll(semanticAnalyzer.resultSchema);
            }
            return plan;
        } catch (SemanticException e) {
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("nls")
    private RelNode genSetOpLogicalPlan(
            HiveParserQBExpr.Opcode opcode,
            String alias,
            String leftalias,
            RelNode leftRel,
            String rightalias,
            RelNode rightRel)
            throws SemanticException {
        // 1. Get Row Resolvers, Column map for original left and right input of SetOp Rel
        HiveParserRowResolver leftRR = relToRowResolver.get(leftRel);
        HiveParserRowResolver rightRR = relToRowResolver.get(rightRel);
        HashMap<String, ColumnInfo> leftMap = leftRR.getFieldMap(leftalias);
        HashMap<String, ColumnInfo> rightMap = rightRR.getFieldMap(rightalias);

        // 2. Validate that SetOp is feasible according to Hive (by using type info from RR)
        if (leftMap.size() != rightMap.size()) {
            throw new SemanticException("Schema of both sides of union should match.");
        }

        // 3. construct SetOp Output RR using original left & right Input
        HiveParserRowResolver setOpOutRR = new HiveParserRowResolver();

        Iterator<Map.Entry<String, ColumnInfo>> lIter = leftMap.entrySet().iterator();
        Iterator<Map.Entry<String, ColumnInfo>> rIter = rightMap.entrySet().iterator();
        while (lIter.hasNext()) {
            Map.Entry<String, ColumnInfo> lEntry = lIter.next();
            Map.Entry<String, ColumnInfo> rEntry = rIter.next();
            ColumnInfo lInfo = lEntry.getValue();
            ColumnInfo rInfo = rEntry.getValue();

            String field = lEntry.getKey();
            // try widening conversion, otherwise fail union
            TypeInfo commonTypeInfo =
                    FunctionRegistry.getCommonClassForUnionAll(lInfo.getType(), rInfo.getType());
            if (commonTypeInfo == null) {
                HiveParserASTNode tabRef =
                        getQB().getAliases().isEmpty()
                                ? null
                                : getQB().getParseInfo()
                                        .getSrcForAlias(getQB().getAliases().get(0));
                throw new SemanticException(
                        generateErrorMessage(
                                tabRef,
                                "Schema of both sides of setop should match: Column "
                                        + field
                                        + " is of type "
                                        + lInfo.getType().getTypeName()
                                        + " on first table and type "
                                        + rInfo.getType().getTypeName()
                                        + " on second table"));
            }
            ColumnInfo setOpColInfo = new ColumnInfo(lInfo);
            setOpColInfo.setType(commonTypeInfo);
            setOpOutRR.put(alias, field, setOpColInfo);
        }

        // 4. Determine which columns requires cast on left/right input (Calcite requires exact
        // types on both sides of SetOp)
        boolean leftNeedsTypeCast = false;
        boolean rightNeedsTypeCast = false;
        List<RexNode> leftProjs = new ArrayList<>();
        List<RexNode> rightProjs = new ArrayList<>();
        List<RelDataTypeField> leftFields = leftRel.getRowType().getFieldList();
        List<RelDataTypeField> rightFields = rightRel.getRowType().getFieldList();

        for (int i = 0; i < leftFields.size(); i++) {
            RelDataType leftFieldType = leftFields.get(i).getType();
            RelDataType rightFieldType = rightFields.get(i).getType();
            if (!leftFieldType.equals(rightFieldType)) {
                RelDataType unionFieldType =
                        HiveParserUtils.toRelDataType(
                                setOpOutRR.getColumnInfos().get(i).getType(),
                                cluster.getTypeFactory());
                if (!unionFieldType.equals(leftFieldType)) {
                    leftNeedsTypeCast = true;
                }
                leftProjs.add(
                        cluster.getRexBuilder()
                                .ensureType(
                                        unionFieldType,
                                        cluster.getRexBuilder().makeInputRef(leftFieldType, i),
                                        true));

                if (!unionFieldType.equals(rightFieldType)) {
                    rightNeedsTypeCast = true;
                }
                rightProjs.add(
                        cluster.getRexBuilder()
                                .ensureType(
                                        unionFieldType,
                                        cluster.getRexBuilder().makeInputRef(rightFieldType, i),
                                        true));
            } else {
                leftProjs.add(
                        cluster.getRexBuilder()
                                .ensureType(
                                        leftFieldType,
                                        cluster.getRexBuilder().makeInputRef(leftFieldType, i),
                                        true));
                rightProjs.add(
                        cluster.getRexBuilder()
                                .ensureType(
                                        rightFieldType,
                                        cluster.getRexBuilder().makeInputRef(rightFieldType, i),
                                        true));
            }
        }

        // 5. Introduce Project Rel above original left/right inputs if cast is needed for type
        // parity
        if (leftNeedsTypeCast) {
            leftRel =
                    LogicalProject.create(
                            leftRel,
                            Collections.emptyList(),
                            leftProjs,
                            leftRel.getRowType().getFieldNames());
        }
        if (rightNeedsTypeCast) {
            rightRel =
                    LogicalProject.create(
                            rightRel,
                            Collections.emptyList(),
                            rightProjs,
                            rightRel.getRowType().getFieldNames());
        }

        // 6. Construct SetOp Rel
        List<RelNode> leftAndRight = Arrays.asList(leftRel, rightRel);
        SetOp setOpRel;
        switch (opcode) {
            case UNION:
                setOpRel = LogicalUnion.create(leftAndRight, true);
                break;
            case INTERSECT:
                setOpRel = LogicalIntersect.create(leftAndRight, false);
                break;
            case INTERSECTALL:
                setOpRel = LogicalIntersect.create(leftAndRight, true);
                break;
            case EXCEPT:
                setOpRel = LogicalMinus.create(leftAndRight, false);
                break;
            case EXCEPTALL:
                setOpRel = LogicalMinus.create(leftAndRight, true);
                break;
            default:
                throw new SemanticException("Unsupported set operator " + opcode.toString());
        }
        relToRowResolver.put(setOpRel, setOpOutRR);
        relToHiveColNameCalcitePosMap.put(setOpRel, buildHiveToCalciteColumnMap(setOpOutRR));
        return setOpRel;
    }

    private RelNode genJoinRelNode(
            RelNode leftRel,
            String leftTableAlias,
            RelNode rightRel,
            String rightTableAlias,
            JoinType hiveJoinType,
            HiveParserASTNode joinCondAst)
            throws SemanticException {
        HiveParserRowResolver leftRR = relToRowResolver.get(leftRel);
        HiveParserRowResolver rightRR = relToRowResolver.get(rightRel);

        // 1. Construct ExpressionNodeDesc representing Join Condition
        RexNode joinCondRex;
        List<String> namedColumns = null;
        if (joinCondAst != null) {
            HiveParserJoinTypeCheckCtx jCtx =
                    new HiveParserJoinTypeCheckCtx(
                            leftRR, rightRR, hiveJoinType, frameworkConfig, cluster);
            jCtx.setUnparseTranslator(semanticAnalyzer.unparseTranslator);
            HiveParserRowResolver combinedRR = HiveParserRowResolver.getCombinedRR(leftRR, rightRR);
            if (joinCondAst.getType() == HiveASTParser.TOK_TABCOLNAME
                    && !hiveJoinType.equals(JoinType.LEFTSEMI)) {
                namedColumns = new ArrayList<>();
                // We will transform using clause and make it look like an on-clause.
                // So, lets generate a valid on-clause AST from using.
                HiveParserASTNode and =
                        (HiveParserASTNode)
                                HiveASTParseDriver.ADAPTOR.create(HiveASTParser.KW_AND, "and");
                HiveParserASTNode equal = null;
                int count = 0;
                for (Node child : joinCondAst.getChildren()) {
                    String columnName = ((HiveParserASTNode) child).getText();
                    // dealing with views
                    if (semanticAnalyzer.unparseTranslator != null
                            && semanticAnalyzer.unparseTranslator.isEnabled()) {
                        semanticAnalyzer.unparseTranslator.addIdentifierTranslation(
                                (HiveParserASTNode) child);
                    }
                    namedColumns.add(columnName);
                    HiveParserASTNode left =
                            HiveParserASTBuilder.qualifiedName(leftTableAlias, columnName);
                    HiveParserASTNode right =
                            HiveParserASTBuilder.qualifiedName(rightTableAlias, columnName);
                    equal =
                            (HiveParserASTNode)
                                    HiveASTParseDriver.ADAPTOR.create(HiveASTParser.EQUAL, "=");
                    HiveASTParseDriver.ADAPTOR.addChild(equal, left);
                    HiveASTParseDriver.ADAPTOR.addChild(equal, right);
                    HiveASTParseDriver.ADAPTOR.addChild(and, equal);
                    count++;
                }
                joinCondAst = count > 1 ? and : equal;
            } else if (semanticAnalyzer.unparseTranslator != null
                    && semanticAnalyzer.unparseTranslator.isEnabled()) {
                semanticAnalyzer.genAllExprNodeDesc(joinCondAst, combinedRR, jCtx);
            }
            Map<HiveParserASTNode, ExprNodeDesc> exprNodes =
                    HiveParserUtils.genExprNode(joinCondAst, jCtx);
            if (jCtx.getError() != null) {
                throw new SemanticException(
                        generateErrorMessage(jCtx.getErrorSrcNode(), jCtx.getError()));
            }
            ExprNodeDesc joinCondExprNode = exprNodes.get(joinCondAst);
            List<RelNode> inputRels = new ArrayList<>();
            inputRels.add(leftRel);
            inputRels.add(rightRel);
            joinCondRex =
                    HiveParserRexNodeConverter.convert(
                                    cluster,
                                    joinCondExprNode,
                                    inputRels,
                                    relToRowResolver,
                                    relToHiveColNameCalcitePosMap,
                                    false,
                                    funcConverter)
                            .accept(funcConverter);
        } else {
            joinCondRex = cluster.getRexBuilder().makeLiteral(true);
        }

        // 3. Construct Join Rel Node and HiveParserRowResolver for the new Join Node
        boolean leftSemiJoin = false;
        JoinRelType calciteJoinType;
        switch (hiveJoinType) {
            case LEFTOUTER:
                calciteJoinType = JoinRelType.LEFT;
                break;
            case RIGHTOUTER:
                calciteJoinType = JoinRelType.RIGHT;
                break;
            case FULLOUTER:
                calciteJoinType = JoinRelType.FULL;
                break;
            case LEFTSEMI:
                calciteJoinType = JoinRelType.SEMI;
                leftSemiJoin = true;
                break;
            case INNER:
            default:
                calciteJoinType = JoinRelType.INNER;
                break;
        }

        RelNode topRel;
        HiveParserRowResolver topRR;
        if (leftSemiJoin) {
            List<RelDataTypeField> sysFieldList = new ArrayList<>();
            List<RexNode> leftJoinKeys = new ArrayList<>();
            List<RexNode> rightJoinKeys = new ArrayList<>();

            RexNode nonEquiConds =
                    HiveRelOptUtil.splitHiveJoinCondition(
                            sysFieldList,
                            Arrays.asList(leftRel, rightRel),
                            joinCondRex,
                            Arrays.asList(leftJoinKeys, rightJoinKeys),
                            null,
                            null);

            RelNode[] inputRels = new RelNode[] {leftRel, rightRel};
            final List<Integer> leftKeys = new ArrayList<>();
            final List<Integer> rightKeys = new ArrayList<>();
            RexNode remainingEquiCond =
                    HiveParserUtils.projectNonColumnEquiConditions(
                            RelFactories.DEFAULT_PROJECT_FACTORY,
                            inputRels,
                            leftJoinKeys,
                            rightJoinKeys,
                            0,
                            leftKeys,
                            rightKeys);
            // Adjust right input fields in nonEquiConds if previous call modified the input
            if (inputRels[0] != leftRel) {
                nonEquiConds =
                        RexUtil.shift(
                                nonEquiConds,
                                leftRel.getRowType().getFieldCount(),
                                inputRels[0].getRowType().getFieldCount()
                                        - leftRel.getRowType().getFieldCount());
            }
            joinCondRex =
                    remainingEquiCond != null
                            ? RexUtil.composeConjunction(
                                    cluster.getRexBuilder(),
                                    Arrays.asList(remainingEquiCond, nonEquiConds),
                                    false)
                            : nonEquiConds;
            topRel =
                    LogicalJoin.create(
                            inputRels[0],
                            inputRels[1],
                            Collections.emptyList(),
                            joinCondRex,
                            Collections.emptySet(),
                            calciteJoinType);

            // Create join RR: we need to check whether we need to update left RR in case
            // previous call to projectNonColumnEquiConditions updated it
            if (inputRels[0] != leftRel) {
                HiveParserRowResolver newLeftRR = new HiveParserRowResolver();
                if (!HiveParserRowResolver.add(newLeftRR, leftRR)) {
                    LOG.warn("Duplicates detected when adding columns to RR: see previous message");
                }
                for (int i = leftRel.getRowType().getFieldCount();
                        i < inputRels[0].getRowType().getFieldCount();
                        i++) {
                    ColumnInfo oColInfo =
                            new ColumnInfo(
                                    getColumnInternalName(i),
                                    HiveParserTypeConverter.convert(
                                            inputRels[0]
                                                    .getRowType()
                                                    .getFieldList()
                                                    .get(i)
                                                    .getType()),
                                    null,
                                    false);
                    newLeftRR.put(oColInfo.getTabAlias(), oColInfo.getInternalName(), oColInfo);
                }

                HiveParserRowResolver joinRR = new HiveParserRowResolver();
                if (!HiveParserRowResolver.add(joinRR, newLeftRR)) {
                    LOG.warn("Duplicates detected when adding columns to RR: see previous message");
                }
                relToHiveColNameCalcitePosMap.put(topRel, buildHiveToCalciteColumnMap(joinRR));
                relToRowResolver.put(topRel, joinRR);

                // Introduce top project operator to remove additional column(s) that have been
                // introduced
                List<RexNode> topFields = new ArrayList<>();
                List<String> topFieldNames = new ArrayList<>();
                for (int i = 0; i < leftRel.getRowType().getFieldCount(); i++) {
                    final RelDataTypeField field = leftRel.getRowType().getFieldList().get(i);
                    topFields.add(
                            leftRel.getCluster().getRexBuilder().makeInputRef(field.getType(), i));
                    topFieldNames.add(field.getName());
                }
                topRel =
                        LogicalProject.create(
                                topRel, Collections.emptyList(), topFields, topFieldNames);
            }

            topRR = new HiveParserRowResolver();
            if (!HiveParserRowResolver.add(topRR, leftRR)) {
                LOG.warn("Duplicates detected when adding columns to RR: see previous message");
            }
        } else {
            topRel =
                    LogicalJoin.create(
                            leftRel,
                            rightRel,
                            Collections.emptyList(),
                            joinCondRex,
                            Collections.emptySet(),
                            calciteJoinType);
            topRR = HiveParserRowResolver.getCombinedRR(leftRR, rightRR);
            if (namedColumns != null) {
                List<String> tableAliases = new ArrayList<>();
                tableAliases.add(leftTableAlias);
                tableAliases.add(rightTableAlias);
                topRR.setNamedJoinInfo(
                        new HiveParserNamedJoinInfo(tableAliases, namedColumns, hiveJoinType));
            }
        }

        relToHiveColNameCalcitePosMap.put(topRel, buildHiveToCalciteColumnMap(topRR));
        relToRowResolver.put(topRel, topRR);
        return topRel;
    }

    // Generate Join Logical Plan Relnode by walking through the join AST.
    private RelNode genJoinLogicalPlan(
            HiveParserASTNode joinParseTree, Map<String, RelNode> aliasToRel)
            throws SemanticException {
        RelNode leftRel = null;
        RelNode rightRel = null;
        JoinType hiveJoinType;

        if (joinParseTree.getToken().getType() == HiveASTParser.TOK_UNIQUEJOIN) {
            String msg =
                    "UNIQUE JOIN is currently not supported in CBO, turn off cbo to use UNIQUE JOIN.";
            throw new SemanticException(msg);
        }

        // 1. Determine Join Type
        switch (joinParseTree.getToken().getType()) {
            case HiveASTParser.TOK_LEFTOUTERJOIN:
                hiveJoinType = JoinType.LEFTOUTER;
                break;
            case HiveASTParser.TOK_RIGHTOUTERJOIN:
                hiveJoinType = JoinType.RIGHTOUTER;
                break;
            case HiveASTParser.TOK_FULLOUTERJOIN:
                hiveJoinType = JoinType.FULLOUTER;
                break;
            case HiveASTParser.TOK_LEFTSEMIJOIN:
                hiveJoinType = JoinType.LEFTSEMI;
                break;
            default:
                hiveJoinType = JoinType.INNER;
                break;
        }

        // 2. Get Left Table Alias
        HiveParserASTNode left = (HiveParserASTNode) joinParseTree.getChild(0);
        String leftTableAlias = null;
        if (left.getToken().getType() == HiveASTParser.TOK_TABREF
                || (left.getToken().getType() == HiveASTParser.TOK_SUBQUERY)
                || (left.getToken().getType() == HiveASTParser.TOK_PTBLFUNCTION)) {
            String tableName =
                    HiveParserBaseSemanticAnalyzer.getUnescapedUnqualifiedTableName(
                                    (HiveParserASTNode) left.getChild(0))
                            .toLowerCase();
            leftTableAlias =
                    left.getChildCount() == 1
                            ? tableName
                            : unescapeIdentifier(
                                    left.getChild(left.getChildCount() - 1)
                                            .getText()
                                            .toLowerCase());
            leftTableAlias =
                    left.getToken().getType() == HiveASTParser.TOK_PTBLFUNCTION
                            ? unescapeIdentifier(left.getChild(1).getText().toLowerCase())
                            : leftTableAlias;
            leftRel = aliasToRel.get(leftTableAlias);
        } else if (HiveParserUtils.isJoinToken(left)) {
            leftRel = genJoinLogicalPlan(left, aliasToRel);
        } else {
            assert (false);
        }

        // 3. Get Right Table Alias
        HiveParserASTNode right = (HiveParserASTNode) joinParseTree.getChild(1);
        String rightTableAlias = null;
        if (right.getToken().getType() == HiveASTParser.TOK_TABREF
                || right.getToken().getType() == HiveASTParser.TOK_SUBQUERY
                || right.getToken().getType() == HiveASTParser.TOK_PTBLFUNCTION) {
            String tableName =
                    HiveParserBaseSemanticAnalyzer.getUnescapedUnqualifiedTableName(
                                    (HiveParserASTNode) right.getChild(0))
                            .toLowerCase();
            rightTableAlias =
                    right.getChildCount() == 1
                            ? tableName
                            : unescapeIdentifier(
                                    right.getChild(right.getChildCount() - 1)
                                            .getText()
                                            .toLowerCase());
            rightTableAlias =
                    right.getToken().getType() == HiveASTParser.TOK_PTBLFUNCTION
                            ? unescapeIdentifier(right.getChild(1).getText().toLowerCase())
                            : rightTableAlias;
            rightRel = aliasToRel.get(rightTableAlias);
        } else {
            assert (false);
        }

        // 4. Get Join Condn
        HiveParserASTNode joinCond = (HiveParserASTNode) joinParseTree.getChild(2);

        // 5. Create Join rel
        return genJoinRelNode(
                leftRel, leftTableAlias, rightRel, rightTableAlias, hiveJoinType, joinCond);
    }

    private RelNode genTableLogicalPlan(String tableAlias, HiveParserQB qb)
            throws SemanticException {
        HiveParserRowResolver rowResolver = new HiveParserRowResolver();

        try {
            // 1. If the table has a split sample, and it isn't TABLESAMPLE (n ROWS), throw
            // exception
            // 2. if the table has a bucket sample, throw exception
            SplitSample splitSample = semanticAnalyzer.getNameToSplitSampleMap().get(tableAlias);
            if ((splitSample != null
                            && (splitSample.getPercent() != null
                                    || splitSample.getTotalLength() != null))
                    || qb.getParseInfo().needTableSample(tableAlias)) {
                throw new UnsupportedOperationException("Only TABLESAMPLE (n ROWS) is supported.");
            }

            // 2. Get Table Metadata
            if (qb.getValuesTableToData().containsKey(tableAlias)) {
                // a temp table has been created for VALUES, we need to convert it to LogicalValues
                Tuple2<ResolvedCatalogTable, List<List<String>>> tableValueTuple =
                        qb.getValuesTableToData().get(tableAlias);
                RelNode values =
                        genValues(
                                tableAlias,
                                tableValueTuple.f0,
                                rowResolver,
                                cluster,
                                tableValueTuple.f1);
                relToRowResolver.put(values, rowResolver);
                relToHiveColNameCalcitePosMap.put(values, buildHiveToCalciteColumnMap(rowResolver));
                return values;
            } else {
                // 3. Get Table Logical Schema (Row Type)
                // NOTE: Table logical schema = Non Partition Cols + Partition Cols + Virtual Cols
                Tuple2<String, CatalogTable> nameAndTableTuple =
                        qb.getMetaData().getSrcForAlias(tableAlias);
                String tableName = nameAndTableTuple.f0;
                ResolvedCatalogTable resolvedCatalogTable =
                        (ResolvedCatalogTable) nameAndTableTuple.f1;
                ResolvedSchema resolvedSchema = resolvedCatalogTable.getResolvedSchema();
                String[] fieldNames = resolvedSchema.getColumnNames().toArray(new String[0]);
                ColumnInfo colInfo;
                // 3.1 Add Column info
                for (String fieldName : fieldNames) {
                    Optional<DataType> dataType =
                            resolvedSchema.getColumn(fieldName).map(Column::getDataType);
                    TypeInfo hiveType =
                            HiveTypeUtil.toHiveTypeInfo(
                                    dataType.orElseThrow(
                                            () ->
                                                    new SemanticException(
                                                            String.format(
                                                                    "Can't get data type for column %s of table %s.",
                                                                    fieldName, tableName))),
                                    false);
                    colInfo = new ColumnInfo(fieldName, hiveType, tableAlias, false);
                    colInfo.setSkewedCol(HiveParserUtils.isSkewedCol(tableAlias, qb, fieldName));
                    rowResolver.put(tableAlias, fieldName, colInfo);
                }

                ObjectIdentifier tableIdentifier =
                        HiveParserBaseSemanticAnalyzer.parseCompoundName(
                                catalogRegistry, tableName);

                // Build Hive Table Scan Rel
                RelNode tableRel =
                        catalogReader
                                .getTable(
                                        Arrays.asList(
                                                tableIdentifier.getCatalogName(),
                                                tableIdentifier.getDatabaseName(),
                                                tableIdentifier.getObjectName()))
                                .toRel(
                                        ViewExpanders.toRelContext(
                                                calciteContext.createToRelContext(), cluster));

                if (splitSample != null) {
                    tableRel =
                            LogicalSort.create(
                                    tableRel,
                                    cluster.traitSet().canonize(RelCollations.EMPTY),
                                    null,
                                    cluster.getRexBuilder()
                                            .makeExactLiteral(
                                                    BigDecimal.valueOf(splitSample.getRowCount())));
                }

                // 6. Add Schema(RR) to RelNode-Schema map
                Map<String, Integer> hiveToCalciteColMap = buildHiveToCalciteColumnMap(rowResolver);
                relToRowResolver.put(tableRel, rowResolver);
                relToHiveColNameCalcitePosMap.put(tableRel, hiveToCalciteColMap);
                return tableRel;
            }
        } catch (Exception e) {
            if (e instanceof SemanticException) {
                throw (SemanticException) e;
            } else {
                throw (new RuntimeException(e));
            }
        }
    }

    private RelNode genFilterRelNode(
            HiveParserASTNode filterExpr,
            RelNode srcRel,
            Map<String, Integer> outerNameToPosMap,
            HiveParserRowResolver outerRR,
            boolean useCaching)
            throws SemanticException {
        ExprNodeDesc filterCond =
                semanticAnalyzer.genExprNodeDesc(
                        filterExpr, relToRowResolver.get(srcRel), outerRR, null, useCaching);
        if (filterCond instanceof ExprNodeConstantDesc
                && !filterCond.getTypeString().equals(serdeConstants.BOOLEAN_TYPE_NAME)) {
            throw new SemanticException("Filter expression with non-boolean return type.");
        }
        Map<String, Integer> hiveColNameToCalcitePos = relToHiveColNameCalcitePosMap.get(srcRel);
        RexNode convertedFilterExpr =
                new HiveParserRexNodeConverter(
                                cluster,
                                srcRel.getRowType(),
                                outerNameToPosMap,
                                hiveColNameToCalcitePos,
                                relToRowResolver.get(srcRel),
                                outerRR,
                                0,
                                true,
                                subqueryId,
                                funcConverter)
                        .convert(filterCond);
        RexNode factoredFilterExpr =
                RexUtil.pullFactors(cluster.getRexBuilder(), convertedFilterExpr)
                        .accept(funcConverter);
        RelNode filterRel =
                HiveParserUtils.genFilterRelNode(
                        srcRel,
                        factoredFilterExpr,
                        HiveParserBaseSemanticAnalyzer.getVariablesSetForFilter(
                                factoredFilterExpr));
        relToRowResolver.put(filterRel, relToRowResolver.get(srcRel));
        relToHiveColNameCalcitePosMap.put(filterRel, hiveColNameToCalcitePos);

        return filterRel;
    }

    private void subqueryRestrictionCheck(
            HiveParserQB qb,
            HiveParserASTNode searchCond,
            RelNode srcRel,
            boolean forHavingClause,
            Set<HiveParserASTNode> corrScalarQueries)
            throws SemanticException {
        List<HiveParserASTNode> subQueriesInOriginalTree =
                HiveParserSubQueryUtils.findSubQueries(searchCond);

        HiveParserASTNode clonedSearchCond =
                (HiveParserASTNode) HiveParserSubQueryUtils.ADAPTOR.dupTree(searchCond);
        List<HiveParserASTNode> subQueries =
                HiveParserSubQueryUtils.findSubQueries(clonedSearchCond);
        for (int i = 0; i < subQueriesInOriginalTree.size(); i++) {
            int sqIdx = qb.incrNumSubQueryPredicates();
            HiveParserASTNode originalSubQueryAST = subQueriesInOriginalTree.get(i);

            HiveParserASTNode subQueryAST = subQueries.get(i);
            // HiveParserSubQueryUtils.rewriteParentQueryWhere(clonedSearchCond, subQueryAST);
            ObjectPair<Boolean, Integer> subqInfo = new ObjectPair<>(false, 0);
            if (!topLevelConjunctCheck(clonedSearchCond, subqInfo)) {
                // Restriction.7.h :: SubQuery predicates can appear only as top level conjuncts.
                throw new SemanticException(
                        HiveParserErrorMsg.getMsg(
                                ErrorMsg.UNSUPPORTED_SUBQUERY_EXPRESSION,
                                subQueryAST,
                                "Only SubQuery expressions that are top level conjuncts are allowed"));
            }
            HiveParserASTNode outerQueryExpr = (HiveParserASTNode) subQueryAST.getChild(2);

            if (outerQueryExpr != null
                    && outerQueryExpr.getType() == HiveASTParser.TOK_SUBQUERY_EXPR) {

                throw new SemanticException(
                        HiveParserErrorMsg.getMsg(
                                ErrorMsg.UNSUPPORTED_SUBQUERY_EXPRESSION,
                                outerQueryExpr,
                                "IN/NOT IN subqueries are not allowed in LHS"));
            }

            HiveParserQBSubQuery subQuery =
                    HiveParserSubQueryUtils.buildSubQuery(
                            sqIdx,
                            subQueryAST,
                            originalSubQueryAST,
                            semanticAnalyzer.ctx,
                            frameworkConfig,
                            cluster);

            HiveParserRowResolver inputRR = relToRowResolver.get(srcRel);
            String havingInputAlias = null;
            boolean isCorrScalarWithAgg =
                    subQuery.subqueryRestrictionsCheck(inputRR, forHavingClause, havingInputAlias);
            if (isCorrScalarWithAgg) {
                corrScalarQueries.add(originalSubQueryAST);
            }
        }
    }

    private boolean genSubQueryRelNode(
            HiveParserQB qb,
            HiveParserASTNode node,
            RelNode srcRel,
            boolean forHavingClause,
            Map<HiveParserASTNode, RelNode> subQueryToRelNode)
            throws SemanticException {

        Set<HiveParserASTNode> corrScalarQueriesWithAgg = new HashSet<>();
        // disallow sub-queries which HIVE doesn't currently support
        subqueryRestrictionCheck(qb, node, srcRel, forHavingClause, corrScalarQueriesWithAgg);
        Deque<HiveParserASTNode> stack = new ArrayDeque<>();
        stack.push(node);

        boolean isSubQuery = false;

        while (!stack.isEmpty()) {
            HiveParserASTNode next = stack.pop();

            switch (next.getType()) {
                case HiveASTParser.TOK_SUBQUERY_EXPR:
                    // Restriction 2.h Subquery is not allowed in LHS
                    if (next.getChildren().size() == 3
                            && next.getChild(2).getType() == HiveASTParser.TOK_SUBQUERY_EXPR) {
                        throw new SemanticException(
                                HiveParserErrorMsg.getMsg(
                                        ErrorMsg.UNSUPPORTED_SUBQUERY_EXPRESSION,
                                        next.getChild(2),
                                        "SubQuery in LHS expressions are not supported."));
                    }
                    String sbQueryAlias = "sq_" + qb.incrNumSubQueryPredicates();
                    HiveParserQB subQB = new HiveParserQB(qb.getId(), sbQueryAlias, true);
                    HiveParserBaseSemanticAnalyzer.Phase1Ctx ctx1 = initPhase1Ctx();
                    semanticAnalyzer.doPhase1(
                            (HiveParserASTNode) next.getChild(1), subQB, ctx1, null);
                    semanticAnalyzer.getMetaData(subQB, false);
                    RelNode subQueryRelNode =
                            genLogicalPlan(
                                    subQB,
                                    false,
                                    relToHiveColNameCalcitePosMap.get(srcRel),
                                    relToRowResolver.get(srcRel));
                    subQueryToRelNode.put(next, subQueryRelNode);
                    isSubQuery = true;
                    break;
                default:
                    int childCount = next.getChildCount();
                    for (int i = childCount - 1; i >= 0; i--) {
                        stack.push((HiveParserASTNode) next.getChild(i));
                    }
            }
        }
        return isSubQuery;
    }

    private RelNode genFilterRelNode(
            HiveParserQB qb,
            HiveParserASTNode searchCond,
            RelNode srcRel,
            Map<String, Integer> outerNameToPosMap,
            HiveParserRowResolver outerRR,
            boolean forHavingClause)
            throws SemanticException {

        Map<HiveParserASTNode, RelNode> subQueryToRelNode = new HashMap<>();
        boolean isSubQuery =
                genSubQueryRelNode(qb, searchCond, srcRel, forHavingClause, subQueryToRelNode);
        if (isSubQuery) {
            ExprNodeDesc subQueryExpr =
                    semanticAnalyzer.genExprNodeDesc(
                            searchCond,
                            relToRowResolver.get(srcRel),
                            outerRR,
                            subQueryToRelNode,
                            forHavingClause);

            Map<String, Integer> hiveColNameToCalcitePos =
                    relToHiveColNameCalcitePosMap.get(srcRel);
            RexNode convertedFilterLHS =
                    new HiveParserRexNodeConverter(
                                    cluster,
                                    srcRel.getRowType(),
                                    outerNameToPosMap,
                                    hiveColNameToCalcitePos,
                                    relToRowResolver.get(srcRel),
                                    outerRR,
                                    0,
                                    true,
                                    subqueryId,
                                    funcConverter)
                            .convert(subQueryExpr)
                            .accept(funcConverter);

            RelNode filterRel =
                    HiveParserUtils.genFilterRelNode(
                            srcRel,
                            convertedFilterLHS,
                            HiveParserBaseSemanticAnalyzer.getVariablesSetForFilter(
                                    convertedFilterLHS));

            relToHiveColNameCalcitePosMap.put(filterRel, relToHiveColNameCalcitePosMap.get(srcRel));
            relToRowResolver.put(filterRel, relToRowResolver.get(srcRel));
            subqueryId++;
            return filterRel;
        } else {
            return genFilterRelNode(
                    searchCond, srcRel, outerNameToPosMap, outerRR, forHavingClause);
        }
    }

    private RelNode genFilterLogicalPlan(
            HiveParserQB qb,
            RelNode srcRel,
            Map<String, Integer> outerNameToPosMap,
            HiveParserRowResolver outerRR)
            throws SemanticException {
        RelNode filterRel = null;

        Iterator<HiveParserASTNode> whereClauseIterator =
                qb.getParseInfo().getDestToWhereExpr().values().iterator();
        if (whereClauseIterator.hasNext()) {
            filterRel =
                    genFilterRelNode(
                            qb,
                            (HiveParserASTNode) whereClauseIterator.next().getChild(0),
                            srcRel,
                            outerNameToPosMap,
                            outerRR,
                            false);
        }
        return filterRel;
    }

    private RelNode genGBRelNode(
            List<ExprNodeDesc> gbExprs,
            List<AggInfo> aggInfos,
            List<Integer> groupSets,
            RelNode srcRel)
            throws SemanticException {
        Map<String, Integer> colNameToPos = relToHiveColNameCalcitePosMap.get(srcRel);
        HiveParserRexNodeConverter converter =
                new HiveParserRexNodeConverter(
                        cluster, srcRel.getRowType(), colNameToPos, 0, false, funcConverter);

        final boolean hasGroupSets = groupSets != null && !groupSets.isEmpty();
        final List<RexNode> gbInputRexNodes = new ArrayList<>();
        final HashMap<String, Integer> inputRexNodeToIndex = new HashMap<>();
        final List<Integer> gbKeyIndices = new ArrayList<>();
        int inputIndex = 0;
        for (ExprNodeDesc key : gbExprs) {
            // also convert null literal here to support grouping by NULLs
            RexNode keyRex = convertNullLiteral(converter.convert(key)).accept(funcConverter);
            gbInputRexNodes.add(keyRex);
            gbKeyIndices.add(inputIndex);
            inputRexNodeToIndex.put(keyRex.toString(), inputIndex);
            inputIndex++;
        }
        final ImmutableBitSet groupSet = ImmutableBitSet.of(gbKeyIndices);

        // Grouping sets: we need to transform them into ImmutableBitSet objects for Calcite
        List<ImmutableBitSet> transformedGroupSets = null;
        if (hasGroupSets) {
            Set<ImmutableBitSet> set = CollectionUtil.newHashSetWithExpectedSize(groupSets.size());
            for (int val : groupSets) {
                set.add(convert(val, groupSet.cardinality()));
            }
            // Calcite expects the grouping sets sorted and without duplicates
            transformedGroupSets = new ArrayList<>(set);
            transformedGroupSets.sort(ImmutableBitSet.COMPARATOR);
        }

        // add Agg parameters to inputs
        for (AggInfo aggInfo : aggInfos) {
            for (ExprNodeDesc expr : aggInfo.getAggParams()) {
                RexNode paramRex = converter.convert(expr).accept(funcConverter);
                Integer argIndex = inputRexNodeToIndex.get(paramRex.toString());
                if (argIndex == null) {
                    argIndex = gbInputRexNodes.size();
                    inputRexNodeToIndex.put(paramRex.toString(), argIndex);
                    gbInputRexNodes.add(paramRex);
                }
            }
        }

        // create the actual input before creating agg calls so that the calls can properly infer
        // return type
        RelNode gbInputRel =
                LogicalProject.create(
                        srcRel, Collections.emptyList(), gbInputRexNodes, (List<String>) null);

        List<AggregateCall> aggregateCalls = new ArrayList<>();
        for (AggInfo aggInfo : aggInfos) {
            aggregateCalls.add(
                    HiveParserUtils.toAggCall(
                            aggInfo,
                            converter,
                            inputRexNodeToIndex,
                            groupSet.cardinality(),
                            gbInputRel,
                            cluster,
                            funcConverter));
        }

        // GROUPING__ID is a virtual col in Hive, so we use Flink's function
        if (hasGroupSets) {
            // Create GroupingID column
            AggregateCall aggCall =
                    AggregateCall.create(
                            SqlStdOperatorTable.GROUPING_ID,
                            false,
                            false,
                            false,
                            gbKeyIndices,
                            -1,
                            null,
                            RelCollations.EMPTY,
                            groupSet.cardinality(),
                            gbInputRel,
                            null,
                            null);
            aggregateCalls.add(aggCall);
        }

        if (gbInputRexNodes.isEmpty()) {
            // This will happen for count(*), in such cases we arbitrarily pick
            // first element from srcRel
            gbInputRexNodes.add(cluster.getRexBuilder().makeInputRef(srcRel, 0));
        }

        return LogicalAggregate.create(
                gbInputRel, ImmutableList.of(), groupSet, transformedGroupSets, aggregateCalls);
    }

    // Generate GB plan.
    private RelNode genGBLogicalPlan(HiveParserQB qb, RelNode srcRel) throws SemanticException {
        RelNode gbRel = null;
        HiveParserQBParseInfo qbp = qb.getParseInfo();

        // 1. Gather GB Expressions (AST) (GB + Aggregations)
        // NOTE: Multi Insert is not supported
        String detsClauseName = qbp.getClauseNames().iterator().next();
        HiveParserASTNode selExprList = qb.getParseInfo().getSelForClause(detsClauseName);
        HiveParserSubQueryUtils.checkForTopLevelSubqueries(selExprList);
        if (selExprList.getToken().getType() == HiveASTParser.TOK_SELECTDI
                && selExprList.getChildCount() == 1
                && selExprList.getChild(0).getChildCount() == 1) {
            HiveParserASTNode node = (HiveParserASTNode) selExprList.getChild(0).getChild(0);
            if (node.getToken().getType() == HiveASTParser.TOK_ALLCOLREF) {
                srcRel = genSelectLogicalPlan(qb, srcRel, srcRel, null, null);
                HiveParserRowResolver rr = relToRowResolver.get(srcRel);
                qbp.setSelExprForClause(detsClauseName, HiveParserUtils.genSelectDIAST(rr));
            }
        }

        // Select DISTINCT + windowing; GBy handled by genSelectForWindowing
        if (selExprList.getToken().getType() == HiveASTParser.TOK_SELECTDI
                && !qb.getAllWindowingSpecs().isEmpty()) {
            return null;
        }

        List<HiveParserASTNode> gbAstExprs = getGroupByForClause(qbp, detsClauseName);
        HashMap<String, HiveParserASTNode> aggregationTrees =
                qbp.getAggregationExprsForClause(detsClauseName);
        boolean hasGrpByAstExprs = !gbAstExprs.isEmpty();
        boolean hasAggregationTrees = aggregationTrees != null && !aggregationTrees.isEmpty();

        final boolean cubeRollupGrpSetPresent =
                !qbp.getDestRollups().isEmpty()
                        || !qbp.getDestGroupingSets().isEmpty()
                        || !qbp.getDestCubes().isEmpty();

        // 2. Sanity check
        if (semanticAnalyzer.getConf().getBoolVar(HiveConf.ConfVars.HIVEGROUPBYSKEW)
                && qbp.getDistinctFuncExprsForClause(detsClauseName).size() > 1) {
            throw new SemanticException(ErrorMsg.UNSUPPORTED_MULTIPLE_DISTINCTS.getMsg());
        }

        if (hasGrpByAstExprs || hasAggregationTrees) {
            ArrayList<ExprNodeDesc> gbExprNodeDescs = new ArrayList<>();
            ArrayList<String> outputColNames = new ArrayList<>();

            // 3. Input, Output Row Resolvers
            HiveParserRowResolver inputRR = relToRowResolver.get(srcRel);
            HiveParserRowResolver outputRR = new HiveParserRowResolver();
            outputRR.setIsExprResolver(true);

            if (hasGrpByAstExprs) {
                // 4. Construct GB Keys (ExprNode)
                for (HiveParserASTNode gbAstExpr : gbAstExprs) {
                    Map<HiveParserASTNode, ExprNodeDesc> astToExprNodeDesc =
                            semanticAnalyzer.genAllExprNodeDesc(gbAstExpr, inputRR);
                    ExprNodeDesc grpbyExprNDesc = astToExprNodeDesc.get(gbAstExpr);
                    if (grpbyExprNDesc == null) {
                        throw new SemanticException(
                                "Invalid Column Reference: " + gbAstExpr.dump());
                    }

                    addToGBExpr(
                            outputRR,
                            inputRR,
                            gbAstExpr,
                            grpbyExprNDesc,
                            gbExprNodeDescs,
                            outputColNames);
                }
            }

            // 5. GroupingSets, Cube, Rollup
            int numGroupCols = gbExprNodeDescs.size();
            List<Integer> groupingSets = null;
            if (cubeRollupGrpSetPresent) {
                if (qbp.getDestRollups().contains(detsClauseName)) {
                    groupingSets = getGroupingSetsForRollup(gbAstExprs.size());
                } else if (qbp.getDestCubes().contains(detsClauseName)) {
                    groupingSets = getGroupingSetsForCube(gbAstExprs.size());
                } else if (qbp.getDestGroupingSets().contains(detsClauseName)) {
                    groupingSets = getGroupingSets(gbAstExprs, qbp, detsClauseName);
                }
            }

            // 6. Construct aggregation function Info
            ArrayList<AggInfo> aggInfos = new ArrayList<>();
            if (hasAggregationTrees) {
                for (HiveParserASTNode value : aggregationTrees.values()) {
                    // 6.1 Determine type of UDAF
                    // This is the GenericUDAF name
                    String aggName = unescapeIdentifier(value.getChild(0).getText());
                    boolean isDistinct = value.getType() == HiveASTParser.TOK_FUNCTIONDI;
                    boolean isAllColumns = value.getType() == HiveASTParser.TOK_FUNCTIONSTAR;

                    // 6.2 Convert UDAF Params to ExprNodeDesc
                    ArrayList<ExprNodeDesc> aggParameters = new ArrayList<>();
                    for (int i = 1; i < value.getChildCount(); i++) {
                        HiveParserASTNode paraExpr = (HiveParserASTNode) value.getChild(i);
                        ExprNodeDesc paraExprNode =
                                semanticAnalyzer.genExprNodeDesc(paraExpr, inputRR);
                        aggParameters.add(paraExprNode);
                    }

                    GenericUDAFEvaluator.Mode aggMode =
                            HiveParserUtils.groupByDescModeToUDAFMode(
                                    GroupByDesc.Mode.COMPLETE, isDistinct);
                    GenericUDAFEvaluator genericUDAFEvaluator =
                            HiveParserUtils.getGenericUDAFEvaluator(
                                    aggName,
                                    aggParameters,
                                    value,
                                    isDistinct,
                                    isAllColumns,
                                    frameworkConfig.getOperatorTable());
                    assert (genericUDAFEvaluator != null);
                    HiveParserBaseSemanticAnalyzer.GenericUDAFInfo udaf =
                            HiveParserUtils.getGenericUDAFInfo(
                                    genericUDAFEvaluator, aggMode, aggParameters);
                    String aggAlias = null;
                    if (value.getParent().getType() == HiveASTParser.TOK_SELEXPR
                            && value.getParent().getChildCount() == 2) {
                        aggAlias =
                                unescapeIdentifier(
                                        value.getParent().getChild(1).getText().toLowerCase());
                    }
                    AggInfo aggInfo =
                            new AggInfo(
                                    aggParameters,
                                    udaf.returnType,
                                    aggName,
                                    isDistinct,
                                    isAllColumns,
                                    aggAlias);
                    aggInfos.add(aggInfo);
                    String field =
                            aggAlias == null
                                    ? getColumnInternalName(numGroupCols + aggInfos.size() - 1)
                                    : aggAlias;
                    outputColNames.add(field);
                    outputRR.putExpression(
                            value, new ColumnInfo(field, aggInfo.getReturnType(), "", false));
                }
            }

            // 7. If GroupingSets, Cube, Rollup were used, we account grouping__id
            // GROUPING__ID is also required by the GROUPING function, so let's always add it for
            // grouping sets
            if (groupingSets != null && !groupingSets.isEmpty()) {
                String field = getColumnInternalName(numGroupCols + aggInfos.size());
                outputColNames.add(field);
                outputRR.put(
                        null,
                        VirtualColumn.GROUPINGID.getName(),
                        new ColumnInfo(
                                field,
                                // flink grouping_id's return type is bigint
                                TypeInfoFactory.longTypeInfo,
                                null,
                                true));
            }

            // 8. We create the group_by operator
            gbRel = genGBRelNode(gbExprNodeDescs, aggInfos, groupingSets, srcRel);
            relToHiveColNameCalcitePosMap.put(gbRel, buildHiveToCalciteColumnMap(outputRR));
            relToRowResolver.put(gbRel, outputRR);
        }

        return gbRel;
    }

    // Generate plan for sort by, cluster by and distribute by. This is basically same as generating
    // order by plan.
    // Should refactor to combine them.
    private Pair<RelNode, RelNode> genDistSortBy(
            HiveParserQB qb, RelNode srcRel, boolean outermostOB) throws SemanticException {
        RelNode res = null;
        RelNode originalInput = null;

        HiveParserQBParseInfo qbp = qb.getParseInfo();
        String destClause = qbp.getClauseNames().iterator().next();

        HiveParserASTNode sortAST = qbp.getSortByForClause(destClause);
        HiveParserASTNode distAST = qbp.getDistributeByForClause(destClause);
        HiveParserASTNode clusterAST = qbp.getClusterByForClause(destClause);

        if (sortAST != null || distAST != null || clusterAST != null) {
            List<RexNode> virtualCols = new ArrayList<>();
            List<Pair<HiveParserASTNode, TypeInfo>> vcASTAndType = new ArrayList<>();
            List<RelFieldCollation> fieldCollations = new ArrayList<>();
            List<Integer> distKeys = new ArrayList<>();

            HiveParserRowResolver inputRR = relToRowResolver.get(srcRel);
            HiveParserRexNodeConverter converter =
                    new HiveParserRexNodeConverter(
                            cluster,
                            srcRel.getRowType(),
                            relToHiveColNameCalcitePosMap.get(srcRel),
                            0,
                            false,
                            funcConverter);
            int numSrcFields = srcRel.getRowType().getFieldCount();

            // handle cluster by
            if (clusterAST != null) {
                if (sortAST != null) {
                    throw new SemanticException("Cannot have both CLUSTER BY and SORT BY");
                }
                if (distAST != null) {
                    throw new SemanticException("Cannot have both CLUSTER BY and DISTRIBUTE BY");
                }
                for (Node node : clusterAST.getChildren()) {
                    HiveParserASTNode childAST = (HiveParserASTNode) node;
                    Map<HiveParserASTNode, ExprNodeDesc> astToExprNodeDesc =
                            semanticAnalyzer.genAllExprNodeDesc(childAST, inputRR);
                    ExprNodeDesc childNodeDesc = astToExprNodeDesc.get(childAST);
                    if (childNodeDesc == null) {
                        throw new SemanticException(
                                "Invalid CLUSTER BY expression: " + childAST.toString());
                    }
                    RexNode childRexNode = converter.convert(childNodeDesc).accept(funcConverter);
                    int fieldIndex;
                    if (childRexNode instanceof RexInputRef) {
                        fieldIndex = ((RexInputRef) childRexNode).getIndex();
                    } else {
                        fieldIndex = numSrcFields + virtualCols.size();
                        virtualCols.add(childRexNode);
                        vcASTAndType.add(new Pair<>(childAST, childNodeDesc.getTypeInfo()));
                    }
                    // cluster by doesn't support specifying ASC/DESC or NULLS FIRST/LAST, so use
                    // default values
                    fieldCollations.add(
                            new RelFieldCollation(
                                    fieldIndex,
                                    RelFieldCollation.Direction.ASCENDING,
                                    RelFieldCollation.NullDirection.FIRST));
                    distKeys.add(fieldIndex);
                }
            } else {
                // handle sort by
                if (sortAST != null) {
                    for (Node node : sortAST.getChildren()) {
                        HiveParserASTNode childAST = (HiveParserASTNode) node;
                        HiveParserASTNode nullOrderAST = (HiveParserASTNode) childAST.getChild(0);
                        HiveParserASTNode fieldAST = (HiveParserASTNode) nullOrderAST.getChild(0);
                        Map<HiveParserASTNode, ExprNodeDesc> astToExprNodeDesc =
                                semanticAnalyzer.genAllExprNodeDesc(fieldAST, inputRR);
                        ExprNodeDesc fieldNodeDesc = astToExprNodeDesc.get(fieldAST);
                        if (fieldNodeDesc == null) {
                            throw new SemanticException(
                                    "Invalid sort by expression: " + fieldAST.toString());
                        }
                        RexNode childRexNode =
                                converter.convert(fieldNodeDesc).accept(funcConverter);
                        int fieldIndex;
                        if (childRexNode instanceof RexInputRef) {
                            fieldIndex = ((RexInputRef) childRexNode).getIndex();
                        } else {
                            fieldIndex = numSrcFields + virtualCols.size();
                            virtualCols.add(childRexNode);
                            vcASTAndType.add(new Pair<>(childAST, fieldNodeDesc.getTypeInfo()));
                        }
                        RelFieldCollation.Direction direction =
                                RelFieldCollation.Direction.DESCENDING;
                        if (childAST.getType() == HiveASTParser.TOK_TABSORTCOLNAMEASC) {
                            direction = RelFieldCollation.Direction.ASCENDING;
                        }
                        RelFieldCollation.NullDirection nullOrder;
                        if (nullOrderAST.getType() == HiveASTParser.TOK_NULLS_FIRST) {
                            nullOrder = RelFieldCollation.NullDirection.FIRST;
                        } else if (nullOrderAST.getType() == HiveASTParser.TOK_NULLS_LAST) {
                            nullOrder = RelFieldCollation.NullDirection.LAST;
                        } else {
                            throw new SemanticException(
                                    "Unexpected null ordering option: " + nullOrderAST.getType());
                        }
                        fieldCollations.add(
                                new RelFieldCollation(fieldIndex, direction, nullOrder));
                    }
                }
                // handle distribute by
                if (distAST != null) {
                    for (Node node : distAST.getChildren()) {
                        HiveParserASTNode childAST = (HiveParserASTNode) node;
                        Map<HiveParserASTNode, ExprNodeDesc> astToExprNodeDesc =
                                semanticAnalyzer.genAllExprNodeDesc(childAST, inputRR);
                        ExprNodeDesc childNodeDesc = astToExprNodeDesc.get(childAST);
                        if (childNodeDesc == null) {
                            throw new SemanticException(
                                    "Invalid DISTRIBUTE BY expression: " + childAST.toString());
                        }
                        RexNode childRexNode =
                                converter.convert(childNodeDesc).accept(funcConverter);
                        int fieldIndex;
                        if (childRexNode instanceof RexInputRef) {
                            fieldIndex = ((RexInputRef) childRexNode).getIndex();
                        } else {
                            fieldIndex = numSrcFields + virtualCols.size();
                            virtualCols.add(childRexNode);
                            vcASTAndType.add(new Pair<>(childAST, childNodeDesc.getTypeInfo()));
                        }
                        distKeys.add(fieldIndex);
                    }
                }
            }
            Preconditions.checkState(
                    !fieldCollations.isEmpty() || !distKeys.isEmpty(),
                    "Both field collations and dist keys are empty");

            // add child SEL if needed
            RelNode realInput = srcRel;
            HiveParserRowResolver outputRR = new HiveParserRowResolver();
            if (!virtualCols.isEmpty()) {
                List<RexNode> originalInputRefs =
                        srcRel.getRowType().getFieldList().stream()
                                .map(input -> new RexInputRef(input.getIndex(), input.getType()))
                                .collect(Collectors.toList());
                HiveParserRowResolver addedProjectRR = new HiveParserRowResolver();
                if (!HiveParserRowResolver.add(addedProjectRR, inputRR)) {
                    throw new SemanticException(
                            "Duplicates detected when adding columns to RR: see previous message");
                }
                int vColPos = inputRR.getRowSchema().getSignature().size();
                for (Pair<HiveParserASTNode, TypeInfo> astTypePair : vcASTAndType) {
                    addedProjectRR.putExpression(
                            astTypePair.getKey(),
                            new ColumnInfo(
                                    getColumnInternalName(vColPos),
                                    astTypePair.getValue(),
                                    null,
                                    false));
                    vColPos++;
                }
                realInput =
                        genSelectRelNode(
                                CompositeList.of(originalInputRefs, virtualCols),
                                addedProjectRR,
                                srcRel);

                if (outermostOB) {
                    if (!HiveParserRowResolver.add(outputRR, inputRR)) {
                        throw new SemanticException(
                                "Duplicates detected when adding columns to RR: see previous message");
                    }
                } else {
                    if (!HiveParserRowResolver.add(outputRR, addedProjectRR)) {
                        throw new SemanticException(
                                "Duplicates detected when adding columns to RR: see previous message");
                    }
                }
                originalInput = srcRel;
            } else {
                if (!HiveParserRowResolver.add(outputRR, inputRR)) {
                    throw new SemanticException(
                            "Duplicates detected when adding columns to RR: see previous message");
                }
            }

            // create rel node
            RelTraitSet traitSet = cluster.traitSet();
            RelCollation canonizedCollation = traitSet.canonize(RelCollations.of(fieldCollations));
            res = LogicalDistribution.create(realInput, canonizedCollation, distKeys);

            Map<String, Integer> hiveColNameCalcitePosMap = buildHiveToCalciteColumnMap(outputRR);
            relToRowResolver.put(res, outputRR);
            relToHiveColNameCalcitePosMap.put(res, hiveColNameCalcitePosMap);
        }

        return (new Pair<>(res, originalInput));
    }

    private Pair<Sort, RelNode> genOBLogicalPlan(
            HiveParserQB qb, RelNode srcRel, boolean outermostOB) throws SemanticException {
        Sort sortRel = null;
        RelNode originalOBInput = null;

        HiveParserQBParseInfo qbp = qb.getParseInfo();
        String dest = qbp.getClauseNames().iterator().next();
        HiveParserASTNode obAST = qbp.getOrderByForClause(dest);

        if (obAST != null) {
            // 1. OB Expr sanity test
            // in strict mode, in the presence of order by, limit must be specified
            Integer limit = qb.getParseInfo().getDestLimit(dest);
            if (limit == null) {
                String mapRedMode =
                        semanticAnalyzer.getConf().getVar(HiveConf.ConfVars.HIVEMAPREDMODE);
                boolean banLargeQuery =
                        Boolean.parseBoolean(
                                semanticAnalyzer
                                        .getConf()
                                        .get("hive.strict.checks.large.query", "false"));
                if ("strict".equalsIgnoreCase(mapRedMode) || banLargeQuery) {
                    throw new SemanticException(
                            generateErrorMessage(obAST, "Order by-s without limit"));
                }
            }

            // 2. Walk through OB exprs and extract field collations and additional
            // virtual columns needed
            final List<RexNode> virtualCols = new ArrayList<>();
            final List<RelFieldCollation> fieldCollations = new ArrayList<>();
            int fieldIndex;

            List<Node> obASTExprLst = obAST.getChildren();
            HiveParserASTNode obASTExpr;
            HiveParserASTNode nullOrderASTExpr;
            List<Pair<HiveParserASTNode, TypeInfo>> vcASTAndType = new ArrayList<>();
            HiveParserRowResolver inputRR = relToRowResolver.get(srcRel);
            HiveParserRowResolver outputRR = new HiveParserRowResolver();

            HiveParserRexNodeConverter converter =
                    new HiveParserRexNodeConverter(
                            cluster,
                            srcRel.getRowType(),
                            relToHiveColNameCalcitePosMap.get(srcRel),
                            0,
                            false,
                            funcConverter);
            int numSrcFields = srcRel.getRowType().getFieldCount();

            for (Node node : obASTExprLst) {
                // 2.1 Convert AST Expr to ExprNode
                obASTExpr = (HiveParserASTNode) node;
                nullOrderASTExpr = (HiveParserASTNode) obASTExpr.getChild(0);
                HiveParserASTNode ref = (HiveParserASTNode) nullOrderASTExpr.getChild(0);
                Map<HiveParserASTNode, ExprNodeDesc> astToExprNodeDesc =
                        semanticAnalyzer.genAllExprNodeDesc(ref, inputRR);
                ExprNodeDesc obExprNodeDesc = astToExprNodeDesc.get(ref);
                if (obExprNodeDesc == null) {
                    throw new SemanticException(
                            "Invalid order by expression: " + obASTExpr.toString());
                }

                // 2.2 Convert ExprNode to RexNode
                RexNode rexNode = converter.convert(obExprNodeDesc).accept(funcConverter);

                // 2.3 Determine the index of ob expr in child schema
                // NOTE: Calcite can not take compound exprs in OB without it being
                // present in the child (& hence we add a child Project Rel)
                if (rexNode instanceof RexInputRef) {
                    fieldIndex = ((RexInputRef) rexNode).getIndex();
                } else {
                    fieldIndex = numSrcFields + virtualCols.size();
                    virtualCols.add(rexNode);
                    vcASTAndType.add(new Pair<>(ref, obExprNodeDesc.getTypeInfo()));
                }

                // 2.4 Determine the Direction of order by
                RelFieldCollation.Direction direction = RelFieldCollation.Direction.DESCENDING;
                if (obASTExpr.getType() == HiveASTParser.TOK_TABSORTCOLNAMEASC) {
                    direction = RelFieldCollation.Direction.ASCENDING;
                }
                RelFieldCollation.NullDirection nullOrder;
                if (nullOrderASTExpr.getType() == HiveASTParser.TOK_NULLS_FIRST) {
                    nullOrder = RelFieldCollation.NullDirection.FIRST;
                } else if (nullOrderASTExpr.getType() == HiveASTParser.TOK_NULLS_LAST) {
                    nullOrder = RelFieldCollation.NullDirection.LAST;
                } else {
                    throw new SemanticException(
                            "Unexpected null ordering option: " + nullOrderASTExpr.getType());
                }

                // 2.5 Add to field collations
                fieldCollations.add(new RelFieldCollation(fieldIndex, direction, nullOrder));
            }

            // 3. Add Child Project Rel if needed, Generate Output RR, input Sel Rel
            // for top constraining Sel
            RelNode obInputRel = srcRel;
            if (!virtualCols.isEmpty()) {
                List<RexNode> originalInputRefs =
                        srcRel.getRowType().getFieldList().stream()
                                .map(input -> new RexInputRef(input.getIndex(), input.getType()))
                                .collect(Collectors.toList());
                HiveParserRowResolver obSyntheticProjectRR = new HiveParserRowResolver();
                if (!HiveParserRowResolver.add(obSyntheticProjectRR, inputRR)) {
                    throw new SemanticException(
                            "Duplicates detected when adding columns to RR: see previous message");
                }
                int vcolPos = inputRR.getRowSchema().getSignature().size();
                for (Pair<HiveParserASTNode, TypeInfo> astTypePair : vcASTAndType) {
                    obSyntheticProjectRR.putExpression(
                            astTypePair.getKey(),
                            new ColumnInfo(
                                    getColumnInternalName(vcolPos),
                                    astTypePair.getValue(),
                                    null,
                                    false));
                    vcolPos++;
                }
                obInputRel =
                        genSelectRelNode(
                                CompositeList.of(originalInputRefs, virtualCols),
                                obSyntheticProjectRR,
                                srcRel);

                if (outermostOB) {
                    if (!HiveParserRowResolver.add(outputRR, inputRR)) {
                        throw new SemanticException(
                                "Duplicates detected when adding columns to RR: see previous message");
                    }
                } else {
                    if (!HiveParserRowResolver.add(outputRR, obSyntheticProjectRR)) {
                        throw new SemanticException(
                                "Duplicates detected when adding columns to RR: see previous message");
                    }
                }
                originalOBInput = srcRel;
            } else {
                if (!HiveParserRowResolver.add(outputRR, inputRR)) {
                    throw new SemanticException(
                            "Duplicates detected when adding columns to RR: see previous message");
                }
            }

            // 4. Construct SortRel
            RelTraitSet traitSet = cluster.traitSet();
            RelCollation canonizedCollation = traitSet.canonize(RelCollations.of(fieldCollations));
            sortRel = LogicalSort.create(obInputRel, canonizedCollation, null, null);

            // 5. Update the maps
            Map<String, Integer> hiveColNameCalcitePosMap = buildHiveToCalciteColumnMap(outputRR);
            relToRowResolver.put(sortRel, outputRR);
            relToHiveColNameCalcitePosMap.put(sortRel, hiveColNameCalcitePosMap);
        }

        return (new Pair<>(sortRel, originalOBInput));
    }

    private Sort genLimitLogicalPlan(HiveParserQB qb, RelNode srcRel) throws SemanticException {
        Sort sortRel = null;
        HiveParserQBParseInfo qbp = qb.getParseInfo();
        AbstractMap.SimpleEntry<Integer, Integer> entry =
                qbp.getDestToLimit().get(qbp.getClauseNames().iterator().next());
        int offset = (entry == null) ? 0 : entry.getKey();
        Integer fetch = (entry == null) ? null : entry.getValue();

        if (fetch != null) {
            RexNode offsetRex =
                    cluster.getRexBuilder().makeExactLiteral(BigDecimal.valueOf(offset));
            RexNode fetchRex = cluster.getRexBuilder().makeExactLiteral(BigDecimal.valueOf(fetch));
            RelTraitSet traitSet = cluster.traitSet();
            RelCollation canonizedCollation = traitSet.canonize(RelCollations.EMPTY);
            sortRel = LogicalSort.create(srcRel, canonizedCollation, offsetRex, fetchRex);

            HiveParserRowResolver inputRR = relToRowResolver.get(srcRel);
            HiveParserRowResolver outputRR = inputRR.duplicate();
            Map<String, Integer> hiveColNameCalcitePosMap = buildHiveToCalciteColumnMap(outputRR);
            relToRowResolver.put(sortRel, outputRR);
            relToHiveColNameCalcitePosMap.put(sortRel, hiveColNameCalcitePosMap);
        }
        return sortRel;
    }

    private Pair<RexNode, TypeInfo> getWindowRexAndType(
            HiveParserWindowingSpec.WindowExpressionSpec winExprSpec, RelNode srcRel)
            throws SemanticException {
        RexNode window;

        if (winExprSpec instanceof HiveParserWindowingSpec.WindowFunctionSpec) {
            HiveParserWindowingSpec.WindowFunctionSpec wFnSpec =
                    (HiveParserWindowingSpec.WindowFunctionSpec) winExprSpec;
            HiveParserASTNode windowProjAst = wFnSpec.getExpression();
            // TODO: do we need to get to child?
            int wndSpecASTIndx = getWindowSpecIndx(windowProjAst);
            // 2. Get Hive Aggregate Info
            AggInfo hiveAggInfo =
                    getHiveAggInfo(
                            windowProjAst,
                            wndSpecASTIndx - 1,
                            relToRowResolver.get(srcRel),
                            (HiveParserWindowingSpec.WindowFunctionSpec) winExprSpec,
                            semanticAnalyzer,
                            frameworkConfig,
                            cluster);

            // 3. Get Calcite Return type for Agg Fn
            RelDataType calciteAggFnRetType =
                    HiveParserUtils.toRelDataType(
                            hiveAggInfo.getReturnType(), cluster.getTypeFactory());

            // 4. Convert Agg Fn args to Calcite
            Map<String, Integer> posMap = relToHiveColNameCalcitePosMap.get(srcRel);
            HiveParserRexNodeConverter converter =
                    new HiveParserRexNodeConverter(
                            cluster, srcRel.getRowType(), posMap, 0, false, funcConverter);
            List<RexNode> calciteAggFnArgs = new ArrayList<>();
            List<RelDataType> calciteAggFnArgTypes = new ArrayList<>();
            for (int i = 0; i < hiveAggInfo.getAggParams().size(); i++) {
                calciteAggFnArgs.add(converter.convert(hiveAggInfo.getAggParams().get(i)));
                calciteAggFnArgTypes.add(
                        HiveParserUtils.toRelDataType(
                                hiveAggInfo.getAggParams().get(i).getTypeInfo(),
                                cluster.getTypeFactory()));
            }

            // 5. Get Calcite Agg Fn
            final SqlAggFunction calciteAggFn =
                    HiveParserSqlFunctionConverter.getCalciteAggFn(
                            hiveAggInfo.getUdfName(),
                            hiveAggInfo.isDistinct(),
                            calciteAggFnArgTypes,
                            calciteAggFnRetType);

            // 6. Translate Window spec
            HiveParserRowResolver inputRR = relToRowResolver.get(srcRel);
            HiveParserWindowingSpec.WindowSpec wndSpec =
                    ((HiveParserWindowingSpec.WindowFunctionSpec) winExprSpec).getWindowSpec();
            List<RexNode> partitionKeys =
                    getPartitionKeys(
                            wndSpec.getPartition(),
                            converter,
                            inputRR,
                            new HiveParserTypeCheckCtx(inputRR, frameworkConfig, cluster),
                            semanticAnalyzer);
            List<RexFieldCollation> orderKeys =
                    getOrderKeys(
                            wndSpec.getOrder(),
                            converter,
                            inputRR,
                            new HiveParserTypeCheckCtx(inputRR, frameworkConfig, cluster),
                            semanticAnalyzer);
            RexWindowBound lowerBound = getBound(wndSpec.getWindowFrame().getStart(), cluster);
            RexWindowBound upperBound = getBound(wndSpec.getWindowFrame().getEnd(), cluster);
            boolean isRows =
                    wndSpec.getWindowFrame().getWindowType()
                            == HiveParserWindowingSpec.WindowType.ROWS;

            window =
                    HiveParserUtils.makeOver(
                            cluster.getRexBuilder(),
                            calciteAggFnRetType,
                            calciteAggFn,
                            calciteAggFnArgs,
                            partitionKeys,
                            orderKeys,
                            lowerBound,
                            upperBound,
                            isRows,
                            true,
                            false,
                            false,
                            false);
            window = window.accept(funcConverter);
        } else {
            throw new SemanticException("Unsupported window Spec");
        }

        return new Pair<>(window, HiveParserTypeConverter.convert(window.getType()));
    }

    private RelNode genSelectForWindowing(
            HiveParserQB qb, RelNode srcRel, HashSet<ColumnInfo> newColumns)
            throws SemanticException {
        HiveParserWindowingSpec wSpec =
                !qb.getAllWindowingSpecs().isEmpty()
                        ? qb.getAllWindowingSpecs().values().iterator().next()
                        : null;
        if (wSpec == null) {
            return null;
        }
        // 1. Get valid Window Function Spec
        wSpec.validateAndMakeEffective();
        List<HiveParserWindowingSpec.WindowExpressionSpec> windowExpressions =
                wSpec.getWindowExpressions();
        if (windowExpressions == null || windowExpressions.isEmpty()) {
            return null;
        }

        HiveParserRowResolver inputRR = relToRowResolver.get(srcRel);
        // 2. Get RexNodes for original Projections from below
        List<RexNode> projsForWindowSelOp =
                new ArrayList<>(HiveParserUtils.getProjsFromBelowAsInputRef(srcRel));

        // 3. Construct new Row Resolver with everything from below.
        HiveParserRowResolver outRR = new HiveParserRowResolver();
        if (!HiveParserRowResolver.add(outRR, inputRR)) {
            LOG.warn("Duplicates detected when adding columns to RR: see previous message");
        }

        // 4. Walk through Window Expressions & Construct RexNodes for those. Update out_rwsch
        final HiveParserQBParseInfo qbp = qb.getParseInfo();
        final String selClauseName = qbp.getClauseNames().iterator().next();
        final boolean cubeRollupGrpSetPresent =
                (!qbp.getDestRollups().isEmpty()
                        || !qbp.getDestGroupingSets().isEmpty()
                        || !qbp.getDestCubes().isEmpty());
        for (HiveParserWindowingSpec.WindowExpressionSpec winExprSpec : windowExpressions) {
            if (!qbp.getDestToGroupBy().isEmpty()) {
                // Special handling of grouping function
                winExprSpec.setExpression(
                        rewriteGroupingFunctionAST(
                                getGroupByForClause(qbp, selClauseName),
                                winExprSpec.getExpression(),
                                !cubeRollupGrpSetPresent));
            }
            if (outRR.getExpression(winExprSpec.getExpression()) == null) {
                Pair<RexNode, TypeInfo> rexAndType = getWindowRexAndType(winExprSpec, srcRel);
                projsForWindowSelOp.add(rexAndType.getKey());

                // 6.2.2 Update Output Row Schema
                ColumnInfo oColInfo =
                        new ColumnInfo(
                                getColumnInternalName(projsForWindowSelOp.size()),
                                rexAndType.getValue(),
                                null,
                                false);
                outRR.putExpression(winExprSpec.getExpression(), oColInfo);
                newColumns.add(oColInfo);
            }
        }

        return genSelectRelNode(projsForWindowSelOp, outRR, srcRel, windowExpressions);
    }

    private RelNode genSelectRelNode(
            List<RexNode> calciteColLst, HiveParserRowResolver outRR, RelNode srcRel) {
        return genSelectRelNode(calciteColLst, outRR, srcRel, null);
    }

    private RelNode genSelectRelNode(
            List<RexNode> calciteColLst,
            HiveParserRowResolver outRR,
            RelNode srcRel,
            List<HiveParserWindowingSpec.WindowExpressionSpec> windowExpressions) {
        // 1. Build Column Names
        Set<String> colNames = new HashSet<>();
        List<ColumnInfo> colInfos = outRR.getRowSchema().getSignature();
        ArrayList<String> columnNames = new ArrayList<>();
        Map<String, String> windowToAlias = null;
        if (windowExpressions != null) {
            windowToAlias = new HashMap<>();
            for (HiveParserWindowingSpec.WindowExpressionSpec wes : windowExpressions) {
                windowToAlias.put(wes.getExpression().toStringTree().toLowerCase(), wes.getAlias());
            }
        }
        String[] qualifiedColNames;
        String tmpColAlias;
        for (int i = 0; i < calciteColLst.size(); i++) {
            ColumnInfo cInfo = colInfos.get(i);
            qualifiedColNames = outRR.reverseLookup(cInfo.getInternalName());
            tmpColAlias = qualifiedColNames[1];

            if (tmpColAlias.contains(".") || tmpColAlias.contains(":")) {
                tmpColAlias = cInfo.getInternalName();
            }
            // Prepend column names with '_o_' if it starts with '_c'
            // Hive treats names that start with '_c' as internalNames; so change
            // the names so we don't run into this issue when converting back to Hive AST.
            if (tmpColAlias.startsWith("_c")) {
                tmpColAlias = "_o_" + tmpColAlias;
            } else if (windowToAlias != null && windowToAlias.containsKey(tmpColAlias)) {
                tmpColAlias = windowToAlias.get(tmpColAlias);
            }
            int suffix = 1;
            while (colNames.contains(tmpColAlias)) {
                tmpColAlias = qualifiedColNames[1] + suffix;
                suffix++;
            }

            colNames.add(tmpColAlias);
            columnNames.add(tmpColAlias);
        }

        // 3 Build Calcite Rel Node for project using converted projections & col names
        RelNode selRel =
                LogicalProject.create(srcRel, Collections.emptyList(), calciteColLst, columnNames);

        // 4. Keep track of col name-to-pos map && RR for new select
        relToHiveColNameCalcitePosMap.put(selRel, buildHiveToCalciteColumnMap(outRR));
        relToRowResolver.put(selRel, outRR);

        return selRel;
    }

    // NOTE: there can only be one select clause since we don't handle multi destination insert.
    private RelNode genSelectLogicalPlan(
            HiveParserQB qb,
            RelNode srcRel,
            RelNode starSrcRel,
            Map<String, Integer> outerNameToPos,
            HiveParserRowResolver outerRR)
            throws SemanticException {
        // 0. Generate a Select Node for Windowing
        // Exclude the newly-generated select columns from */etc. resolution.
        HashSet<ColumnInfo> excludedColumns = new HashSet<>();
        RelNode selForWindow = genSelectForWindowing(qb, srcRel, excludedColumns);
        srcRel = (selForWindow == null) ? srcRel : selForWindow;

        ArrayList<ExprNodeDesc> exprNodeDescs = new ArrayList<>();

        HiveParserASTNode trfm = null;

        // 1. Get Select Expression List
        HiveParserQBParseInfo qbp = qb.getParseInfo();
        String selClauseName = qbp.getClauseNames().iterator().next();
        HiveParserASTNode selExprList = qbp.getSelForClause(selClauseName);

        // make sure if there is subquery it is top level expression
        HiveParserSubQueryUtils.checkForTopLevelSubqueries(selExprList);

        final boolean cubeRollupGrpSetPresent =
                !qbp.getDestRollups().isEmpty()
                        || !qbp.getDestGroupingSets().isEmpty()
                        || !qbp.getDestCubes().isEmpty();

        // 3. Query Hints
        int posn = 0;
        boolean hintPresent = selExprList.getChild(0).getType() == HiveASTParser.QUERY_HINT;
        if (hintPresent) {
            posn++;
        }

        // 4. Bailout if select involves Transform
        boolean isInTransform =
                selExprList.getChild(posn).getChild(0).getType() == HiveASTParser.TOK_TRANSFORM;
        if (isInTransform) {
            trfm = (HiveParserASTNode) selExprList.getChild(posn).getChild(0);
        }

        // 2.Row resolvers for input, output
        HiveParserRowResolver outRR = new HiveParserRowResolver();
        // SELECT * or SELECT TRANSFORM(*)
        Integer pos = 0;
        // TODO: will this also fix windowing? try
        HiveParserRowResolver inputRR = relToRowResolver.get(srcRel), starRR = inputRR;
        inputRR.setCheckForAmbiguity(true);
        if (starSrcRel != null) {
            starRR = relToRowResolver.get(starSrcRel);
        }

        // 5. Check if select involves UDTF
        String udtfTableAlias = null;
        SqlOperator udtfOperator = null;
        String genericUDTFName = null;
        ArrayList<String> udtfColAliases = new ArrayList<>();
        HiveParserASTNode expr = (HiveParserASTNode) selExprList.getChild(posn).getChild(0);
        int exprType = expr.getType();
        if (exprType == HiveASTParser.TOK_FUNCTION || exprType == HiveASTParser.TOK_FUNCTIONSTAR) {
            String funcName =
                    HiveParserTypeCheckProcFactory.DefaultExprProcessor.getFunctionText(expr, true);
            // we can't just try to get table function here because the operator table throws
            // exception if it's not a table function
            SqlOperator sqlOperator =
                    HiveParserUtils.getAnySqlOperator(funcName, frameworkConfig.getOperatorTable());
            if (HiveParserUtils.isUDTF(sqlOperator)) {
                LOG.debug("Found UDTF " + funcName);
                udtfOperator = sqlOperator;
                genericUDTFName = funcName;
                if (!HiveParserUtils.isNative(sqlOperator)) {
                    semanticAnalyzer.unparseTranslator.addIdentifierTranslation(
                            (HiveParserASTNode) expr.getChild(0));
                }
                if (exprType == HiveASTParser.TOK_FUNCTIONSTAR) {
                    semanticAnalyzer.genColListRegex(
                            ".*",
                            null,
                            (HiveParserASTNode) expr.getChild(0),
                            exprNodeDescs,
                            null,
                            inputRR,
                            starRR,
                            pos,
                            outRR,
                            qb.getAliases(),
                            false);
                }
            }
        }

        if (udtfOperator != null) {
            // Only support a single expression when it's a UDTF
            if (selExprList.getChildCount() > 1) {
                throw new SemanticException(
                        generateErrorMessage(
                                (HiveParserASTNode) selExprList.getChild(1),
                                ErrorMsg.UDTF_MULTIPLE_EXPR.getMsg()));
            }

            HiveParserASTNode selExpr = (HiveParserASTNode) selExprList.getChild(posn);

            // Get the column / table aliases from the expression. Start from 1 as
            // 0 is the TOK_FUNCTION
            // column names also can be inferred from result of UDTF
            for (int i = 1; i < selExpr.getChildCount(); i++) {
                HiveParserASTNode selExprChild = (HiveParserASTNode) selExpr.getChild(i);
                switch (selExprChild.getType()) {
                    case HiveASTParser.Identifier:
                        udtfColAliases.add(
                                unescapeIdentifier(selExprChild.getText().toLowerCase()));
                        semanticAnalyzer.unparseTranslator.addIdentifierTranslation(selExprChild);
                        break;
                    case HiveASTParser.TOK_TABALIAS:
                        assert (selExprChild.getChildCount() == 1);
                        udtfTableAlias = unescapeIdentifier(selExprChild.getChild(0).getText());
                        qb.addAlias(udtfTableAlias);
                        semanticAnalyzer.unparseTranslator.addIdentifierTranslation(
                                (HiveParserASTNode) selExprChild.getChild(0));
                        break;
                    default:
                        throw new SemanticException(
                                "Find invalid token type " + selExprChild.getType() + " in UDTF.");
                }
            }
            LOG.debug("UDTF table alias is " + udtfTableAlias);
            LOG.debug("UDTF col aliases are " + udtfColAliases);
        }

        // 6. Iterate over all expression (after SELECT)
        HiveParserASTNode exprList;
        if (isInTransform) {
            exprList = (HiveParserASTNode) trfm.getChild(0);
        } else if (udtfOperator != null) {
            exprList = expr;
        } else {
            exprList = selExprList;
        }
        // For UDTF's, skip the function name to get the expressions
        int startPos = udtfOperator != null ? posn + 1 : posn;
        if (isInTransform) {
            startPos = 0;
        }

        // track the col aliases provided by user
        List<String> colAliases = new ArrayList<>();
        for (int i = startPos; i < exprList.getChildCount(); ++i) {
            colAliases.add(null);

            // 6.1 child can be EXPR AS ALIAS, or EXPR.
            HiveParserASTNode child = (HiveParserASTNode) exprList.getChild(i);
            boolean hasAsClause = child.getChildCount() == 2 && !isInTransform;
            boolean isWindowSpec =
                    child.getChildCount() == 3
                            && child.getChild(2).getType() == HiveASTParser.TOK_WINDOWSPEC;

            // 6.2 EXPR AS (ALIAS,...) parses, but is only allowed for UDTF's
            // This check is not needed and invalid when there is a transform b/c the AST's are
            // slightly different.
            if (!isWindowSpec
                    && !isInTransform
                    && udtfOperator == null
                    && child.getChildCount() > 2) {
                throw new SemanticException(
                        generateErrorMessage(
                                (HiveParserASTNode) child.getChild(2),
                                ErrorMsg.INVALID_AS.getMsg()));
            }

            String tabAlias;
            String colAlias;

            if (isInTransform || udtfOperator != null) {
                tabAlias = null;
                colAlias = semanticAnalyzer.getAutogenColAliasPrfxLbl() + i;
                expr = child;
            } else {
                // 6.3 Get rid of TOK_SELEXPR
                expr = (HiveParserASTNode) child.getChild(0);
                String[] colRef =
                        HiveParserUtils.getColAlias(
                                child,
                                semanticAnalyzer.getAutogenColAliasPrfxLbl(),
                                inputRR,
                                semanticAnalyzer.autogenColAliasPrfxIncludeFuncName(),
                                i);
                tabAlias = colRef[0];
                colAlias = colRef[1];
                if (hasAsClause) {
                    colAliases.set(colAliases.size() - 1, colAlias);
                    semanticAnalyzer.unparseTranslator.addIdentifierTranslation(
                            (HiveParserASTNode) child.getChild(1));
                }
            }

            Map<HiveParserASTNode, RelNode> subQueryToRelNode = new HashMap<>();
            boolean isSubQuery = genSubQueryRelNode(qb, expr, srcRel, false, subQueryToRelNode);
            if (isSubQuery) {
                ExprNodeDesc subQueryDesc =
                        semanticAnalyzer.genExprNodeDesc(
                                expr,
                                relToRowResolver.get(srcRel),
                                outerRR,
                                subQueryToRelNode,
                                false);
                exprNodeDescs.add(subQueryDesc);

                ColumnInfo colInfo =
                        new ColumnInfo(
                                getColumnInternalName(pos),
                                subQueryDesc.getWritableObjectInspector(),
                                tabAlias,
                                false);
                if (!outRR.putWithCheck(tabAlias, colAlias, null, colInfo)) {
                    throw new SemanticException(
                            "Cannot add column to RR: "
                                    + tabAlias
                                    + "."
                                    + colAlias
                                    + " => "
                                    + colInfo
                                    + " due to duplication, see previous warnings");
                }
            } else {
                // 6.4 Build ExprNode corresponding to columns
                if (expr.getType() == HiveASTParser.TOK_ALLCOLREF) {
                    pos =
                            semanticAnalyzer.genColListRegex(
                                    ".*",
                                    expr.getChildCount() == 0
                                            ? null
                                            : HiveParserBaseSemanticAnalyzer.getUnescapedName(
                                                            (HiveParserASTNode) expr.getChild(0))
                                                    .toLowerCase(),
                                    expr,
                                    exprNodeDescs,
                                    excludedColumns,
                                    inputRR,
                                    starRR,
                                    pos,
                                    outRR,
                                    qb.getAliases(),
                                    false /* don't require uniqueness */);
                } else if (expr.getType() == HiveASTParser.TOK_TABLE_OR_COL
                        && !hasAsClause
                        && !inputRR.getIsExprResolver()
                        && HiveParserUtils.isRegex(
                                unescapeIdentifier(expr.getChild(0).getText()),
                                semanticAnalyzer.getConf())) {
                    // In case the expression is a regex COL. This can only happen without AS clause
                    // We don't allow this for ExprResolver - the Group By case
                    pos =
                            semanticAnalyzer.genColListRegex(
                                    unescapeIdentifier(expr.getChild(0).getText()),
                                    null,
                                    expr,
                                    exprNodeDescs,
                                    excludedColumns,
                                    inputRR,
                                    starRR,
                                    pos,
                                    outRR,
                                    qb.getAliases(),
                                    true);
                } else if (expr.getType() == HiveASTParser.DOT
                        && expr.getChild(0).getType() == HiveASTParser.TOK_TABLE_OR_COL
                        && inputRR.hasTableAlias(
                                unescapeIdentifier(
                                        expr.getChild(0).getChild(0).getText().toLowerCase()))
                        && !hasAsClause
                        && !inputRR.getIsExprResolver()
                        && HiveParserUtils.isRegex(
                                unescapeIdentifier(expr.getChild(1).getText()),
                                semanticAnalyzer.getConf())) {
                    // In case the expression is TABLE.COL (col can be regex). This can only happen
                    // without AS clause
                    // We don't allow this for ExprResolver - the Group By case
                    pos =
                            semanticAnalyzer.genColListRegex(
                                    unescapeIdentifier(expr.getChild(1).getText()),
                                    unescapeIdentifier(
                                            expr.getChild(0).getChild(0).getText().toLowerCase()),
                                    expr,
                                    exprNodeDescs,
                                    excludedColumns,
                                    inputRR,
                                    starRR,
                                    pos,
                                    outRR,
                                    qb.getAliases(),
                                    false /* don't require uniqueness */);
                } else if (HiveASTParseUtils.containsTokenOfType(expr, HiveASTParser.TOK_FUNCTIONDI)
                        && !(srcRel instanceof Aggregate)) {
                    // Likely a malformed query eg, select hash(distinct c1) from t1;
                    throw new SemanticException("Distinct without an aggregation.");
                } else {
                    // Case when this is an expression
                    HiveParserTypeCheckCtx typeCheckCtx =
                            new HiveParserTypeCheckCtx(
                                    inputRR, true, true, frameworkConfig, cluster);
                    // We allow stateful functions in the SELECT list (but nowhere else)
                    typeCheckCtx.setAllowStatefulFunctions(true);
                    if (!qbp.getDestToGroupBy().isEmpty()) {
                        // Special handling of grouping function
                        expr =
                                rewriteGroupingFunctionAST(
                                        getGroupByForClause(qbp, selClauseName),
                                        expr,
                                        !cubeRollupGrpSetPresent);
                    }
                    ExprNodeDesc exprDesc =
                            semanticAnalyzer.genExprNodeDesc(expr, inputRR, typeCheckCtx);
                    String recommended = semanticAnalyzer.recommendName(exprDesc, colAlias);
                    if (recommended != null && outRR.get(null, recommended) == null) {
                        colAlias = recommended;
                    }
                    exprNodeDescs.add(exprDesc);

                    ColumnInfo colInfo =
                            new ColumnInfo(
                                    getColumnInternalName(pos),
                                    exprDesc.getWritableObjectInspector(),
                                    tabAlias,
                                    false);
                    colInfo.setSkewedCol(
                            exprDesc instanceof ExprNodeColumnDesc
                                    && ((ExprNodeColumnDesc) exprDesc).isSkewedCol());
                    outRR.put(tabAlias, colAlias, colInfo);

                    if (exprDesc instanceof ExprNodeColumnDesc) {
                        ExprNodeColumnDesc colExp = (ExprNodeColumnDesc) exprDesc;
                        String[] altMapping = inputRR.getAlternateMappings(colExp.getColumn());
                        if (altMapping != null) {
                            // TODO: this can overwrite the mapping. Should this be allowed?
                            outRR.put(altMapping[0], altMapping[1], colInfo);
                        }
                    }

                    pos++;
                }
            }
        }
        // 7. Convert Hive projections to Calcite
        List<RexNode> calciteColLst = new ArrayList<>();

        HiveParserRexNodeConverter rexNodeConverter =
                new HiveParserRexNodeConverter(
                        cluster,
                        srcRel.getRowType(),
                        outerNameToPos,
                        buildHiveColNameToInputPosMap(exprNodeDescs, inputRR),
                        relToRowResolver.get(srcRel),
                        outerRR,
                        0,
                        false,
                        subqueryId,
                        funcConverter);
        for (ExprNodeDesc colExpr : exprNodeDescs) {
            RexNode calciteCol = rexNodeConverter.convert(colExpr);
            calciteCol = convertNullLiteral(calciteCol).accept(funcConverter);
            calciteColLst.add(calciteCol);
        }

        // 8. Build Calcite Rel
        RelNode res;
        if (isInTransform) {
            HiveParserScriptTransformHelper transformHelper =
                    new HiveParserScriptTransformHelper(
                            cluster, relToRowResolver, relToHiveColNameCalcitePosMap, hiveConf);
            res = transformHelper.genScriptPlan(trfm, qb, calciteColLst, srcRel);
        } else if (udtfOperator != null) {
            // The basic idea for CBO support of UDTF is to treat UDTF as a special project.
            res =
                    genUDTFPlan(
                            udtfOperator,
                            genericUDTFName,
                            udtfTableAlias,
                            udtfColAliases,
                            qb,
                            calciteColLst,
                            outRR.getColumnInfos(),
                            srcRel,
                            true,
                            false);
        } else {
            // If it's a subquery and the project is identity, we skip creating this project.
            // This is to handle an issue with calcite SubQueryRemoveRule. The rule checks col
            // uniqueness by calling
            // RelMetadataQuery::areColumnsUnique with an empty col set, which always returns null
            // for a project
            // and thus introduces unnecessary agg node.
            if (HiveParserUtils.isIdentityProject(srcRel, calciteColLst, colAliases)
                    && outerRR != null) {
                res = srcRel;
            } else {
                res = genSelectRelNode(calciteColLst, outRR, srcRel);
            }
        }

        // 9. Handle select distinct as GBY if there exist windowing functions
        if (selForWindow != null
                && selExprList.getToken().getType() == HiveASTParser.TOK_SELECTDI) {
            ImmutableBitSet groupSet =
                    ImmutableBitSet.range(res.getRowType().getFieldList().size());
            res =
                    LogicalAggregate.create(
                            res,
                            ImmutableList.of(),
                            groupSet,
                            Collections.emptyList(),
                            Collections.emptyList());
            HiveParserRowResolver groupByOutputRowResolver = new HiveParserRowResolver();
            for (int i = 0; i < outRR.getColumnInfos().size(); i++) {
                ColumnInfo colInfo = outRR.getColumnInfos().get(i);
                ColumnInfo newColInfo =
                        new ColumnInfo(
                                colInfo.getInternalName(),
                                colInfo.getType(),
                                colInfo.getTabAlias(),
                                colInfo.getIsVirtualCol());
                groupByOutputRowResolver.put(colInfo.getTabAlias(), colInfo.getAlias(), newColInfo);
            }
            relToHiveColNameCalcitePosMap.put(
                    res, buildHiveToCalciteColumnMap(groupByOutputRowResolver));
            relToRowResolver.put(res, groupByOutputRowResolver);
        }

        inputRR.setCheckForAmbiguity(false);
        if (selForWindow != null && res instanceof Project) {
            // if exist windowing expression, trim the project node with window
            res =
                    HiveParserProjectWindowTrimmer.trimProjectWindow(
                            (Project) res,
                            (Project) selForWindow,
                            relToRowResolver,
                            relToHiveColNameCalcitePosMap);
        }

        return res;
    }

    // flink doesn't support type NULL, so we need to convert such literals
    private RexNode convertNullLiteral(RexNode rexNode) {
        if (rexNode instanceof RexLiteral) {
            RexLiteral literal = (RexLiteral) rexNode;
            if (literal.isNull() && literal.getTypeName() == SqlTypeName.NULL) {
                return cluster.getRexBuilder()
                        .makeNullLiteral(
                                cluster.getTypeFactory().createSqlType(SqlTypeName.VARCHAR));
            }
        }
        return rexNode;
    }

    private RelNode genUDTFPlan(
            SqlOperator sqlOperator,
            String genericUDTFName,
            String outputTableAlias,
            List<String> colAliases,
            HiveParserQB qb,
            List<RexNode> operands,
            List<ColumnInfo> opColInfos,
            RelNode input,
            boolean inSelect,
            boolean isOuter)
            throws SemanticException {
        Preconditions.checkState(!isOuter || !inSelect, "OUTER is not supported for SELECT UDTF");
        // No GROUP BY / DISTRIBUTE BY / SORT BY / CLUSTER BY
        HiveParserQBParseInfo qbp = qb.getParseInfo();
        if (inSelect && !qbp.getDestToGroupBy().isEmpty()) {
            throw new SemanticException(ErrorMsg.UDTF_NO_GROUP_BY.getMsg());
        }
        if (inSelect && !qbp.getDestToDistributeBy().isEmpty()) {
            throw new SemanticException(ErrorMsg.UDTF_NO_DISTRIBUTE_BY.getMsg());
        }
        if (inSelect && !qbp.getDestToSortBy().isEmpty()) {
            throw new SemanticException(ErrorMsg.UDTF_NO_SORT_BY.getMsg());
        }
        if (inSelect && !qbp.getDestToClusterBy().isEmpty()) {
            throw new SemanticException(ErrorMsg.UDTF_NO_CLUSTER_BY.getMsg());
        }
        if (inSelect && !qbp.getAliasToLateralViews().isEmpty()) {
            throw new SemanticException(ErrorMsg.UDTF_LATERAL_VIEW.getMsg());
        }

        LOG.debug("Table alias: " + outputTableAlias + " Col aliases: " + colAliases);

        // Create the object inspector for the input columns and initialize the UDTF
        RelDataType relDataType =
                HiveParserUtils.inferReturnTypeForOperands(
                        sqlOperator, operands, cluster.getTypeFactory());
        DataType dataType = HiveParserUtils.toDataType(relDataType);
        StructObjectInspector outputOI =
                (StructObjectInspector)
                        HiveInspectors.getObjectInspector(
                                HiveTypeUtil.toHiveTypeInfo(dataType, false));

        // make up a table alias if it's not present, so that we can properly generate a combined RR
        // this should only happen for select udtf
        if (outputTableAlias == null) {
            Preconditions.checkState(inSelect, "Table alias not specified for lateral view");
            String prefix = "select_" + genericUDTFName + "_alias_";
            int i = 0;
            while (qb.getAliases().contains(prefix + i)) {
                i++;
            }
            outputTableAlias = prefix + i;
        }
        if (colAliases.isEmpty()) {
            // user did not specify alias names, infer names from outputOI
            for (StructField field : outputOI.getAllStructFieldRefs()) {
                colAliases.add(field.getFieldName());
            }
        }
        // Make sure that the number of column aliases in the AS clause matches the number of
        // columns output by the UDTF
        int numOutputCols = outputOI.getAllStructFieldRefs().size();
        int numSuppliedAliases = colAliases.size();
        if (numOutputCols != numSuppliedAliases) {
            throw new SemanticException(
                    ErrorMsg.UDTF_ALIAS_MISMATCH.getMsg(
                            "expected "
                                    + numOutputCols
                                    + " aliases "
                                    + "but got "
                                    + numSuppliedAliases));
        }

        // Generate the output column info's / row resolver using internal names.
        ArrayList<ColumnInfo> udtfOutputCols = new ArrayList<>();

        Iterator<String> colAliasesIter = colAliases.iterator();
        for (StructField sf : outputOI.getAllStructFieldRefs()) {
            String colAlias = colAliasesIter.next();
            assert (colAlias != null);

            // Since the UDTF operator feeds into a LVJ operator that will rename all the internal
            // names,
            // we can just use field name from the UDTF's OI as the internal name
            ColumnInfo col =
                    new ColumnInfo(
                            sf.getFieldName(),
                            TypeInfoUtils.getTypeInfoFromObjectInspector(
                                    sf.getFieldObjectInspector()),
                            outputTableAlias,
                            false);
            udtfOutputCols.add(col);
        }

        // Create the row resolver for the table function scan
        HiveParserRowResolver udtfOutRR = new HiveParserRowResolver();
        for (int i = 0; i < udtfOutputCols.size(); i++) {
            udtfOutRR.put(outputTableAlias, colAliases.get(i), udtfOutputCols.get(i));
        }

        // Build row type from field <type, name>
        RelDataType retType = HiveParserTypeConverter.getType(cluster, udtfOutRR, null);

        List<RelDataType> argTypes = new ArrayList<>();

        RelDataTypeFactory dtFactory = cluster.getRexBuilder().getTypeFactory();
        for (ColumnInfo ci : opColInfos) {
            argTypes.add(HiveParserUtils.toRelDataType(ci.getType(), dtFactory));
        }

        SqlOperator calciteOp =
                HiveParserSqlFunctionConverter.getCalciteFn(
                        genericUDTFName, argTypes, retType, false, funcConverter);

        RexNode rexNode = cluster.getRexBuilder().makeCall(calciteOp, operands);

        // convert the rex call
        TableFunctionConverter udtfConverter =
                new TableFunctionConverter(
                        cluster,
                        input,
                        frameworkConfig.getOperatorTable(),
                        catalogReader.nameMatcher());
        RexCall convertedCall = (RexCall) rexNode.accept(udtfConverter);

        SqlOperator convertedOperator = convertedCall.getOperator();
        Preconditions.checkState(
                HiveParserUtils.isBridgingSqlFunction(convertedOperator),
                "Expect operator to be "
                        + HiveParserUtils.BRIDGING_SQL_FUNCTION_CLZ_NAME
                        + ", actually got "
                        + convertedOperator.getClass().getSimpleName());

        // TODO: how to decide this?
        Type elementType = Object[].class;
        // create LogicalTableFunctionScan
        RelNode tableFunctionScan =
                LogicalTableFunctionScan.create(
                        input.getCluster(),
                        Collections.emptyList(),
                        convertedCall,
                        elementType,
                        retType,
                        null);

        // remember the table alias for the UDTF so that we can reference the cols later
        qb.addAlias(outputTableAlias);

        RelNode correlRel;
        RexBuilder rexBuilder = cluster.getRexBuilder();
        // find correlation in the converted call
        Pair<List<CorrelationId>, ImmutableBitSet> correlUse = getCorrelationUse(convertedCall);
        // create correlate node
        if (correlUse == null) {
            correlRel =
                    calciteContext
                            .createRelBuilder()
                            .push(input)
                            .push(tableFunctionScan)
                            .join(
                                    isOuter ? JoinRelType.LEFT : JoinRelType.INNER,
                                    rexBuilder.makeLiteral(true))
                            .build();
        } else {
            if (correlUse.left.size() > 1) {
                tableFunctionScan =
                        DeduplicateCorrelateVariables.go(
                                rexBuilder,
                                correlUse.left.get(0),
                                Util.skip(correlUse.left),
                                tableFunctionScan);
            }
            correlRel =
                    LogicalCorrelate.create(
                            input,
                            tableFunctionScan,
                            correlUse.left.get(0),
                            correlUse.right,
                            isOuter ? JoinRelType.LEFT : JoinRelType.INNER);
        }

        // Add new rel & its RR to the maps
        relToHiveColNameCalcitePosMap.put(
                tableFunctionScan, buildHiveToCalciteColumnMap(udtfOutRR));
        relToRowResolver.put(tableFunctionScan, udtfOutRR);

        HiveParserRowResolver correlRR =
                HiveParserRowResolver.getCombinedRR(
                        relToRowResolver.get(input), relToRowResolver.get(tableFunctionScan));
        relToHiveColNameCalcitePosMap.put(correlRel, buildHiveToCalciteColumnMap(correlRR));
        relToRowResolver.put(correlRel, correlRR);

        if (!inSelect) {
            return correlRel;
        }

        // create project node
        List<RexNode> projects = new ArrayList<>();
        HiveParserRowResolver projectRR = new HiveParserRowResolver();
        int j = 0;
        for (int i = input.getRowType().getFieldCount();
                i < correlRel.getRowType().getFieldCount();
                i++) {
            projects.add(cluster.getRexBuilder().makeInputRef(correlRel, i));
            ColumnInfo inputColInfo = correlRR.getRowSchema().getSignature().get(i);
            String colAlias = inputColInfo.getAlias();
            ColumnInfo colInfo =
                    new ColumnInfo(
                            getColumnInternalName(j++),
                            inputColInfo.getObjectInspector(),
                            null,
                            false);
            projectRR.put(null, colAlias, colInfo);
        }
        RelNode projectNode =
                LogicalProject.create(
                        correlRel,
                        Collections.emptyList(),
                        projects,
                        tableFunctionScan.getRowType());
        relToHiveColNameCalcitePosMap.put(projectNode, buildHiveToCalciteColumnMap(projectRR));
        relToRowResolver.put(projectNode, projectRR);
        return projectNode;
    }

    private RelNode genLogicalPlan(HiveParserQBExpr qbexpr) throws SemanticException {
        switch (qbexpr.getOpcode()) {
            case NULLOP:
                return genLogicalPlan(qbexpr.getQB(), false, null, null);
            case UNION:
            case INTERSECT:
            case INTERSECTALL:
            case EXCEPT:
            case EXCEPTALL:
                RelNode qbexpr1Ops = genLogicalPlan(qbexpr.getQBExpr1());
                RelNode qbexpr2Ops = genLogicalPlan(qbexpr.getQBExpr2());
                return genSetOpLogicalPlan(
                        qbexpr.getOpcode(),
                        qbexpr.getAlias(),
                        qbexpr.getQBExpr1().getAlias(),
                        qbexpr1Ops,
                        qbexpr.getQBExpr2().getAlias(),
                        qbexpr2Ops);
            default:
                return null;
        }
    }

    private RelNode genLogicalPlan(
            HiveParserQB qb,
            boolean outerMostQB,
            Map<String, Integer> outerNameToPosMap,
            HiveParserRowResolver outerRR)
            throws SemanticException {
        RelNode res;
        // First generate all the opInfos for the elements in the from clause
        Map<String, RelNode> aliasToRel = new HashMap<>();
        // 0. Check if we can handle the SubQuery;
        // canHandleQbForCbo returns null if the query can be handled.
        String reason = HiveParserUtils.canHandleQbForCbo(semanticAnalyzer.getQueryProperties());
        if (reason != null) {
            String msg = "CBO can not handle Sub Query" + " because it: " + reason;
            throw new SemanticException(msg);
        }

        // 1. Build Rel For Src (SubQuery, TS, Join)
        // 1.1. Recurse over the subqueries to fill the subquery part of the plan
        for (String subqAlias : qb.getSubqAliases()) {
            HiveParserQBExpr qbexpr = qb.getSubqForAlias(subqAlias);
            RelNode relNode = genLogicalPlan(qbexpr);
            aliasToRel.put(subqAlias, relNode);
            if (qb.getViewToTabSchema().containsKey(subqAlias)) {
                if (!(relNode instanceof Project)) {
                    throw new SemanticException(
                            "View "
                                    + subqAlias
                                    + " is corresponding to "
                                    + relNode.toString()
                                    + ", rather than a Project.");
                }
            }
        }

        // 1.2 Recurse over all the source tables
        for (String tableAlias : qb.getTabAliases()) {
            RelNode op = genTableLogicalPlan(tableAlias, qb);
            aliasToRel.put(tableAlias, op);
        }

        if (aliasToRel.isEmpty()) {
            RelNode dummySrc = LogicalValues.createOneRow(cluster);
            aliasToRel.put(HiveParserSemanticAnalyzer.DUMMY_TABLE, dummySrc);
            HiveParserRowResolver dummyRR = new HiveParserRowResolver();
            dummyRR.put(
                    HiveParserSemanticAnalyzer.DUMMY_TABLE,
                    "dummy_col",
                    new ColumnInfo(
                            getColumnInternalName(0),
                            TypeInfoFactory.intTypeInfo,
                            HiveParserSemanticAnalyzer.DUMMY_TABLE,
                            false));
            relToRowResolver.put(dummySrc, dummyRR);
            relToHiveColNameCalcitePosMap.put(dummySrc, buildHiveToCalciteColumnMap(dummyRR));
        }

        if (!qb.getParseInfo().getAliasToLateralViews().isEmpty()) {
            // process lateral views
            res = genLateralViewPlan(qb, aliasToRel);
        } else if (qb.getParseInfo().getJoinExpr() != null) {
            // 1.3 process join
            res = genJoinLogicalPlan(qb.getParseInfo().getJoinExpr(), aliasToRel);
        } else {
            // If no join then there should only be either 1 TS or 1 SubQuery
            res = aliasToRel.values().iterator().next();
        }

        // 2. Build Rel for where Clause
        RelNode filterRel = genFilterLogicalPlan(qb, res, outerNameToPosMap, outerRR);
        res = (filterRel == null) ? res : filterRel;
        RelNode starSrcRel = res;

        // 3. Build Rel for GB Clause
        RelNode gbRel = genGBLogicalPlan(qb, res);
        res = gbRel == null ? res : gbRel;

        // 4. Build Rel for GB Having Clause
        RelNode gbHavingRel = genGBHavingLogicalPlan(qb, res);
        res = gbHavingRel == null ? res : gbHavingRel;

        // 5. Build Rel for Select Clause
        RelNode selectRel = genSelectLogicalPlan(qb, res, starSrcRel, outerNameToPosMap, outerRR);
        res = selectRel == null ? res : selectRel;

        // 6. Build Rel for OB Clause
        Pair<Sort, RelNode> obAndTopProj = genOBLogicalPlan(qb, res, outerMostQB);
        Sort orderRel = obAndTopProj.getKey();
        RelNode topConstrainingProjRel = obAndTopProj.getValue();
        res = orderRel == null ? res : orderRel;

        // Build Rel for SortBy/ClusterBy/DistributeBy. It can happen only if we don't have OrderBy.
        if (orderRel == null) {
            Pair<RelNode, RelNode> distAndTopProj = genDistSortBy(qb, res, outerMostQB);
            RelNode distRel = distAndTopProj.getKey();
            topConstrainingProjRel = distAndTopProj.getValue();
            res = distRel == null ? res : distRel;
        }

        // 7. Build Rel for Limit Clause
        Sort limitRel = genLimitLogicalPlan(qb, res);
        if (limitRel != null) {
            if (orderRel != null) {
                // merge limit into the order-by node
                HiveParserRowResolver orderRR = relToRowResolver.remove(orderRel);
                Map<String, Integer> orderColNameToPos =
                        relToHiveColNameCalcitePosMap.remove(orderRel);
                res =
                        LogicalSort.create(
                                orderRel.getInput(),
                                orderRel.collation,
                                limitRel.offset,
                                limitRel.fetch);
                relToRowResolver.put(res, orderRR);
                relToHiveColNameCalcitePosMap.put(res, orderColNameToPos);

                relToRowResolver.remove(limitRel);
                relToHiveColNameCalcitePosMap.remove(limitRel);
            } else {
                res = limitRel;
            }
        }

        // 8. Introduce top constraining select if needed.
        if (topConstrainingProjRel != null) {
            List<RexNode> originalInputRefs =
                    topConstrainingProjRel.getRowType().getFieldList().stream()
                            .map(input -> new RexInputRef(input.getIndex(), input.getType()))
                            .collect(Collectors.toList());
            HiveParserRowResolver topConstrainingProjRR = new HiveParserRowResolver();
            if (!HiveParserRowResolver.add(
                    topConstrainingProjRR, relToRowResolver.get(topConstrainingProjRel))) {
                LOG.warn("Duplicates detected when adding columns to RR: see previous message");
            }
            res = genSelectRelNode(originalInputRefs, topConstrainingProjRR, res);
        }

        // 9. In case this HiveParserQB corresponds to subquery then modify its RR to point to
        // subquery alias.
        if (qb.getParseInfo().getAlias() != null) {
            HiveParserRowResolver rr = relToRowResolver.get(res);
            HiveParserRowResolver newRR = new HiveParserRowResolver();
            String alias = qb.getParseInfo().getAlias();
            for (ColumnInfo colInfo : rr.getColumnInfos()) {
                String name = colInfo.getInternalName();
                String[] tmp = rr.reverseLookup(name);
                if ("".equals(tmp[0]) || tmp[1] == null) {
                    // ast expression is not a valid column name for table
                    tmp[1] = colInfo.getInternalName();
                }
                ColumnInfo newColInfo = new ColumnInfo(colInfo);
                newColInfo.setTabAlias(alias);
                newRR.putWithCheck(alias, tmp[1], colInfo.getInternalName(), newColInfo);
            }
            relToRowResolver.put(res, newRR);
            relToHiveColNameCalcitePosMap.put(res, buildHiveToCalciteColumnMap(newRR));
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Created Plan for Query Block " + qb.getId());
        }

        semanticAnalyzer.setQB(qb);
        return res;
    }

    private RelNode genLateralViewPlan(HiveParserQB qb, Map<String, RelNode> aliasToRel)
            throws SemanticException {
        Map<String, ArrayList<HiveParserASTNode>> aliasToLateralViews =
                qb.getParseInfo().getAliasToLateralViews();
        Preconditions.checkArgument(
                aliasToLateralViews.size() == 1, "We only support lateral views for 1 alias");
        Map.Entry<String, ArrayList<HiveParserASTNode>> entry =
                aliasToLateralViews.entrySet().iterator().next();
        String alias = entry.getKey();
        RelNode res = null;
        List<HiveParserASTNode> lateralViews = entry.getValue();
        for (HiveParserASTNode lateralView : lateralViews) {
            Preconditions.checkArgument(lateralView.getChildCount() == 2);
            final boolean isOuter = lateralView.getType() == HiveASTParser.TOK_LATERAL_VIEW_OUTER;
            // this is the 1st lateral view
            if (res == null) {
                // LHS can be table or sub-query
                res = aliasToRel.get(alias);
            }
            Preconditions.checkState(
                    res != null, "Failed to decide LHS table for current lateral view");
            HiveParserRowResolver inputRR = relToRowResolver.get(res);
            HiveParserUtils.LateralViewInfo info =
                    HiveParserUtils.extractLateralViewInfo(
                            lateralView, inputRR, semanticAnalyzer, frameworkConfig, cluster);
            HiveParserRexNodeConverter rexNodeConverter =
                    new HiveParserRexNodeConverter(
                            cluster,
                            res.getRowType(),
                            relToHiveColNameCalcitePosMap.get(res),
                            0,
                            false,
                            funcConverter);
            List<RexNode> operands = new ArrayList<>(info.getOperands().size());
            for (ExprNodeDesc exprDesc : info.getOperands()) {
                operands.add(rexNodeConverter.convert(exprDesc).accept(funcConverter));
            }
            res =
                    genUDTFPlan(
                            info.getSqlOperator(),
                            info.getFuncName(),
                            info.getTabAlias(),
                            info.getColAliases(),
                            qb,
                            operands,
                            info.getOperandColInfos(),
                            res,
                            false,
                            isOuter);
        }
        return res;
    }

    private RelNode genGBHavingLogicalPlan(HiveParserQB qb, RelNode srcRel)
            throws SemanticException {
        RelNode gbFilter = null;
        HiveParserQBParseInfo qbp = qb.getParseInfo();
        String destClauseName = qbp.getClauseNames().iterator().next();
        HiveParserASTNode havingClause =
                qbp.getHavingForClause(qbp.getClauseNames().iterator().next());

        if (havingClause != null) {
            if (!(srcRel instanceof Aggregate)) {
                // ill-formed query like select * from t1 having c1 > 0;
                throw new SemanticException("Having clause without any group-by.");
            }
            HiveParserASTNode targetNode = (HiveParserASTNode) havingClause.getChild(0);
            validateNoHavingReferenceToAlias(
                    qb, targetNode, relToRowResolver.get(srcRel), semanticAnalyzer);
            if (!qbp.getDestToGroupBy().isEmpty()) {
                final boolean cubeRollupGrpSetPresent =
                        (!qbp.getDestRollups().isEmpty()
                                || !qbp.getDestGroupingSets().isEmpty()
                                || !qbp.getDestCubes().isEmpty());
                // Special handling of grouping function
                targetNode =
                        rewriteGroupingFunctionAST(
                                getGroupByForClause(qbp, destClauseName),
                                targetNode,
                                !cubeRollupGrpSetPresent);
            }
            gbFilter = genFilterRelNode(qb, targetNode, srcRel, null, null, true);
        }
        return gbFilter;
    }

    public List<String> getDestSchemaForClause(String clause) {
        return getQB().getParseInfo().getDestSchemaForClause(clause);
    }
}
