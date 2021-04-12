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

import org.apache.flink.table.catalog.hive.client.HiveShim;
import org.apache.flink.table.catalog.hive.util.HiveTableUtil;
import org.apache.flink.table.planner.delegation.hive.HiveParserTypeCheckProcFactory;
import org.apache.flink.table.planner.delegation.hive.HiveParserUtils;
import org.apache.flink.table.planner.delegation.hive.copy.HiveParserBaseSemanticAnalyzer.TableSpec;
import org.apache.flink.table.planner.delegation.hive.copy.HiveParserBaseSemanticAnalyzer.TableSpec.SpecType;
import org.apache.flink.table.planner.delegation.hive.copy.HiveParserPTFInvocationSpec.PTFInputSpec;
import org.apache.flink.table.planner.delegation.hive.copy.HiveParserPTFInvocationSpec.PTFQueryInputSpec;
import org.apache.flink.table.planner.delegation.hive.copy.HiveParserPTFInvocationSpec.PartitionedTableFunctionSpec;
import org.apache.flink.table.planner.delegation.hive.copy.HiveParserPTFInvocationSpec.PartitioningSpec;
import org.apache.flink.table.planner.delegation.hive.copy.HiveParserWindowingSpec.WindowFunctionSpec;
import org.apache.flink.table.planner.delegation.hive.parse.HiveASTParser;
import org.apache.flink.table.planner.delegation.hive.parse.HiveParserDDLSemanticAnalyzer;
import org.apache.flink.table.planner.delegation.hive.parse.HiveParserErrorMsg;

import org.antlr.runtime.ClassicToken;
import org.antlr.runtime.Token;
import org.antlr.runtime.tree.Tree;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.ObjectPair;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.QueryProperties;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.hive.ql.io.RCFileInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.lib.GraphWalker;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveUtils;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ColumnAccessInfo;
import org.apache.hadoop.hive.ql.parse.GlobalLimitCtx;
import org.apache.hadoop.hive.ql.parse.JoinType;
import org.apache.hadoop.hive.ql.parse.PTFInvocationSpec.PTFQueryInputType;
import org.apache.hadoop.hive.ql.parse.PrunedPartitionList;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.SplitSample;
import org.apache.hadoop.hive.ql.plan.CreateTableDesc;
import org.apache.hadoop.hive.ql.plan.CreateViewDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDescUtils;
import org.apache.hadoop.hive.ql.plan.ExprNodeFieldDesc;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.apache.hadoop.hive.ql.plan.PlanUtils;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapred.InputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import static org.apache.flink.table.planner.delegation.hive.copy.HiveParserBaseSemanticAnalyzer.containsLeadLagUDF;
import static org.apache.flink.table.planner.delegation.hive.copy.HiveParserBaseSemanticAnalyzer.doPhase1GetDistinctFuncExprs;
import static org.apache.flink.table.planner.delegation.hive.copy.HiveParserBaseSemanticAnalyzer.findSimpleTableName;
import static org.apache.flink.table.planner.delegation.hive.copy.HiveParserBaseSemanticAnalyzer.findTabRefIdxs;
import static org.apache.flink.table.planner.delegation.hive.copy.HiveParserBaseSemanticAnalyzer.getAliasId;
import static org.apache.flink.table.planner.delegation.hive.copy.HiveParserBaseSemanticAnalyzer.getColumnInternalName;
import static org.apache.flink.table.planner.delegation.hive.copy.HiveParserBaseSemanticAnalyzer.getQualifiedTableName;
import static org.apache.flink.table.planner.delegation.hive.copy.HiveParserBaseSemanticAnalyzer.getUnescapedName;
import static org.apache.flink.table.planner.delegation.hive.copy.HiveParserBaseSemanticAnalyzer.handleQueryWindowClauses;
import static org.apache.flink.table.planner.delegation.hive.copy.HiveParserBaseSemanticAnalyzer.initPhase1Ctx;
import static org.apache.flink.table.planner.delegation.hive.copy.HiveParserBaseSemanticAnalyzer.processPTFPartitionSpec;
import static org.apache.flink.table.planner.delegation.hive.copy.HiveParserBaseSemanticAnalyzer.processWindowFunction;
import static org.apache.flink.table.planner.delegation.hive.copy.HiveParserBaseSemanticAnalyzer.readProps;
import static org.apache.flink.table.planner.delegation.hive.copy.HiveParserBaseSemanticAnalyzer.stripQuotes;
import static org.apache.flink.table.planner.delegation.hive.copy.HiveParserBaseSemanticAnalyzer.unescapeIdentifier;
import static org.apache.flink.table.planner.delegation.hive.copy.HiveParserBaseSemanticAnalyzer.unescapeSQLString;
import static org.apache.flink.table.planner.delegation.hive.copy.HiveParserBaseSemanticAnalyzer.unparseExprForValuesClause;
import static org.apache.flink.table.planner.delegation.hive.copy.HiveParserBaseSemanticAnalyzer.validatePartSpec;

/**
 * Counterpart of hive's org.apache.hadoop.hive.ql.parse.SemanticAnalyzer and adapted to our needs.
 */
public class HiveParserSemanticAnalyzer {

    private static final Logger LOG = LoggerFactory.getLogger(HiveParserSemanticAnalyzer.class);

    public static final String DUMMY_TABLE = "_dummy_table";
    public static final String SUBQUERY_TAG_1 = "-subquery1";
    public static final String SUBQUERY_TAG_2 = "-subquery2";

    // Max characters when auto generating the column name with func name
    public static final int AUTOGEN_COLALIAS_PRFX_MAXLENGTH = 20;

    public static final String VALUES_TMP_TABLE_NAME_PREFIX = "Values__Tmp__Table__";

    private HiveParserQB qb;
    private HiveParserASTNode ast;
    // a map for the split sampling, from alias to an instance of SplitSample that describes
    // percentage and number.
    private final HashMap<String, SplitSample> nameToSplitSample;
    Map<String, PrunedPartitionList> prunedPartitions;
    public List<FieldSchema> resultSchema;
    protected CreateViewDesc createVwDesc;
    protected ArrayList<String> viewsExpanded;
    protected HiveParserASTNode viewSelect;
    public final HiveParserUnparseTranslator unparseTranslator;
    private final GlobalLimitCtx globalLimitCtx;

    // prefix for column names auto generated by hive
    private final String autogenColAliasPrfxLbl;
    private final boolean autogenColAliasPrfxIncludeFuncName;

    // Keep track of view alias to read entity corresponding to the view
    // For eg: for a query like 'select * from V3', where V3 -> V2, V2 -> V1, V1 -> T
    // keeps track of aliases for V3, V3:V2, V3:V2:V1.
    // This is used when T is added as an input for the query, the parents of T is
    // derived from the alias V3:V2:V1:T
    private final Map<String, ReadEntity> viewAliasToInput;

    // need merge isDirect flag to input even if the newInput does not have a parent
    private boolean mergeIsDirect;

    // flag for no scan during analyze ... compute statistics
    protected boolean noscan;

    // flag for partial scan during analyze ... compute statistics
    protected boolean partialscan;

    public volatile boolean disableJoinMerge = false;
    protected final boolean defaultJoinMerge;

    // Capture the CTE definitions in a Query.
    final Map<String, HiveParserBaseSemanticAnalyzer.CTEClause> aliasToCTEs;

    // Used to check recursive CTE invocations. Similar to viewsExpanded
    ArrayList<String> ctesExpanded;

    protected HiveParserBaseSemanticAnalyzer.AnalyzeRewriteContext analyzeRewrite;

    // A mapping from a tableName to a table object in metastore.
    Map<String, Table> tabNameToTabObject;

    public ColumnAccessInfo columnAccessInfo;

    private final HiveConf conf;

    public HiveParserContext ctx;

    QueryProperties queryProperties;

    private final HiveShim hiveShim;

    private final Hive db;

    // ReadEntities that are passed to the hooks.
    protected HashSet<ReadEntity> inputs = new LinkedHashSet<>();

    private final HiveParserQueryState queryState;

    private final FrameworkConfig frameworkConfig;
    private final RelOptCluster cluster;

    public HiveParserSemanticAnalyzer(
            HiveParserQueryState queryState,
            HiveShim hiveShim,
            FrameworkConfig frameworkConfig,
            RelOptCluster cluster)
            throws SemanticException {
        this.queryState = queryState;
        this.conf = queryState.getConf();
        this.hiveShim = hiveShim;
        try {
            this.db = Hive.get(conf);
        } catch (HiveException e) {
            throw new SemanticException(e);
        }
        nameToSplitSample = new HashMap<>();
        prunedPartitions = new HashMap<>();
        tabNameToTabObject = new HashMap<>();
        unparseTranslator = new HiveParserUnparseTranslator(conf);
        autogenColAliasPrfxLbl =
                HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_AUTOGEN_COLUMNALIAS_PREFIX_LABEL);
        autogenColAliasPrfxIncludeFuncName =
                HiveConf.getBoolVar(
                        conf, HiveConf.ConfVars.HIVE_AUTOGEN_COLUMNALIAS_PREFIX_INCLUDEFUNCNAME);
        queryProperties = new QueryProperties();
        aliasToCTEs = new HashMap<>();
        globalLimitCtx = new GlobalLimitCtx();
        viewAliasToInput = new HashMap<>();
        mergeIsDirect = true;
        noscan = partialscan = false;
        tabNameToTabObject = new HashMap<>();
        defaultJoinMerge = !Boolean.parseBoolean(conf.get("hive.merge.nway.joins", "true"));
        disableJoinMerge = defaultJoinMerge;
        this.frameworkConfig = frameworkConfig;
        this.cluster = cluster;
    }

    public HiveConf getConf() {
        return conf;
    }

    public void initCtx(HiveParserContext context) {
        this.ctx = context;
    }

    public QueryProperties getQueryProperties() {
        return queryProperties;
    }

    private void reset(boolean clearPartsCache) {
        if (clearPartsCache) {
            prunedPartitions.clear();
            // When init(true) combine with genResolvedParseTree, it will generate Resolved Parse
            // tree from syntax tree
            // ReadEntity created under these conditions should be all relevant to the syntax tree
            // even the ones without parents
            // set mergeIsDirect to true here.
            mergeIsDirect = true;
        } else {
            mergeIsDirect = false;
        }
        tabNameToTabObject.clear();
        qb = null;
        ast = null;
        disableJoinMerge = defaultJoinMerge;
        aliasToCTEs.clear();
        nameToSplitSample.clear();
        resultSchema = null;
        createVwDesc = null;
        viewsExpanded = null;
        viewSelect = null;
        ctesExpanded = null;
        globalLimitCtx.disableOpt();
        viewAliasToInput.clear();
        unparseTranslator.clear();
        queryProperties.clear();
    }

    public void doPhase1QBExpr(
            HiveParserASTNode ast, HiveParserQBExpr qbexpr, String id, String alias)
            throws SemanticException {
        doPhase1QBExpr(ast, qbexpr, id, alias, false);
    }

    @SuppressWarnings("nls")
    public void doPhase1QBExpr(
            HiveParserASTNode ast,
            HiveParserQBExpr qbexpr,
            String id,
            String alias,
            boolean insideView)
            throws SemanticException {

        assert (ast.getToken() != null);
        if (ast.getToken().getType() == HiveASTParser.TOK_QUERY) {
            HiveParserQB qb = new HiveParserQB(id, alias, true);
            qb.setInsideView(insideView);
            HiveParserBaseSemanticAnalyzer.Phase1Ctx ctx1 = initPhase1Ctx();
            doPhase1(ast, qb, ctx1, null);

            qbexpr.setOpcode(HiveParserQBExpr.Opcode.NULLOP);
            qbexpr.setQB(qb);
        }
        // setop
        else {
            int type = ast.getToken().getType();
            switch (type) {
                case HiveASTParser.TOK_UNIONALL:
                    qbexpr.setOpcode(HiveParserQBExpr.Opcode.UNION);
                    break;
                case HiveASTParser.TOK_INTERSECTALL:
                    qbexpr.setOpcode(HiveParserQBExpr.Opcode.INTERSECTALL);
                    break;
                case HiveASTParser.TOK_INTERSECTDISTINCT:
                    qbexpr.setOpcode(HiveParserQBExpr.Opcode.INTERSECT);
                    break;
                case HiveASTParser.TOK_EXCEPTALL:
                    qbexpr.setOpcode(HiveParserQBExpr.Opcode.EXCEPTALL);
                    break;
                case HiveASTParser.TOK_EXCEPTDISTINCT:
                    qbexpr.setOpcode(HiveParserQBExpr.Opcode.EXCEPT);
                    break;
                default:
                    throw new SemanticException("Unsupported set operator type: " + type);
            }
            // query 1
            assert (ast.getChild(0) != null);
            HiveParserQBExpr qbexpr1 = new HiveParserQBExpr(alias + SUBQUERY_TAG_1);
            doPhase1QBExpr(
                    (HiveParserASTNode) ast.getChild(0),
                    qbexpr1,
                    id + SUBQUERY_TAG_1,
                    alias + SUBQUERY_TAG_1,
                    insideView);
            qbexpr.setQBExpr1(qbexpr1);

            // query 2
            assert (ast.getChild(1) != null);
            HiveParserQBExpr qbexpr2 = new HiveParserQBExpr(alias + SUBQUERY_TAG_2);
            doPhase1QBExpr(
                    (HiveParserASTNode) ast.getChild(1),
                    qbexpr2,
                    id + SUBQUERY_TAG_2,
                    alias + SUBQUERY_TAG_2,
                    insideView);
            qbexpr.setQBExpr2(qbexpr2);
        }
    }

    private LinkedHashMap<String, HiveParserASTNode> doPhase1GetAggregationsFromSelect(
            HiveParserASTNode selExpr, HiveParserQB qb, String dest) throws SemanticException {

        // Iterate over the selects search for aggregation Trees.
        // Use String as keys to eliminate duplicate trees.
        LinkedHashMap<String, HiveParserASTNode> aggregationTrees = new LinkedHashMap<>();
        List<HiveParserASTNode> wdwFns = new ArrayList<>();
        for (int i = 0; i < selExpr.getChildCount(); ++i) {
            HiveParserASTNode function = (HiveParserASTNode) selExpr.getChild(i);
            if (function.getType() == HiveASTParser.TOK_SELEXPR
                    || function.getType() == HiveASTParser.TOK_SUBQUERY_EXPR) {
                function = (HiveParserASTNode) function.getChild(0);
            }
            doPhase1GetAllAggregations(function, aggregationTrees, wdwFns);
        }

        // window based aggregations are handled differently
        for (HiveParserASTNode wdwFn : wdwFns) {
            HiveParserWindowingSpec spec = qb.getWindowingSpec(dest);
            if (spec == null) {
                queryProperties.setHasWindowing(true);
                spec = new HiveParserWindowingSpec();
                qb.addDestToWindowingSpec(dest, spec);
            }
            HashMap<String, HiveParserASTNode> wExprsInDest =
                    qb.getParseInfo().getWindowingExprsForClause(dest);
            int wColIdx =
                    spec.getWindowExpressions() == null ? 0 : spec.getWindowExpressions().size();
            WindowFunctionSpec wFnSpec =
                    processWindowFunction(
                            wdwFn, (HiveParserASTNode) wdwFn.getChild(wdwFn.getChildCount() - 1));
            // If this is a duplicate invocation of a function; don't add to
            // HiveParserWindowingSpec.
            if (wExprsInDest != null
                    && wExprsInDest.containsKey(wFnSpec.getExpression().toStringTree())) {
                continue;
            }
            wFnSpec.setAlias(wFnSpec.getName() + "_window_" + wColIdx);
            spec.addWindowFunction(wFnSpec);
            qb.getParseInfo().addWindowingExprToClause(dest, wFnSpec.getExpression());
        }

        return aggregationTrees;
    }

    private void doPhase1GetColumnAliasesFromSelect(
            HiveParserASTNode selectExpr, HiveParserQBParseInfo qbp) {
        for (int i = 0; i < selectExpr.getChildCount(); ++i) {
            HiveParserASTNode selExpr = (HiveParserASTNode) selectExpr.getChild(i);
            if ((selExpr.getToken().getType() == HiveASTParser.TOK_SELEXPR)
                    && (selExpr.getChildCount() == 2)) {
                String columnAlias = unescapeIdentifier(selExpr.getChild(1).getText());
                qbp.setExprToColumnAlias((HiveParserASTNode) selExpr.getChild(0), columnAlias);
            }
        }
    }

    // DFS-scan the expressionTree to find all aggregation subtrees and put them in aggregations.
    private void doPhase1GetAllAggregations(
            HiveParserASTNode expressionTree,
            HashMap<String, HiveParserASTNode> aggregations,
            List<HiveParserASTNode> wdwFns)
            throws SemanticException {
        int exprTokenType = expressionTree.getToken().getType();
        if (exprTokenType == HiveASTParser.TOK_SUBQUERY_EXPR) {
            // since now we have scalar subqueries we can get subquery expression in having
            // we don't want to include aggregate from within subquery
            return;
        }

        if (exprTokenType == HiveASTParser.TOK_FUNCTION
                || exprTokenType == HiveASTParser.TOK_FUNCTIONDI
                || exprTokenType == HiveASTParser.TOK_FUNCTIONSTAR) {
            assert (expressionTree.getChildCount() != 0);
            if (expressionTree.getChild(expressionTree.getChildCount() - 1).getType()
                    == HiveASTParser.TOK_WINDOWSPEC) {
                // If it is a windowing spec, we include it in the list
                // Further, we will examine its children AST nodes to check whether there are
                // aggregation functions within
                wdwFns.add(expressionTree);
                doPhase1GetAllAggregations(
                        (HiveParserASTNode)
                                expressionTree.getChild(expressionTree.getChildCount() - 1),
                        aggregations,
                        wdwFns);
                return;
            }
            if (expressionTree.getChild(0).getType() == HiveASTParser.Identifier) {
                String functionName = unescapeIdentifier(expressionTree.getChild(0).getText());
                SqlOperator sqlOperator =
                        HiveParserUtils.getAnySqlOperator(
                                functionName, frameworkConfig.getOperatorTable());
                if (sqlOperator == null) {
                    throw new SemanticException(ErrorMsg.INVALID_FUNCTION.getMsg(functionName));
                }
                if (FunctionRegistry.impliesOrder(functionName)) {
                    throw new SemanticException(ErrorMsg.MISSING_OVER_CLAUSE.getMsg(functionName));
                }
                if (HiveParserUtils.isUDAF(sqlOperator)) {
                    if (containsLeadLagUDF(expressionTree)) {
                        throw new SemanticException(
                                ErrorMsg.MISSING_OVER_CLAUSE.getMsg(functionName));
                    }
                    aggregations.put(expressionTree.toStringTree(), expressionTree);
                    if (!HiveParserUtils.isNative(sqlOperator)) {
                        unparseTranslator.addIdentifierTranslation(
                                (HiveParserASTNode) expressionTree.getChild(0));
                    }
                    return;
                }
            }
        }
        for (int i = 0; i < expressionTree.getChildCount(); i++) {
            doPhase1GetAllAggregations(
                    (HiveParserASTNode) expressionTree.getChild(i), aggregations, wdwFns);
        }
    }

    /**
     * Goes though the tabref tree and finds the alias for the table. Once found, it records the
     * table name-> alias association in aliasToTabs. It also makes an association from the alias to
     * the table AST in parse info.
     */
    private String processTable(HiveParserQB qb, HiveParserASTNode tabref)
            throws SemanticException {
        // For each table reference get the table name
        // and the alias (if alias is not present, the table name
        // is used as an alias)
        int[] indexes = findTabRefIdxs(tabref);
        int aliasIndex = indexes[0];
        int propsIndex = indexes[1];
        int tsampleIndex = indexes[2];
        int ssampleIndex = indexes[3];

        HiveParserASTNode tableTree = (HiveParserASTNode) (tabref.getChild(0));

        String tabIdName = getUnescapedName(tableTree).toLowerCase();

        String alias = findSimpleTableName(tabref, aliasIndex);

        if (propsIndex >= 0) {
            Tree propsAST = tabref.getChild(propsIndex);
            Map<String, String> props =
                    HiveParserDDLSemanticAnalyzer.getProps(
                            (HiveParserASTNode) propsAST.getChild(0));
            // We get the information from Calcite.
            if ("TRUE".equals(props.get("insideView"))) {
                qb.getAliasInsideView().add(alias.toLowerCase());
            }
            qb.setTabProps(alias, props);
        }

        // If the alias is already there then we have a conflict
        if (qb.exists(alias)) {
            throw new SemanticException(
                    HiveParserErrorMsg.getMsg(
                            ErrorMsg.AMBIGUOUS_TABLE_ALIAS, tabref.getChild(aliasIndex)));
        }
        if (tsampleIndex >= 0) {
            HiveParserASTNode sampleClause = (HiveParserASTNode) tabref.getChild(tsampleIndex);
            ArrayList<HiveParserASTNode> sampleCols = new ArrayList<>();
            if (sampleClause.getChildCount() > 2) {
                for (int i = 2; i < sampleClause.getChildCount(); i++) {
                    sampleCols.add((HiveParserASTNode) sampleClause.getChild(i));
                }
            }
            // TODO: For now only support sampling on up to two columns
            // Need to change it to list of columns
            if (sampleCols.size() > 2) {
                throw new SemanticException(
                        HiveParserUtils.generateErrorMessage(
                                (HiveParserASTNode) tabref.getChild(0),
                                ErrorMsg.SAMPLE_RESTRICTION.getMsg()));
            }
            qb.getParseInfo().setTabSample(alias);
            if (unparseTranslator.isEnabled()) {
                for (HiveParserASTNode sampleCol : sampleCols) {
                    unparseTranslator.addIdentifierTranslation(
                            (HiveParserASTNode) sampleCol.getChild(0));
                }
            }
        } else if (ssampleIndex >= 0) {
            HiveParserASTNode sampleClause = (HiveParserASTNode) tabref.getChild(ssampleIndex);

            Tree type = sampleClause.getChild(0);
            Tree numerator = sampleClause.getChild(1);
            String value = unescapeIdentifier(numerator.getText());

            SplitSample sample;
            if (type.getType() == HiveASTParser.TOK_PERCENT) {
                double percent = Double.parseDouble(value);
                if (percent < 0 || percent > 100) {
                    throw new SemanticException(
                            HiveParserUtils.generateErrorMessage(
                                    (HiveParserASTNode) numerator,
                                    "Sampling percentage should be between 0 and 100"));
                }
                int seedNum = conf.getIntVar(ConfVars.HIVESAMPLERANDOMNUM);
                sample = new SplitSample(percent, seedNum);
            } else if (type.getType() == HiveASTParser.TOK_ROWCOUNT) {
                sample = new SplitSample(Integer.parseInt(value));
            } else {
                assert type.getType() == HiveASTParser.TOK_LENGTH;
                long length = Integer.parseInt(value.substring(0, value.length() - 1));
                char last = value.charAt(value.length() - 1);
                if (last == 'k' || last == 'K') {
                    length <<= 10;
                } else if (last == 'm' || last == 'M') {
                    length <<= 20;
                } else if (last == 'g' || last == 'G') {
                    length <<= 30;
                }
                int seedNum = conf.getIntVar(ConfVars.HIVESAMPLERANDOMNUM);
                sample = new SplitSample(length, seedNum);
            }
            String aliasId = getAliasId(alias, qb);
            nameToSplitSample.put(aliasId, sample);
        }
        // Insert this map into the stats
        qb.setTabAlias(alias, tabIdName);
        if (qb.isInsideView()) {
            qb.getAliasInsideView().add(alias.toLowerCase());
        }
        qb.addAlias(alias);

        qb.getParseInfo().setSrcForAlias(alias, tableTree);

        // if alias to CTE contains the table name, we do not do the translation because
        // cte is actually a subquery.
        if (!this.aliasToCTEs.containsKey(tabIdName)) {
            unparseTranslator.addTableNameTranslation(
                    tableTree, SessionState.get().getCurrentDatabase());
            if (aliasIndex != 0) {
                unparseTranslator.addIdentifierTranslation(
                        (HiveParserASTNode) tabref.getChild(aliasIndex));
            }
        }

        return alias;
    }

    public Map<String, SplitSample> getNameToSplitSampleMap() {
        return this.nameToSplitSample;
    }

    // Generate a temp table out of a values clause.
    // See also preProcessForInsert(HiveParserASTNode, HiveParserQB)
    private HiveParserASTNode genValuesTempTable(HiveParserASTNode originalFrom, HiveParserQB qb)
            throws SemanticException {
        Path dataDir = null;
        if (!qb.getEncryptedTargetTablePaths().isEmpty()) {
            // currently only Insert into T values(...) is supported thus only 1 values clause
            // and only 1 target table are possible.  If/when support for
            // select ... from values(...) is added an insert statement may have multiple
            // encrypted target tables.
            dataDir = ctx.getMRTmpPath(qb.getEncryptedTargetTablePaths().get(0).toUri());
        }
        // Pick a name for the table
        SessionState ss = SessionState.get();
        String tableName = VALUES_TMP_TABLE_NAME_PREFIX + ss.getNextValuesTempTableSuffix();

        // Step 1, parse the values clause we were handed
        List<? extends Node> fromChildren = originalFrom.getChildren();
        // First child should be the virtual table ref
        HiveParserASTNode virtualTableRef = (HiveParserASTNode) fromChildren.get(0);
        assert virtualTableRef.getToken().getType() == HiveASTParser.TOK_VIRTUAL_TABREF
                : "Expected first child of TOK_VIRTUAL_TABLE to be TOK_VIRTUAL_TABREF but was "
                        + virtualTableRef.getName();

        List<? extends Node> virtualTableRefChildren = virtualTableRef.getChildren();
        // First child of this should be the table name.  If it's anonymous,
        // then we don't have a table name.
        HiveParserASTNode tabName = (HiveParserASTNode) virtualTableRefChildren.get(0);
        if (tabName.getToken().getType() != HiveASTParser.TOK_ANONYMOUS) {
            // TODO, if you want to make select ... from (values(...) as foo(...) work,
            // you need to parse this list of columns names and build it into the table
            throw new SemanticException(ErrorMsg.VALUES_TABLE_CONSTRUCTOR_NOT_SUPPORTED.getMsg());
        }

        // The second child of the TOK_VIRTUAL_TABLE should be TOK_VALUES_TABLE
        HiveParserASTNode valuesTable = (HiveParserASTNode) fromChildren.get(1);
        assert valuesTable.getToken().getType() == HiveASTParser.TOK_VALUES_TABLE
                : "Expected second child of TOK_VIRTUAL_TABLE to be TOK_VALUE_TABLE but was "
                        + valuesTable.getName();
        // Each of the children of TOK_VALUES_TABLE will be a TOK_VALUE_ROW
        List<? extends Node> valuesTableChildren = valuesTable.getChildren();

        // Now that we're going to start reading through the rows, open a file to write the rows too
        // If we leave this method before creating the temporary table we need to be sure to clean
        // up this file.
        Path tablePath = null;
        FileSystem fs = null;
        FSDataOutputStream out = null;
        try {
            if (dataDir == null) {
                tablePath = Warehouse.getDnsPath(new Path(ss.getTempTableSpace(), tableName), conf);
            } else {
                // if target table of insert is encrypted, make sure temporary table data is stored
                // similarly encrypted
                tablePath = Warehouse.getDnsPath(new Path(dataDir, tableName), conf);
            }
            fs = tablePath.getFileSystem(conf);
            fs.mkdirs(tablePath);
            Path dataFile = new Path(tablePath, "data_file");
            out = fs.create(dataFile);
            List<FieldSchema> fields = new ArrayList<>();

            boolean firstRow = true;
            for (Node n : valuesTableChildren) {
                HiveParserASTNode valuesRow = (HiveParserASTNode) n;
                assert valuesRow.getToken().getType() == HiveASTParser.TOK_VALUE_ROW
                        : "Expected child of TOK_VALUE_TABLE to be TOK_VALUE_ROW but was "
                                + valuesRow.getName();
                // Each of the children of this should be a literal
                List<? extends Node> valuesRowChildren = valuesRow.getChildren();
                boolean isFirst = true;
                int nextColNum = 1;
                for (Node n1 : valuesRowChildren) {
                    HiveParserASTNode value = (HiveParserASTNode) n1;
                    if (firstRow) {
                        fields.add(new FieldSchema("tmp_values_col" + nextColNum++, "string", ""));
                    }
                    if (isFirst) {
                        isFirst = false;
                    } else {
                        HiveParserUtils.writeAsText("\u0001", out);
                    }
                    HiveParserUtils.writeAsText(unparseExprForValuesClause(value), out);
                }
                HiveParserUtils.writeAsText("\n", out);
                firstRow = false;
            }

            // Step 2, create a temp table, using the created file as the data
            Table table = db.newTable(tableName);
            table.setSerializationLib(conf.getVar(HiveConf.ConfVars.HIVEDEFAULTSERDE));
            HiveTableUtil.setStorageFormat(table.getSd(), "TextFile", conf);
            table.setFields(fields);
            table.setDataLocation(tablePath);
            table.getTTable().setTemporary(true);
            table.setStoredAsSubDirectories(false);
            db.createTable(table, false);
        } catch (Exception e) {
            String errMsg = ErrorMsg.INSERT_CANNOT_CREATE_TEMP_FILE.getMsg() + e.getMessage();
            LOG.error(errMsg);
            // Try to delete the file
            if (fs != null && tablePath != null) {
                try {
                    fs.delete(tablePath, false);
                } catch (IOException swallowIt) {
                }
            }
            throw new SemanticException(errMsg, e);
        } finally {
            IOUtils.closeStream(out);
        }

        // Step 3, return a new subtree with a from clause built around that temp table
        // The form of the tree is TOK_TABREF->TOK_TABNAME->identifier(tablename)
        Token t = new ClassicToken(HiveASTParser.TOK_TABREF);
        HiveParserASTNode tabRef = new HiveParserASTNode(t);
        t = new ClassicToken(HiveASTParser.TOK_TABNAME);
        HiveParserASTNode tabNameNode = new HiveParserASTNode(t);
        tabRef.addChild(tabNameNode);
        t = new ClassicToken(HiveASTParser.Identifier, tableName);
        HiveParserASTNode identifier = new HiveParserASTNode(t);
        tabNameNode.addChild(identifier);
        return tabRef;
    }

    private String processSubQuery(HiveParserQB qb, HiveParserASTNode subq)
            throws SemanticException {

        // This is a subquery and must have an alias
        if (subq.getChildCount() != 2) {
            throw new SemanticException(
                    HiveParserErrorMsg.getMsg(ErrorMsg.NO_SUBQUERY_ALIAS, subq));
        }
        HiveParserASTNode subqref = (HiveParserASTNode) subq.getChild(0);
        String alias = unescapeIdentifier(subq.getChild(1).getText());

        // Recursively do the first phase of semantic analysis for the subquery
        HiveParserQBExpr qbexpr = new HiveParserQBExpr(alias);

        doPhase1QBExpr(subqref, qbexpr, qb.getId(), alias, qb.isInsideView());

        // If the alias is already there then we have a conflict
        if (qb.exists(alias)) {
            throw new SemanticException(
                    HiveParserErrorMsg.getMsg(ErrorMsg.AMBIGUOUS_TABLE_ALIAS, subq.getChild(1)));
        }
        // Insert this map into the stats
        qb.setSubqAlias(alias, qbexpr);
        qb.addAlias(alias);

        unparseTranslator.addIdentifierTranslation((HiveParserASTNode) subq.getChild(1));

        return alias;
    }

    /*
     * Phase1: hold onto any CTE definitions in aliasToCTE.
     * CTE definitions are global to the Query.
     */
    private void processCTE(HiveParserQB qb, HiveParserASTNode ctes) throws SemanticException {

        int numCTEs = ctes.getChildCount();

        for (int i = 0; i < numCTEs; i++) {
            HiveParserASTNode cte = (HiveParserASTNode) ctes.getChild(i);
            HiveParserASTNode cteQry = (HiveParserASTNode) cte.getChild(0);
            String alias = unescapeIdentifier(cte.getChild(1).getText());

            String qName = qb.getId() == null ? "" : qb.getId() + ":";
            qName += alias.toLowerCase();

            if (aliasToCTEs.containsKey(qName)) {
                throw new SemanticException(
                        HiveParserErrorMsg.getMsg(ErrorMsg.AMBIGUOUS_TABLE_ALIAS, cte.getChild(1)));
            }
            aliasToCTEs.put(qName, new HiveParserBaseSemanticAnalyzer.CTEClause(qName, cteQry));
        }
    }

    /*
     * We allow CTE definitions in views. So we can end up with a hierarchy of CTE definitions:
     * - at the top level of a query statement
     * - where a view is referenced.
     * - views may refer to other views.
     *
     * The scoping rules we use are: to search for a CTE from the current HiveParserQB outwards. In order to
     * disambiguate between CTES are different levels we qualify(prefix) them with the id of the HiveParserQB
     * they appear in when adding them to the <code>aliasToCTEs</code> map.
     *
     */
    private HiveParserBaseSemanticAnalyzer.CTEClause findCTEFromName(
            HiveParserQB qb, String cteName) {
        StringBuilder qId = new StringBuilder();
        if (qb.getId() != null) {
            qId.append(qb.getId());
        }

        while (qId.length() > 0) {
            String nm = qId + ":" + cteName;
            HiveParserBaseSemanticAnalyzer.CTEClause cte = aliasToCTEs.get(nm);
            if (cte != null) {
                return cte;
            }
            int lastIndex = qId.lastIndexOf(":");
            lastIndex = Math.max(lastIndex, 0);
            qId.setLength(lastIndex);
        }
        return aliasToCTEs.get(cteName);
    }

    /*
     * If a CTE is referenced in a QueryBlock:
     * - add it as a SubQuery for now.
     *   - SQ.alias is the alias used in HiveParserQB. (if no alias is specified,
     *     it used the CTE name. Works just like table references)
     *   - Adding SQ done by:
     *     - copying AST of CTE
     *     - setting ASTOrigin on cloned AST.
     *   - trigger phase 1 on new HiveParserQBExpr.
     *   - update HiveParserQB data structs: remove this as a table reference, move it to a SQ invocation.
     */
    private void addCTEAsSubQuery(HiveParserQB qb, String cteName, String cteAlias)
            throws SemanticException {
        cteAlias = cteAlias == null ? cteName : cteAlias;
        HiveParserBaseSemanticAnalyzer.CTEClause cte = findCTEFromName(qb, cteName);
        HiveParserASTNode cteQryNode = cte.cteNode;
        HiveParserQBExpr cteQBExpr = new HiveParserQBExpr(cteAlias);
        doPhase1QBExpr(cteQryNode, cteQBExpr, qb.getId(), cteAlias);
        qb.rewriteCTEToSubq(cteAlias, cteName, cteQBExpr);
    }

    private final HiveParserBaseSemanticAnalyzer.CTEClause rootClause =
            new HiveParserBaseSemanticAnalyzer.CTEClause(null, null);

    /**
     * Given the AST with TOK_JOIN as the root, get all the aliases for the tables or subqueries in
     * the join.
     */
    @SuppressWarnings("nls")
    private void processJoin(HiveParserQB qb, HiveParserASTNode join) throws SemanticException {
        int numChildren = join.getChildCount();
        if ((numChildren != 2)
                && (numChildren != 3)
                && join.getToken().getType() != HiveASTParser.TOK_UNIQUEJOIN) {
            throw new SemanticException(
                    HiveParserUtils.generateErrorMessage(join, "Join with multiple children"));
        }

        queryProperties.incrementJoinCount(HiveParserUtils.isOuterJoinToken(join));
        for (int num = 0; num < numChildren; num++) {
            HiveParserASTNode child = (HiveParserASTNode) join.getChild(num);
            if (child.getToken().getType() == HiveASTParser.TOK_TABREF) {
                processTable(qb, child);
            } else if (child.getToken().getType() == HiveASTParser.TOK_SUBQUERY) {
                processSubQuery(qb, child);
            } else if (child.getToken().getType() == HiveASTParser.TOK_PTBLFUNCTION) {
                queryProperties.setHasPTF(true);
                processPTF(qb, child);
                HiveParserPTFInvocationSpec ptfInvocationSpec = qb.getPTFInvocationSpec(child);
                String inputAlias =
                        ptfInvocationSpec == null
                                ? null
                                : ptfInvocationSpec.getFunction().getAlias();
                if (inputAlias == null) {
                    throw new SemanticException(
                            HiveParserUtils.generateErrorMessage(
                                    child, "PTF invocation in a Join must have an alias"));
                }

            } else if (child.getToken().getType() == HiveASTParser.TOK_LATERAL_VIEW
                    || child.getToken().getType() == HiveASTParser.TOK_LATERAL_VIEW_OUTER) {
                // SELECT * FROM src1 LATERAL VIEW udtf() AS myTable JOIN src2 ...
                // is not supported. Instead, the lateral view must be in a subquery
                // SELECT * FROM (SELECT * FROM src1 LATERAL VIEW udtf() AS myTable) a
                // JOIN src2 ...
                throw new SemanticException(
                        HiveParserErrorMsg.getMsg(ErrorMsg.LATERAL_VIEW_WITH_JOIN, join));
            } else if (HiveParserUtils.isJoinToken(child)) {
                processJoin(qb, child);
            }
        }
    }

    /**
     * Given the AST with TOK_LATERAL_VIEW as the root, get the alias for the table or subquery in
     * the lateral view and also make a mapping from the alias to all the lateral view AST's.
     */
    private String processLateralView(HiveParserQB qb, HiveParserASTNode lateralView)
            throws SemanticException {
        int numChildren = lateralView.getChildCount();

        assert (numChildren == 2);
        HiveParserASTNode next = (HiveParserASTNode) lateralView.getChild(1);

        String alias;

        switch (next.getToken().getType()) {
            case HiveASTParser.TOK_TABREF:
                alias = processTable(qb, next);
                break;
            case HiveASTParser.TOK_SUBQUERY:
                alias = processSubQuery(qb, next);
                break;
            case HiveASTParser.TOK_LATERAL_VIEW:
            case HiveASTParser.TOK_LATERAL_VIEW_OUTER:
                alias = processLateralView(qb, next);
                break;
            default:
                throw new SemanticException(
                        HiveParserErrorMsg.getMsg(
                                ErrorMsg.LATERAL_VIEW_INVALID_CHILD, lateralView));
        }
        alias = alias.toLowerCase();
        qb.getParseInfo().addLateralViewForAlias(alias, lateralView);
        qb.addAlias(alias);
        return alias;
    }

    /**
     * Phase 1: (including, but not limited to): 1. Gets all the aliases for all the tables /
     * subqueries and makes the appropriate mapping in aliasToTabs, aliasToSubq 2. Gets the location
     * of the destination and names the clause "inclause" + i 3. Creates a map from a string
     * representation of an aggregation tree to the actual aggregation AST 4. Creates a mapping from
     * the clause name to the select expression AST in destToSelExpr 5. Creates a mapping from a
     * table alias to the lateral view AST's in aliasToLateralViews
     */
    @SuppressWarnings({"fallthrough", "nls"})
    public boolean doPhase1(
            HiveParserASTNode ast,
            HiveParserQB qb,
            HiveParserBaseSemanticAnalyzer.Phase1Ctx ctx1,
            HiveParserPlannerContext plannerCtx)
            throws SemanticException {

        boolean phase1Result = true;
        HiveParserQBParseInfo qbp = qb.getParseInfo();
        boolean skipRecursion = false;

        if (ast.getToken() != null) {
            skipRecursion = true;
            switch (ast.getToken().getType()) {
                case HiveASTParser.TOK_SELECTDI:
                    qb.countSelDi();
                    // fall through
                case HiveASTParser.TOK_SELECT:
                    qb.countSel();
                    qbp.setSelExprForClause(ctx1.dest, ast);

                    int posn = 0;
                    if (((HiveParserASTNode) ast.getChild(0)).getToken().getType()
                            == HiveASTParser.QUERY_HINT) {
                        HiveASTParseDriver pd = new HiveASTParseDriver();
                        String queryHintStr = ast.getChild(0).getText();
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("QUERY HINT: " + queryHintStr);
                        }
                        try {
                            HiveParserASTNode hintNode = pd.parseHint(queryHintStr);
                            qbp.setHints(hintNode);
                            posn++;
                        } catch (HiveASTParseException e) {
                            throw new SemanticException(
                                    "failed to parse query hint: " + e.getMessage(), e);
                        }
                    }

                    if ((ast.getChild(posn).getChild(0).getType() == HiveASTParser.TOK_TRANSFORM)) {
                        queryProperties.setUsesScript(true);
                    }

                    LinkedHashMap<String, HiveParserASTNode> aggregations =
                            doPhase1GetAggregationsFromSelect(ast, qb, ctx1.dest);
                    doPhase1GetColumnAliasesFromSelect(ast, qbp);
                    qbp.setAggregationExprsForClause(ctx1.dest, aggregations);
                    qbp.setDistinctFuncExprsForClause(
                            ctx1.dest, doPhase1GetDistinctFuncExprs(aggregations));
                    break;

                case HiveASTParser.TOK_WHERE:
                    qbp.setWhrExprForClause(ctx1.dest, ast);
                    if (!HiveParserSubQueryUtils.findSubQueries((HiveParserASTNode) ast.getChild(0))
                            .isEmpty()) {
                        queryProperties.setFilterWithSubQuery(true);
                    }
                    break;

                case HiveASTParser.TOK_INSERT_INTO:
                    String currentDatabase = SessionState.get().getCurrentDatabase();
                    String tabName =
                            getUnescapedName(
                                    (HiveParserASTNode) ast.getChild(0).getChild(0),
                                    currentDatabase);
                    qbp.addInsertIntoTable(tabName, ast);
                    // TODO: hive doesn't break here, so we copy what's below here
                    handleTokDestination(ctx1, ast, qbp, plannerCtx);
                    break;

                case HiveASTParser.TOK_DESTINATION:
                    handleTokDestination(ctx1, ast, qbp, plannerCtx);
                    break;

                case HiveASTParser.TOK_FROM:
                    int childCount = ast.getChildCount();
                    if (childCount != 1) {
                        throw new SemanticException(
                                HiveParserUtils.generateErrorMessage(
                                        ast, "Multiple Children " + childCount));
                    }

                    if (!qbp.getIsSubQ()) {
                        qbp.setQueryFromExpr(ast);
                    }

                    // Check if this is a subquery / lateral view
                    HiveParserASTNode frm = (HiveParserASTNode) ast.getChild(0);
                    if (frm.getToken().getType() == HiveASTParser.TOK_TABREF) {
                        processTable(qb, frm);
                    } else if (frm.getToken().getType() == HiveASTParser.TOK_VIRTUAL_TABLE) {
                        // Create a temp table with the passed values in it then rewrite this
                        // portion of the tree to be from that table.
                        HiveParserASTNode newFrom = genValuesTempTable(frm, qb);
                        ast.setChild(0, newFrom);
                        processTable(qb, newFrom);
                    } else if (frm.getToken().getType() == HiveASTParser.TOK_SUBQUERY) {
                        processSubQuery(qb, frm);
                    } else if (frm.getToken().getType() == HiveASTParser.TOK_LATERAL_VIEW
                            || frm.getToken().getType() == HiveASTParser.TOK_LATERAL_VIEW_OUTER) {
                        queryProperties.setHasLateralViews(true);
                        processLateralView(qb, frm);
                    } else if (HiveParserUtils.isJoinToken(frm)) {
                        processJoin(qb, frm);
                        qbp.setJoinExpr(frm);
                    } else if (frm.getToken().getType() == HiveASTParser.TOK_PTBLFUNCTION) {
                        queryProperties.setHasPTF(true);
                        processPTF(qb, frm);
                    }
                    break;

                case HiveASTParser.TOK_CLUSTERBY:
                    // Get the clusterby aliases - these are aliased to the entries in the select
                    // list
                    queryProperties.setHasClusterBy(true);
                    qbp.setClusterByExprForClause(ctx1.dest, ast);
                    break;

                case HiveASTParser.TOK_DISTRIBUTEBY:
                    // Get the distribute by aliases - these are aliased to the entries in the
                    // select list
                    queryProperties.setHasDistributeBy(true);
                    qbp.setDistributeByExprForClause(ctx1.dest, ast);
                    if (qbp.getClusterByForClause(ctx1.dest) != null) {
                        throw new SemanticException(
                                HiveParserUtils.generateErrorMessage(
                                        ast, ErrorMsg.CLUSTERBY_DISTRIBUTEBY_CONFLICT.getMsg()));
                    } else if (qbp.getOrderByForClause(ctx1.dest) != null) {
                        throw new SemanticException(
                                HiveParserUtils.generateErrorMessage(
                                        ast, ErrorMsg.ORDERBY_DISTRIBUTEBY_CONFLICT.getMsg()));
                    }
                    break;

                case HiveASTParser.TOK_SORTBY:
                    // Get the sort by aliases - these are aliased to the entries in the select list
                    queryProperties.setHasSortBy(true);
                    qbp.setSortByExprForClause(ctx1.dest, ast);
                    if (qbp.getClusterByForClause(ctx1.dest) != null) {
                        throw new SemanticException(
                                HiveParserUtils.generateErrorMessage(
                                        ast, ErrorMsg.CLUSTERBY_SORTBY_CONFLICT.getMsg()));
                    } else if (qbp.getOrderByForClause(ctx1.dest) != null) {
                        throw new SemanticException(
                                HiveParserUtils.generateErrorMessage(
                                        ast, ErrorMsg.ORDERBY_SORTBY_CONFLICT.getMsg()));
                    }
                    break;

                case HiveASTParser.TOK_ORDERBY:
                    // Get the order by aliases - these are aliased to the entries in the select
                    // list
                    queryProperties.setHasOrderBy(true);
                    qbp.setOrderByExprForClause(ctx1.dest, ast);
                    if (qbp.getClusterByForClause(ctx1.dest) != null) {
                        throw new SemanticException(
                                HiveParserUtils.generateErrorMessage(
                                        ast, ErrorMsg.CLUSTERBY_ORDERBY_CONFLICT.getMsg()));
                    }
                    break;

                case HiveASTParser.TOK_GROUPBY:
                case HiveASTParser.TOK_ROLLUP_GROUPBY:
                case HiveASTParser.TOK_CUBE_GROUPBY:
                case HiveASTParser.TOK_GROUPING_SETS:
                    // Get the groupby aliases - these are aliased to the entries in the select list
                    queryProperties.setHasGroupBy(true);
                    if (qbp.getJoinExpr() != null) {
                        queryProperties.setHasJoinFollowedByGroupBy(true);
                    }
                    if (qbp.getSelForClause(ctx1.dest).getToken().getType()
                            == HiveASTParser.TOK_SELECTDI) {
                        throw new SemanticException(
                                HiveParserUtils.generateErrorMessage(
                                        ast, ErrorMsg.SELECT_DISTINCT_WITH_GROUPBY.getMsg()));
                    }
                    qbp.setGroupByExprForClause(ctx1.dest, ast);
                    skipRecursion = true;

                    // Rollup and Cubes are syntactic sugar on top of grouping sets
                    if (ast.getToken().getType() == HiveASTParser.TOK_ROLLUP_GROUPBY) {
                        qbp.getDestRollups().add(ctx1.dest);
                    } else if (ast.getToken().getType() == HiveASTParser.TOK_CUBE_GROUPBY) {
                        qbp.getDestCubes().add(ctx1.dest);
                    } else if (ast.getToken().getType() == HiveASTParser.TOK_GROUPING_SETS) {
                        qbp.getDestGroupingSets().add(ctx1.dest);
                    }
                    break;

                case HiveASTParser.TOK_HAVING:
                    qbp.setHavingExprForClause(ctx1.dest, ast);
                    qbp.addAggregationExprsForClause(
                            ctx1.dest, doPhase1GetAggregationsFromSelect(ast, qb, ctx1.dest));
                    break;

                case HiveASTParser.KW_WINDOW:
                    if (!qb.hasWindowingSpec(ctx1.dest)) {
                        throw new SemanticException(
                                HiveParserUtils.generateErrorMessage(
                                        ast,
                                        "Query has no Cluster/Distribute By; but has a Window definition"));
                    }
                    handleQueryWindowClauses(qb, ctx1, ast);
                    break;

                case HiveASTParser.TOK_LIMIT:
                    if (ast.getChildCount() == 2) {
                        qbp.setDestLimit(
                                ctx1.dest,
                                new Integer(ast.getChild(0).getText()),
                                new Integer(ast.getChild(1).getText()));
                    } else {
                        qbp.setDestLimit(ctx1.dest, 0, new Integer(ast.getChild(0).getText()));
                    }
                    break;

                case HiveASTParser.TOK_ANALYZE:
                    // Case of analyze command
                    String tableName =
                            getUnescapedName((HiveParserASTNode) ast.getChild(0).getChild(0))
                                    .toLowerCase();

                    qb.setTabAlias(tableName, tableName);
                    qb.addAlias(tableName);
                    qb.getParseInfo().setIsAnalyzeCommand(true);
                    qb.getParseInfo().setNoScanAnalyzeCommand(this.noscan);
                    qb.getParseInfo().setPartialScanAnalyzeCommand(this.partialscan);
                    // Allow analyze the whole table and dynamic partitions
                    HiveConf.setVar(conf, HiveConf.ConfVars.DYNAMICPARTITIONINGMODE, "nonstrict");
                    HiveConf.setVar(conf, HiveConf.ConfVars.HIVEMAPREDMODE, "nonstrict");

                    break;

                case HiveASTParser.TOK_UNIONALL:
                    if (!qbp.getIsSubQ()) {
                        // this shouldn't happen. The parser should have converted the union to be
                        // contained in a subquery. Just in case, we keep the error as a fallback.
                        throw new SemanticException(
                                HiveParserUtils.generateErrorMessage(
                                        ast, ErrorMsg.UNION_NOTIN_SUBQ.getMsg()));
                    }
                    skipRecursion = false;
                    break;

                case HiveASTParser.TOK_INSERT:
                    HiveParserASTNode destination = (HiveParserASTNode) ast.getChild(0);
                    Tree tab = destination.getChild(0);

                    // Proceed if AST contains partition & If Not Exists
                    if (destination.getChildCount() == 2
                            && tab.getChildCount() == 2
                            && destination.getChild(1).getType() == HiveASTParser.TOK_IFNOTEXISTS) {
                        String name = tab.getChild(0).getChild(0).getText();

                        Tree partitions = tab.getChild(1);
                        int numChildren = partitions.getChildCount();
                        HashMap<String, String> partition = new HashMap<>();
                        for (int i = 0; i < numChildren; i++) {
                            String partitionName = partitions.getChild(i).getChild(0).getText();
                            Tree pvalue = partitions.getChild(i).getChild(1);
                            if (pvalue == null) {
                                break;
                            }
                            String partitionVal = stripQuotes(pvalue.getText());
                            partition.put(partitionName, partitionVal);
                        }
                        // if it is a dynamic partition throw the exception
                        if (numChildren != partition.size()) {
                            throw new SemanticException(
                                    ErrorMsg.INSERT_INTO_DYNAMICPARTITION_IFNOTEXISTS.getMsg(
                                            partition.toString()));
                        }
                        Table table;
                        try {
                            table = getTableObjectByName(name);
                        } catch (HiveException ex) {
                            throw new SemanticException(ex);
                        }
                        try {
                            Partition parMetaData = db.getPartition(table, partition, false);
                            // Check partition exists if it exists skip the overwrite
                            if (parMetaData != null) {
                                phase1Result = false;
                                skipRecursion = true;
                                LOG.info(
                                        "Partition already exists so insert into overwrite "
                                                + "skipped for partition : "
                                                + parMetaData.toString());
                                break;
                            }
                        } catch (HiveException e) {
                            LOG.info("Error while getting metadata : ", e);
                        }
                        validatePartSpec(
                                table,
                                partition,
                                (HiveParserASTNode) tab,
                                conf,
                                false,
                                frameworkConfig,
                                cluster);
                    }
                    skipRecursion = false;
                    break;
                case HiveASTParser.TOK_LATERAL_VIEW:
                case HiveASTParser.TOK_LATERAL_VIEW_OUTER:
                    // todo: nested LV
                    assert ast.getChildCount() == 1;
                    qb.getParseInfo().getDestToLateralView().put(ctx1.dest, ast);
                    break;
                case HiveASTParser.TOK_CTE:
                    processCTE(qb, ast);
                    break;
                default:
                    skipRecursion = false;
                    break;
            }
        }

        if (!skipRecursion) {
            // Iterate over the rest of the children
            int childCount = ast.getChildCount();
            for (int childPos = 0; childPos < childCount && phase1Result; ++childPos) {
                phase1Result =
                        doPhase1((HiveParserASTNode) ast.getChild(childPos), qb, ctx1, plannerCtx);
            }
        }
        return phase1Result;
    }

    private void handleTokDestination(
            HiveParserBaseSemanticAnalyzer.Phase1Ctx ctx1,
            HiveParserASTNode ast,
            HiveParserQBParseInfo qbp,
            HiveParserPlannerContext plannerCtx)
            throws SemanticException {
        ctx1.dest = this.ctx.getDestNamePrefix(ast).toString() + ctx1.nextNum;
        ctx1.nextNum++;
        boolean isTmpFileDest = false;
        if (ast.getChildCount() > 0 && ast.getChild(0) instanceof HiveParserASTNode) {
            HiveParserASTNode ch = (HiveParserASTNode) ast.getChild(0);
            if (ch.getToken().getType() == HiveASTParser.TOK_DIR
                    && ch.getChildCount() > 0
                    && ch.getChild(0) instanceof HiveParserASTNode) {
                ch = (HiveParserASTNode) ch.getChild(0);
                isTmpFileDest = ch.getToken().getType() == HiveASTParser.TOK_TMP_FILE;
            } else {
                if (ast.getToken().getType() == HiveASTParser.TOK_DESTINATION
                        && ast.getChild(0).getType() == HiveASTParser.TOK_TAB) {
                    String fullTableName =
                            getUnescapedName(
                                    (HiveParserASTNode) ast.getChild(0).getChild(0),
                                    SessionState.get().getCurrentDatabase());
                    qbp.getInsertOverwriteTables().put(fullTableName, ast);
                }
            }
        }

        // is there a insert in the subquery
        if (qbp.getIsSubQ() && !isTmpFileDest) {
            throw new SemanticException(
                    HiveParserErrorMsg.getMsg(ErrorMsg.NO_INSERT_INSUBQUERY, ast));
        }

        qbp.setDestForClause(ctx1.dest, (HiveParserASTNode) ast.getChild(0));
        handleInsertStatementSpecPhase1(ast, qbp, ctx1);

        if (qbp.getClauseNamesForDest().size() == 2) {
            // From the moment that we have two destination clauses,
            // we know that this is a multi-insert query.
            // Thus, set property to right value.
            // Using qbp.getClauseNamesForDest().size() >= 2 would be
            // equivalent, but we use == to avoid setting the property
            // multiple times
            queryProperties.setMultiDestQuery(true);
        }

        if (plannerCtx != null && !queryProperties.hasMultiDestQuery()) {
            plannerCtx.setInsertToken(ast, isTmpFileDest);
        } else if (plannerCtx != null && qbp.getClauseNamesForDest().size() == 2) {
            // For multi-insert query, currently we only optimize the FROM clause.
            // Hence, introduce multi-insert token on top of it.
            // However, first we need to reset existing token (insert).
            // Using qbp.getClauseNamesForDest().size() >= 2 would be
            // equivalent, but we use == to avoid setting the property
            // multiple times
            plannerCtx.resetToken();
            plannerCtx.setMultiInsertToken((HiveParserASTNode) qbp.getQueryFrom().getChild(0));
        }
    }

    // This is phase1 of supporting specifying schema in insert statement.
    // insert into foo(z,y) select a,b from bar;
    private void handleInsertStatementSpecPhase1(
            HiveParserASTNode ast,
            HiveParserQBParseInfo qbp,
            HiveParserBaseSemanticAnalyzer.Phase1Ctx ctx1)
            throws SemanticException {
        HiveParserASTNode tabColName = (HiveParserASTNode) ast.getChild(1);
        if (ast.getType() == HiveASTParser.TOK_INSERT_INTO
                && tabColName != null
                && tabColName.getType() == HiveASTParser.TOK_TABCOLNAME) {
            // we have "insert into foo(a,b)..."; parser will enforce that 1+ columns are listed if
            // TOK_TABCOLNAME is present
            List<String> targetColNames = new ArrayList<>();
            for (Node col : tabColName.getChildren()) {
                assert ((HiveParserASTNode) col).getType() == HiveASTParser.Identifier
                        : "expected token "
                                + HiveASTParser.Identifier
                                + " found "
                                + ((HiveParserASTNode) col).getType();
                targetColNames.add(((HiveParserASTNode) col).getText());
            }
            String fullTableName =
                    getUnescapedName(
                            (HiveParserASTNode) ast.getChild(0).getChild(0),
                            SessionState.get().getCurrentDatabase());
            qbp.setDestSchemaForClause(ctx1.dest, targetColNames);
            Set<String> targetColumns = new HashSet<>(targetColNames);
            if (targetColNames.size() != targetColumns.size()) {
                throw new SemanticException(
                        HiveParserUtils.generateErrorMessage(
                                tabColName,
                                "Duplicate column name detected in "
                                        + fullTableName
                                        + " table schema specification"));
            }
            Table targetTable;
            try {
                targetTable = db.getTable(fullTableName, false);
            } catch (HiveException ex) {
                LOG.error("Error processing HiveASTParser.TOK_DESTINATION: " + ex.getMessage(), ex);
                throw new SemanticException(ex);
            }
            if (targetTable == null) {
                throw new SemanticException(
                        HiveParserUtils.generateErrorMessage(
                                ast, "Unable to access metadata for table " + fullTableName));
            }
            for (FieldSchema f : targetTable.getCols()) {
                // parser only allows foo(a,b), not foo(foo.a, foo.b)
                targetColumns.remove(f.getName());
            }
            // here we need to see if remaining columns are dynamic partition columns
            if (!targetColumns.isEmpty()) {
                /* We just checked the user specified schema columns among regular table column and found some which are not
                'regular'.  Now check is they are dynamic partition columns
                  For dynamic partitioning,
                  Given "create table multipart(a int, b int) partitioned by (c int, d int);"
                  for "insert into multipart partition(c='1',d)(d,a) values(2,3);" we expect parse tree to look like this
                   (TOK_INSERT_INTO
                    (TOK_TAB
                      (TOK_TABNAME multipart)
                      (TOK_PARTSPEC
                        (TOK_PARTVAL c '1')
                        (TOK_PARTVAL d)
                      )
                    )
                    (TOK_TABCOLNAME d a)
                   )*/
                List<String> dynamicPartitionColumns = new ArrayList<String>();
                if (ast.getChild(0) != null && ast.getChild(0).getType() == HiveASTParser.TOK_TAB) {
                    HiveParserASTNode tokTab = (HiveParserASTNode) ast.getChild(0);
                    HiveParserASTNode tokPartSpec =
                            (HiveParserASTNode)
                                    tokTab.getFirstChildWithType(HiveASTParser.TOK_PARTSPEC);
                    if (tokPartSpec != null) {
                        for (Node n : tokPartSpec.getChildren()) {
                            HiveParserASTNode tokPartVal = null;
                            if (n instanceof HiveParserASTNode) {
                                tokPartVal = (HiveParserASTNode) n;
                            }
                            if (tokPartVal != null
                                    && tokPartVal.getType() == HiveASTParser.TOK_PARTVAL
                                    && tokPartVal.getChildCount() == 1) {
                                assert tokPartVal.getChild(0).getType() == HiveASTParser.Identifier
                                        : "Expected column name; found tokType="
                                                + tokPartVal.getType();
                                dynamicPartitionColumns.add(tokPartVal.getChild(0).getText());
                            }
                        }
                    }
                }
                for (String colName : dynamicPartitionColumns) {
                    targetColumns.remove(colName);
                }
                if (!targetColumns.isEmpty()) {
                    // Found some columns in user specified schema which are neither regular not
                    // dynamic partition columns
                    throw new SemanticException(
                            HiveParserUtils.generateErrorMessage(
                                    tabColName,
                                    "'"
                                            + (targetColumns.size() == 1
                                                    ? targetColumns.iterator().next()
                                                    : targetColumns)
                                            + "' in insert schema specification "
                                            + (targetColumns.size() == 1 ? "is" : "are")
                                            + " not found among regular columns of "
                                            + fullTableName
                                            + " nor dynamic partition columns."));
                }
            }
        }
    }

    public void getMaterializationMetadata(HiveParserQB qb) throws SemanticException {
        try {
            gatherCTEReferences(qb, rootClause);
            int threshold =
                    Integer.parseInt(conf.get("hive.optimize.cte.materialize.threshold", "-1"));
            for (HiveParserBaseSemanticAnalyzer.CTEClause cte :
                    new HashSet<>(aliasToCTEs.values())) {
                if (threshold >= 0 && cte.reference >= threshold) {
                    cte.materialize = true;
                }
            }
        } catch (HiveException e) {
            LOG.error(org.apache.hadoop.util.StringUtils.stringifyException(e));
            if (e instanceof SemanticException) {
                throw (SemanticException) e;
            }
            throw new SemanticException(e.getMessage(), e);
        }
    }

    private void gatherCTEReferences(
            HiveParserQBExpr qbexpr, HiveParserBaseSemanticAnalyzer.CTEClause parent)
            throws HiveException {
        if (qbexpr.getOpcode() == HiveParserQBExpr.Opcode.NULLOP) {
            gatherCTEReferences(qbexpr.getQB(), parent);
        } else {
            gatherCTEReferences(qbexpr.getQBExpr1(), parent);
            gatherCTEReferences(qbexpr.getQBExpr2(), parent);
        }
    }

    // TODO: check view references, too
    private void gatherCTEReferences(
            HiveParserQB qb, HiveParserBaseSemanticAnalyzer.CTEClause current)
            throws HiveException {
        for (String alias : qb.getTabAliases()) {
            String tabName = qb.getTabNameForAlias(alias);
            String cteName = tabName.toLowerCase();

            HiveParserBaseSemanticAnalyzer.CTEClause cte = findCTEFromName(qb, cteName);
            if (cte != null) {
                if (ctesExpanded.contains(cteName)) {
                    throw new SemanticException(
                            "Recursive cte "
                                    + cteName
                                    + " detected (cycle: "
                                    + StringUtils.join(ctesExpanded, " -> ")
                                    + " -> "
                                    + cteName
                                    + ").");
                }
                cte.reference++;
                current.parents.add(cte);
                if (cte.qbExpr != null) {
                    continue;
                }
                cte.qbExpr = new HiveParserQBExpr(cteName);
                doPhase1QBExpr(cte.cteNode, cte.qbExpr, qb.getId(), cteName);

                ctesExpanded.add(cteName);
                gatherCTEReferences(cte.qbExpr, cte);
                ctesExpanded.remove(ctesExpanded.size() - 1);
            }
        }
        for (String alias : qb.getSubqAliases()) {
            gatherCTEReferences(qb.getSubqForAlias(alias), current);
        }
    }

    public void getMetaData(HiveParserQB qb) throws SemanticException {
        getMetaData(qb, false);
    }

    public void getMetaData(HiveParserQB qb, boolean enableMaterialization)
            throws SemanticException {
        try {
            if (enableMaterialization) {
                getMaterializationMetadata(qb);
            }
            getMetaData(qb, null);
        } catch (HiveException e) {
            LOG.error(org.apache.hadoop.util.StringUtils.stringifyException(e));
            if (e instanceof SemanticException) {
                throw (SemanticException) e;
            }
            throw new SemanticException(e.getMessage(), e);
        }
    }

    private void getMetaData(HiveParserQBExpr qbexpr, ReadEntity parentInput) throws HiveException {
        if (qbexpr.getOpcode() == HiveParserQBExpr.Opcode.NULLOP) {
            getMetaData(qbexpr.getQB(), parentInput);
        } else {
            getMetaData(qbexpr.getQBExpr1(), parentInput);
            getMetaData(qbexpr.getQBExpr2(), parentInput);
        }
    }

    @SuppressWarnings("nls")
    private void getMetaData(HiveParserQB qb, ReadEntity parentInput) throws HiveException {
        LOG.info("Get metadata for source tables");

        // Go over the tables and populate the related structures. We have to materialize the table
        // alias list since we might
        // modify it in the middle for view rewrite.
        List<String> tabAliases = new ArrayList<>(qb.getTabAliases());

        // Keep track of view alias to view name and read entity
        // For eg: for a query like 'select * from V3', where V3 -> V2, V2 -> V1, V1 -> T
        // keeps track of full view name and read entity corresponding to alias V3, V3:V2, V3:V2:V1.
        // This is needed for tracking the dependencies for inputs, along with their parents.
        Map<String, ObjectPair<String, ReadEntity>> aliasToViewInfo = new HashMap<>();

        // used to capture view to SQ conversions. This is used to check for recursive CTE
        // invocations.
        Map<String, String> sqAliasToCTEName = new HashMap<>();

        for (String alias : tabAliases) {
            String tabName = qb.getTabNameForAlias(alias);
            String cteName = tabName.toLowerCase();

            Table tab = db.getTable(tabName, false);
            if (tab == null || tab.getDbName().equals(SessionState.get().getCurrentDatabase())) {
                // we first look for this alias from CTE, and then from catalog.
                HiveParserBaseSemanticAnalyzer.CTEClause cte = findCTEFromName(qb, cteName);
                if (cte != null) {
                    if (!cte.materialize) {
                        addCTEAsSubQuery(qb, cteName, alias);
                        sqAliasToCTEName.put(alias, cteName);
                        continue;
                    }
                    throw new SemanticException("Materializing CTE is not supported at the moment");
                }
            }

            if (tab == null) {
                HiveParserASTNode src = qb.getParseInfo().getSrcForAlias(alias);
                if (null != src) {
                    throw new SemanticException(
                            HiveParserErrorMsg.getMsg(ErrorMsg.INVALID_TABLE, src));
                } else {
                    throw new SemanticException(ErrorMsg.INVALID_TABLE.getMsg(alias));
                }
            }
            if (tab.isView()) {
                if (qb.getParseInfo().isAnalyzeCommand()) {
                    throw new SemanticException(ErrorMsg.ANALYZE_VIEW.getMsg());
                }
                String fullViewName = tab.getDbName() + "." + tab.getTableName();
                // Prevent view cycles
                if (viewsExpanded.contains(fullViewName)) {
                    throw new SemanticException(
                            "Recursive view "
                                    + fullViewName
                                    + " detected (cycle: "
                                    + StringUtils.join(viewsExpanded, " -> ")
                                    + " -> "
                                    + fullViewName
                                    + ").");
                }
                replaceViewReferenceWithDefinition(qb, tab, tabName, alias);
                // This is the last time we'll see the Table objects for views, so add it to the
                // inputs now. isInsideView will tell if this view is embedded in another view.
                // If the view is Inside another view, it should have at least one parent
                if (qb.isInsideView() && parentInput == null) {
                    parentInput =
                            PlanUtils.getParentViewInfo(getAliasId(alias, qb), viewAliasToInput);
                }
                ReadEntity viewInput = new ReadEntity(tab, parentInput, !qb.isInsideView());
                viewInput = PlanUtils.addInput(inputs, viewInput);
                aliasToViewInfo.put(alias, new ObjectPair<>(fullViewName, viewInput));
                String aliasId = getAliasId(alias, qb);
                if (aliasId != null) {
                    aliasId = aliasId.replace(SUBQUERY_TAG_1, "").replace(SUBQUERY_TAG_2, "");
                }
                viewAliasToInput.put(aliasId, viewInput);
                continue;
            }

            if (!InputFormat.class.isAssignableFrom(tab.getInputFormatClass())) {
                throw new SemanticException(
                        HiveParserUtils.generateErrorMessage(
                                qb.getParseInfo().getSrcForAlias(alias),
                                ErrorMsg.INVALID_INPUT_FORMAT_TYPE.getMsg()));
            }

            qb.getMetaData().setSrcForAlias(alias, tab);

            if (qb.getParseInfo().isAnalyzeCommand()) {
                // allow partial partition specification for nonscan since noscan is fast.
                TableSpec ts =
                        new TableSpec(
                                db,
                                conf,
                                (HiveParserASTNode) ast.getChild(0),
                                true,
                                this.noscan,
                                frameworkConfig,
                                cluster);
                if (ts.specType == SpecType.DYNAMIC_PARTITION) { // dynamic partitions
                    try {
                        ts.partitions = db.getPartitionsByNames(ts.tableHandle, ts.partSpec);
                    } catch (HiveException e) {
                        throw new SemanticException(
                                HiveParserUtils.generateErrorMessage(
                                        qb.getParseInfo().getSrcForAlias(alias),
                                        "Cannot get partitions for " + ts.partSpec),
                                e);
                    }
                }
                // validate partial scan command
                HiveParserQBParseInfo qbpi = qb.getParseInfo();
                if (qbpi.isPartialScanAnalyzeCommand()) {
                    Class<? extends InputFormat> inputFormatClass = null;
                    switch (ts.specType) {
                        case TABLE_ONLY:
                        case DYNAMIC_PARTITION:
                            inputFormatClass = ts.tableHandle.getInputFormatClass();
                            break;
                        case STATIC_PARTITION:
                            inputFormatClass = ts.partHandle.getInputFormatClass();
                            break;
                        default:
                            assert false;
                    }
                    if (!(inputFormatClass.equals(RCFileInputFormat.class)
                            || inputFormatClass.equals(OrcInputFormat.class))) {
                        throw new SemanticException(
                                "ANALYZE TABLE PARTIALSCAN doesn't support non-RCfile.");
                    }
                }

                qb.getParseInfo().addTableSpec(alias, ts);
            }

            ReadEntity parentViewInfo =
                    PlanUtils.getParentViewInfo(getAliasId(alias, qb), viewAliasToInput);
            // Temporary tables created during the execution are not the input sources
            if (!HiveParserUtils.isValuesTempTable(alias)) {
                HiveParserUtils.addInput(
                        inputs,
                        new ReadEntity(tab, parentViewInfo, parentViewInfo == null),
                        mergeIsDirect);
            }
        }

        LOG.info("Get metadata for subqueries");
        // Go over the subqueries and getMetaData for these
        for (String alias : qb.getSubqAliases()) {
            boolean wasView = aliasToViewInfo.containsKey(alias);
            boolean wasCTE = sqAliasToCTEName.containsKey(alias);
            ReadEntity newParentInput = null;
            if (wasView) {
                viewsExpanded.add(aliasToViewInfo.get(alias).getFirst());
                newParentInput = aliasToViewInfo.get(alias).getSecond();
            } else if (wasCTE) {
                ctesExpanded.add(sqAliasToCTEName.get(alias));
            }
            HiveParserQBExpr qbexpr = qb.getSubqForAlias(alias);
            getMetaData(qbexpr, newParentInput);
            if (wasView) {
                viewsExpanded.remove(viewsExpanded.size() - 1);
            } else if (wasCTE) {
                ctesExpanded.remove(ctesExpanded.size() - 1);
            }
        }

        HiveParserBaseSemanticAnalyzer.HiveParserRowFormatParams rowFormatParams =
                new HiveParserBaseSemanticAnalyzer.HiveParserRowFormatParams();
        HiveParserStorageFormat storageFormat = new HiveParserStorageFormat(conf);

        LOG.info("Get metadata for destination tables");
        // Go over all the destination structures and populate the related metadata
        HiveParserQBParseInfo qbp = qb.getParseInfo();

        for (String name : qbp.getClauseNamesForDest()) {
            HiveParserASTNode ast = qbp.getDestForClause(name);
            switch (ast.getToken().getType()) {
                case HiveASTParser.TOK_TAB:
                    {
                        TableSpec ts = new TableSpec(db, conf, ast, frameworkConfig, cluster);
                        if (ts.tableHandle.isView()
                                || hiveShim.isMaterializedView(ts.tableHandle)) {
                            throw new SemanticException(ErrorMsg.DML_AGAINST_VIEW.getMsg());
                        }

                        Class<?> outputFormatClass = ts.tableHandle.getOutputFormatClass();
                        if (!ts.tableHandle.isNonNative()
                                && !HiveOutputFormat.class.isAssignableFrom(outputFormatClass)) {
                            throw new SemanticException(
                                    HiveParserErrorMsg.getMsg(
                                            ErrorMsg.INVALID_OUTPUT_FORMAT_TYPE,
                                            ast,
                                            "The class is " + outputFormatClass.toString()));
                        }

                        boolean isTableWrittenTo =
                                qb.getParseInfo()
                                        .isInsertIntoTable(
                                                ts.tableHandle.getDbName(),
                                                ts.tableHandle.getTableName());
                        isTableWrittenTo |=
                                (qb.getParseInfo()
                                                .getInsertOverwriteTables()
                                                .get(
                                                        getUnescapedName(
                                                                (HiveParserASTNode) ast.getChild(0),
                                                                ts.tableHandle.getDbName()))
                                        != null);
                        assert isTableWrittenTo
                                : "Inconsistent data structure detected: we are writing to "
                                        + ts.tableHandle
                                        + " in "
                                        + name
                                        + " but it's not in isInsertIntoTable() or getInsertOverwriteTables()";
                        // TableSpec ts is got from the query (user specified),
                        // which means the user didn't specify partitions in their query,
                        // but whether the table itself is partitioned is not know.
                        if (ts.specType != SpecType.STATIC_PARTITION) {
                            // This is a table or dynamic partition
                            qb.getMetaData().setDestForAlias(name, ts.tableHandle);
                            // has dynamic as well as static partitions
                            if (ts.partSpec != null && ts.partSpec.size() > 0) {
                                qb.getMetaData().setPartSpecForAlias(name, ts.partSpec);
                            }
                        } else {
                            // This is a partition
                            qb.getMetaData().setDestForAlias(name, ts.partHandle);
                        }
                        if (HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVESTATSAUTOGATHER)) {
                            // Add the table spec for the destination table.
                            qb.getParseInfo().addTableSpec(ts.tableName.toLowerCase(), ts);
                        }
                        break;
                    }

                case HiveASTParser.TOK_DIR:
                    {
                        // This is a dfs file
                        String fname = stripQuotes(ast.getChild(0).getText());
                        if ((!qb.getParseInfo().getIsSubQ())
                                && (((HiveParserASTNode) ast.getChild(0)).getToken().getType()
                                        == HiveASTParser.TOK_TMP_FILE)) {
                            if (qb.isCTAS() || qb.isMaterializedView()) {
                                qb.setIsQuery(false);

                                Path location;
                                // If the CTAS query does specify a location, use the table
                                // location, else use the db location
                                if (qb.getTableDesc() != null
                                        && qb.getTableDesc().getLocation() != null) {
                                    location = new Path(qb.getTableDesc().getLocation());
                                } else {
                                    // allocate a temporary output dir on the location of the table
                                    String tableName =
                                            getUnescapedName((HiveParserASTNode) ast.getChild(0));
                                    String[] names = Utilities.getDbTableName(tableName);
                                    try {
                                        Warehouse wh = new Warehouse(conf);
                                        // Use destination table's db location.
                                        String destTableDb =
                                                qb.getTableDesc() != null
                                                        ? qb.getTableDesc().getDatabaseName()
                                                        : null;
                                        if (destTableDb == null) {
                                            destTableDb = names[0];
                                        }
                                        location = wh.getDatabasePath(db.getDatabase(destTableDb));
                                    } catch (MetaException e) {
                                        throw new SemanticException(e);
                                    }
                                }
                                if (HiveConf.getBoolVar(
                                        conf, HiveConf.ConfVars.HIVESTATSAUTOGATHER)) {
                                    TableSpec ts =
                                            new TableSpec(
                                                    db, conf, this.ast, frameworkConfig, cluster);
                                    // Add the table spec for the destination table.
                                    qb.getParseInfo().addTableSpec(ts.tableName.toLowerCase(), ts);
                                }
                            } else {
                                // This is the only place where isQuery is set to true; it defaults
                                // to false.
                                qb.setIsQuery(true);
                            }
                        }

                        boolean isDfsFile = true;
                        if (ast.getChildCount() >= 2
                                && ast.getChild(1).getText().toLowerCase().equals("local")) {
                            isDfsFile = false;
                        }
                        // Set the destination for the SELECT query inside the CTAS
                        qb.getMetaData().setDestForAlias(name, fname, isDfsFile);

                        CreateTableDesc directoryDesc = new CreateTableDesc();
                        boolean directoryDescIsSet = false;
                        int numCh = ast.getChildCount();
                        for (int num = 1; num < numCh; num++) {
                            HiveParserASTNode child = (HiveParserASTNode) ast.getChild(num);
                            if (child != null) {
                                if (storageFormat.fillStorageFormat(child)) {
                                    directoryDesc.setOutputFormat(storageFormat.getOutputFormat());
                                    directoryDesc.setSerName(storageFormat.getSerde());
                                    directoryDescIsSet = true;
                                    continue;
                                }
                                switch (child.getToken().getType()) {
                                    case HiveASTParser.TOK_TABLEROWFORMAT:
                                        rowFormatParams.analyzeRowFormat(child);
                                        directoryDesc.setFieldDelim(rowFormatParams.fieldDelim);
                                        directoryDesc.setLineDelim(rowFormatParams.lineDelim);
                                        directoryDesc.setCollItemDelim(
                                                rowFormatParams.collItemDelim);
                                        directoryDesc.setMapKeyDelim(rowFormatParams.mapKeyDelim);
                                        directoryDesc.setFieldEscape(rowFormatParams.fieldEscape);
                                        directoryDesc.setNullFormat(rowFormatParams.nullFormat);
                                        directoryDescIsSet = true;
                                        break;
                                    case HiveASTParser.TOK_TABLESERIALIZER:
                                        HiveParserASTNode serdeChild =
                                                (HiveParserASTNode) child.getChild(0);
                                        storageFormat.setSerde(
                                                unescapeSQLString(
                                                        serdeChild.getChild(0).getText()));
                                        directoryDesc.setSerName(storageFormat.getSerde());
                                        if (serdeChild.getChildCount() > 1) {
                                            directoryDesc.setSerdeProps(
                                                    new HashMap<String, String>());
                                            readProps(
                                                    (HiveParserASTNode)
                                                            serdeChild.getChild(1).getChild(0),
                                                    directoryDesc.getSerdeProps());
                                        }
                                        directoryDescIsSet = true;
                                        break;
                                }
                            }
                        }
                        if (directoryDescIsSet) {
                            qb.setDirectoryDesc(directoryDesc);
                        }
                        break;
                    }
                default:
                    throw new SemanticException(
                            HiveParserUtils.generateErrorMessage(
                                    ast, "Unknown Token Type " + ast.getToken().getType()));
            }
        }
    }

    private void replaceViewReferenceWithDefinition(
            HiveParserQB qb, Table tab, String tabName, String alias) throws SemanticException {
        HiveParserASTNode viewTree;
        final HiveParserASTNodeOrigin viewOrigin =
                new HiveParserASTNodeOrigin(
                        "VIEW",
                        tab.getTableName(),
                        tab.getViewExpandedText(),
                        alias,
                        qb.getParseInfo().getSrcForAlias(alias));
        try {
            // Reparse text, passing null for context to avoid clobbering
            // the top-level token stream.
            String viewText = tab.getViewExpandedText();
            viewTree = HiveASTParseUtils.parse(viewText, ctx, tab.getCompleteName());

            Dispatcher nodeOriginDispatcher =
                    (nd, stack, nodeOutputs) -> {
                        ((HiveParserASTNode) nd).setOrigin(viewOrigin);
                        return null;
                    };
            GraphWalker nodeOriginTagger = new HiveParserDefaultGraphWalker(nodeOriginDispatcher);
            nodeOriginTagger.startWalking(Collections.singleton(viewTree), null);
        } catch (HiveASTParseException e) {
            // A user could encounter this if a stored view definition contains
            // an old SQL construct which has been eliminated in a later Hive
            // version, so we need to provide full debugging info to help
            // with fixing the view definition.
            LOG.error(org.apache.hadoop.util.StringUtils.stringifyException(e));
            throw new SemanticException(e.getMessage(), e);
        }
        HiveParserQBExpr qbexpr = new HiveParserQBExpr(alias);
        doPhase1QBExpr(viewTree, qbexpr, qb.getId(), alias, true);
        // if skip authorization, skip checking;
        // if it is inside a view, skip checking;
        // if authorization flag is not enabled, skip checking.
        // if HIVE_STATS_COLLECT_SCANCOLS is enabled, check.
        if ((!this.skipAuthorization()
                        && !qb.isInsideView()
                        && HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_AUTHORIZATION_ENABLED))
                || HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_STATS_COLLECT_SCANCOLS)) {
            qb.rewriteViewToSubq(alias, tabName, qbexpr, tab);
        } else {
            qb.rewriteViewToSubq(alias, tabName, qbexpr, null);
        }
    }

    private boolean skipAuthorization() {
        return true;
    }

    @SuppressWarnings("nls")
    // TODO: make aliases unique, otherwise needless rewriting takes place
    public Integer genColListRegex(
            String colRegex,
            String tabAlias,
            HiveParserASTNode sel,
            ArrayList<ExprNodeDesc> colList,
            HashSet<ColumnInfo> excludeCols,
            HiveParserRowResolver input,
            HiveParserRowResolver colSrcRR,
            Integer pos,
            HiveParserRowResolver output,
            List<String> aliases,
            boolean ensureUniqueCols)
            throws SemanticException {
        if (colSrcRR == null) {
            colSrcRR = input;
        }
        // The table alias should exist
        if (tabAlias != null && !colSrcRR.hasTableAlias(tabAlias)) {
            throw new SemanticException(
                    HiveParserErrorMsg.getMsg(ErrorMsg.INVALID_TABLE_ALIAS, sel));
        }

        // TODO: Have to put in the support for AS clause
        Pattern regex;
        try {
            regex = Pattern.compile(colRegex, Pattern.CASE_INSENSITIVE);
        } catch (PatternSyntaxException e) {
            throw new SemanticException(
                    HiveParserErrorMsg.getMsg(ErrorMsg.INVALID_COLUMN, sel, e.getMessage()));
        }

        StringBuilder replacementText = new StringBuilder();
        int matched = 0;
        // add empty string to the list of aliases. Some operators (ex. GroupBy) add
        // ColumnInfos for table alias "".
        if (!aliases.contains("")) {
            aliases.add("");
        }
        /*
         * track the input ColumnInfos that are added to the output.
         * if a columnInfo has multiple mappings; then add the column only once,
         * but carry the mappings forward.
         */
        Map<ColumnInfo, ColumnInfo> inputColsProcessed = new HashMap<>();
        // For expr "*", aliases should be iterated in the order they are specified in the query.

        if (colSrcRR.getNamedJoinInfo() != null) {
            // We got using() clause in previous join. Need to generate select list as
            // per standard. For * we will have joining columns first non-repeated
            // followed by other columns.
            HashMap<String, ColumnInfo> leftMap =
                    colSrcRR.getFieldMap(colSrcRR.getNamedJoinInfo().getAliases().get(0));
            HashMap<String, ColumnInfo> rightMap =
                    colSrcRR.getFieldMap(colSrcRR.getNamedJoinInfo().getAliases().get(1));
            HashMap<String, ColumnInfo> chosenMap = null;
            if (colSrcRR.getNamedJoinInfo().getHiveJoinType() != JoinType.RIGHTOUTER) {
                chosenMap = leftMap;
            } else {
                chosenMap = rightMap;
            }
            // first get the columns in named columns
            for (String columnName : colSrcRR.getNamedJoinInfo().getNamedColumns()) {
                for (Map.Entry<String, ColumnInfo> entry : chosenMap.entrySet()) {
                    ColumnInfo colInfo = entry.getValue();
                    if (!columnName.equals(colInfo.getAlias())) {
                        continue;
                    }
                    String name = colInfo.getInternalName();
                    String[] tmp = colSrcRR.reverseLookup(name);

                    // Skip the colinfos which are not for this particular alias
                    if (tabAlias != null && !tmp[0].equalsIgnoreCase(tabAlias)) {
                        continue;
                    }

                    if (colInfo.getIsVirtualCol() && colInfo.isHiddenVirtualCol()) {
                        continue;
                    }
                    ColumnInfo oColInfo = inputColsProcessed.get(colInfo);
                    if (oColInfo == null) {
                        ExprNodeColumnDesc expr =
                                new ExprNodeColumnDesc(
                                        colInfo.getType(),
                                        name,
                                        colInfo.getTabAlias(),
                                        colInfo.getIsVirtualCol(),
                                        colInfo.isSkewedCol());
                        colList.add(expr);
                        oColInfo =
                                new ColumnInfo(
                                        getColumnInternalName(pos),
                                        colInfo.getType(),
                                        colInfo.getTabAlias(),
                                        colInfo.getIsVirtualCol(),
                                        colInfo.isHiddenVirtualCol());
                        inputColsProcessed.put(colInfo, oColInfo);
                    }
                    if (ensureUniqueCols) {
                        if (!output.putWithCheck(tmp[0], tmp[1], null, oColInfo)) {
                            throw new SemanticException(
                                    "Cannot add column to RR: "
                                            + tmp[0]
                                            + "."
                                            + tmp[1]
                                            + " => "
                                            + oColInfo
                                            + " due to duplication, see previous warnings");
                        }
                    } else {
                        output.put(tmp[0], tmp[1], oColInfo);
                    }
                    pos = pos + 1;
                    matched++;

                    if (unparseTranslator.isEnabled()) {
                        if (replacementText.length() > 0) {
                            replacementText.append(", ");
                        }
                        replacementText.append(HiveUtils.unparseIdentifier(tmp[0], conf));
                        replacementText.append(".");
                        replacementText.append(HiveUtils.unparseIdentifier(tmp[1], conf));
                    }
                }
            }
        }
        for (String alias : aliases) {
            HashMap<String, ColumnInfo> fMap = colSrcRR.getFieldMap(alias);
            if (fMap == null) {
                continue;
            }
            // For the tab.* case, add all the columns to the fieldList from the input schema
            for (Map.Entry<String, ColumnInfo> entry : fMap.entrySet()) {
                ColumnInfo colInfo = entry.getValue();
                if (colSrcRR.getNamedJoinInfo() != null
                        && colSrcRR.getNamedJoinInfo()
                                .getNamedColumns()
                                .contains(colInfo.getAlias())) {
                    // we already added this column in select list.
                    continue;
                }
                if (excludeCols != null && excludeCols.contains(colInfo)) {
                    continue; // This was added during plan generation.
                }
                // First, look up the column from the source against which * is to be resolved.
                // We'd later translated this into the column from proper input, if it's valid.
                // TODO: excludeCols may be possible to remove using the same technique.
                String name = colInfo.getInternalName();
                String[] tmp = colSrcRR.reverseLookup(name);

                // Skip the colinfos which are not for this particular alias
                if (tabAlias != null && !tmp[0].equalsIgnoreCase(tabAlias)) {
                    continue;
                }

                if (colInfo.getIsVirtualCol() && colInfo.isHiddenVirtualCol()) {
                    continue;
                }

                // Not matching the regex?
                if (!regex.matcher(tmp[1]).matches()) {
                    continue;
                }

                // If input (GBY) is different than the source of columns, find the
                // same column in input.
                // TODO: This is fraught with peril.
                if (input != colSrcRR) {
                    colInfo = input.get(tabAlias, tmp[1]);
                    if (colInfo == null) {
                        LOG.error(
                                "Cannot find colInfo for "
                                        + tabAlias
                                        + "."
                                        + tmp[1]
                                        + ", derived from ["
                                        + colSrcRR
                                        + "], in ["
                                        + input
                                        + "]");
                        throw new SemanticException(ErrorMsg.NON_KEY_EXPR_IN_GROUPBY, tmp[1]);
                    }
                    String oldCol = null;
                    if (LOG.isDebugEnabled()) {
                        oldCol = name + " => " + (tmp == null ? "null" : (tmp[0] + "." + tmp[1]));
                    }
                    name = colInfo.getInternalName();
                    tmp = input.reverseLookup(name);
                    if (LOG.isDebugEnabled()) {
                        String newCol =
                                name + " => " + (tmp == null ? "null" : (tmp[0] + "." + tmp[1]));
                        LOG.debug("Translated [" + oldCol + "] to [" + newCol + "]");
                    }
                }

                ColumnInfo oColInfo = inputColsProcessed.get(colInfo);
                if (oColInfo == null) {
                    ExprNodeColumnDesc expr =
                            new ExprNodeColumnDesc(
                                    colInfo.getType(),
                                    name,
                                    colInfo.getTabAlias(),
                                    colInfo.getIsVirtualCol(),
                                    colInfo.isSkewedCol());
                    colList.add(expr);
                    oColInfo =
                            new ColumnInfo(
                                    getColumnInternalName(pos),
                                    colInfo.getType(),
                                    colInfo.getTabAlias(),
                                    colInfo.getIsVirtualCol(),
                                    colInfo.isHiddenVirtualCol());
                    inputColsProcessed.put(colInfo, oColInfo);
                }
                if (ensureUniqueCols) {
                    if (!output.putWithCheck(tmp[0], tmp[1], null, oColInfo)) {
                        throw new SemanticException(
                                "Cannot add column to RR: "
                                        + tmp[0]
                                        + "."
                                        + tmp[1]
                                        + " => "
                                        + oColInfo
                                        + " due to duplication, see previous warnings");
                    }
                } else {
                    output.put(tmp[0], tmp[1], oColInfo);
                }
                pos++;
                matched++;

                if (unparseTranslator.isEnabled()) {
                    if (replacementText.length() > 0) {
                        replacementText.append(", ");
                    }
                    replacementText.append(HiveUtils.unparseIdentifier(tmp[0], conf));
                    replacementText.append(".");
                    replacementText.append(HiveUtils.unparseIdentifier(tmp[1], conf));
                }
            }
        }

        if (matched == 0) {
            throw new SemanticException(HiveParserErrorMsg.getMsg(ErrorMsg.INVALID_COLUMN, sel));
        }

        if (unparseTranslator.isEnabled()) {
            unparseTranslator.addTranslation(sel, replacementText.toString());
        }
        return pos;
    }

    public String recommendName(ExprNodeDesc exp, String colAlias) {
        if (!colAlias.startsWith(autogenColAliasPrfxLbl)) {
            return null;
        }
        String column = ExprNodeDescUtils.recommendInputName(exp);
        if (column != null && !column.startsWith(autogenColAliasPrfxLbl)) {
            return column;
        }
        return null;
    }

    public String getAutogenColAliasPrfxLbl() {
        return this.autogenColAliasPrfxLbl;
    }

    public boolean autogenColAliasPrfxIncludeFuncName() {
        return this.autogenColAliasPrfxIncludeFuncName;
    }

    public void checkExpressionsForGroupingSet(
            List<HiveParserASTNode> grpByExprs,
            List<HiveParserASTNode> distinctGrpByExprs,
            Map<String, HiveParserASTNode> aggregationTrees,
            HiveParserRowResolver inputRowResolver)
            throws SemanticException {

        Set<String> colNamesGroupByExprs = new HashSet<>();
        Set<String> colNamesGroupByDistinctExprs = new HashSet<>();
        Set<String> colNamesAggregateParameters = new HashSet<>();

        // The columns in the group by expressions should not intersect with the columns in the
        // distinct expressions
        for (HiveParserASTNode grpByExpr : grpByExprs) {
            HiveParserUtils.extractColumns(
                    colNamesGroupByExprs, genExprNodeDesc(grpByExpr, inputRowResolver));
        }

        // If there is a distinctFuncExp, add all parameters to the reduceKeys.
        if (!distinctGrpByExprs.isEmpty()) {
            for (HiveParserASTNode value : distinctGrpByExprs) {
                // 0 is function name
                for (int i = 1; i < value.getChildCount(); i++) {
                    HiveParserASTNode parameter = (HiveParserASTNode) value.getChild(i);
                    ExprNodeDesc distExprNode = genExprNodeDesc(parameter, inputRowResolver);
                    // extract all the columns
                    HiveParserUtils.extractColumns(colNamesGroupByDistinctExprs, distExprNode);
                }

                if (HiveParserUtils.hasCommonElement(
                        colNamesGroupByExprs, colNamesGroupByDistinctExprs)) {
                    throw new SemanticException(
                            ErrorMsg.HIVE_GROUPING_SETS_AGGR_EXPRESSION_INVALID.getMsg());
                }
            }
        }

        for (Map.Entry<String, HiveParserASTNode> entry : aggregationTrees.entrySet()) {
            HiveParserASTNode value = entry.getValue();
            // 0 is the function name
            for (int i = 1; i < value.getChildCount(); i++) {
                HiveParserASTNode paraExpr = (HiveParserASTNode) value.getChild(i);
                ExprNodeDesc paraExprNode = genExprNodeDesc(paraExpr, inputRowResolver);

                // extract all the columns
                HiveParserUtils.extractColumns(colNamesAggregateParameters, paraExprNode);
            }

            if (HiveParserUtils.hasCommonElement(
                    colNamesGroupByExprs, colNamesAggregateParameters)) {
                throw new SemanticException(
                        ErrorMsg.HIVE_GROUPING_SETS_AGGR_EXPRESSION_INVALID.getMsg());
            }
        }
    }

    public void init(boolean clearPartsCache) {
        // clear most members
        reset(clearPartsCache);

        // init
        this.qb = new HiveParserQB(null, null, false);
    }

    private Table getTableObjectByName(String tableName) throws HiveException {
        if (!tabNameToTabObject.containsKey(tableName)) {
            Table table = db.getTable(tableName);
            tabNameToTabObject.put(tableName, table);
            return table;
        } else {
            return tabNameToTabObject.get(tableName);
        }
    }

    public boolean genResolvedParseTree(HiveParserASTNode ast, HiveParserPlannerContext plannerCtx)
            throws SemanticException {
        this.ast = ast;
        viewsExpanded = new ArrayList<>();
        ctesExpanded = new ArrayList<>();

        // 1. analyze and process the position alias
        // step processPositionAlias out of genResolvedParseTree

        // 2. analyze create table command
        // create table won't get here
        queryState.setCommandType(HiveOperation.QUERY);

        // 3. analyze create view command
        // create view won't get here

        // 4. continue analyzing from the child HiveParserASTNode.
        HiveParserBaseSemanticAnalyzer.Phase1Ctx ctx1 = initPhase1Ctx();
        preProcessForInsert(ast);
        if (!doPhase1(ast, qb, ctx1, plannerCtx)) {
            // if phase1Result false return
            return false;
        }
        LOG.info("Completed phase 1 of Semantic Analysis");

        // 5. Resolve Parse Tree
        // Materialization is allowed if it is not a view definition
        getMetaData(qb, createVwDesc == null);
        LOG.info("Completed getting MetaData in Semantic Analysis");
        plannerCtx.setParseTreeAttr(ast, ctx1);
        return true;
    }

    /**
     * This will walk AST of an INSERT statement and assemble a list of target tables which are in
     * an HDFS encryption zone. This is needed to make sure that so that the data from values clause
     * of Insert ... select values(...) is stored securely. See also {@link
     * #genValuesTempTable(HiveParserASTNode, HiveParserQB)}
     */
    private void preProcessForInsert(HiveParserASTNode node) throws SemanticException {
        try {
            if (!(node != null
                    && node.getToken() != null
                    && node.getToken().getType() == HiveASTParser.TOK_QUERY)) {
                return;
            }
            for (Node child : node.getChildren()) {
                // each insert of multi insert looks like
                // (TOK_INSERT (TOK_INSERT_INTO (TOK_TAB (TOK_TABNAME T1)))
                if (((HiveParserASTNode) child).getToken().getType() != HiveASTParser.TOK_INSERT) {
                    continue;
                }
                HiveParserASTNode n =
                        (HiveParserASTNode)
                                ((HiveParserASTNode) child)
                                        .getFirstChildWithType(HiveASTParser.TOK_INSERT_INTO);
                if (n == null) {
                    continue;
                }
                n = (HiveParserASTNode) n.getFirstChildWithType(HiveASTParser.TOK_TAB);
                if (n == null) {
                    continue;
                }
                n = (HiveParserASTNode) n.getFirstChildWithType(HiveASTParser.TOK_TABNAME);
                if (n == null) {
                    continue;
                }
                String[] dbTab = getQualifiedTableName(n);
                db.getTable(dbTab[0], dbTab[1]);
            }
        } catch (Exception ex) {
            throw new SemanticException(ex);
        }
    }

    // Generates an expression node descriptor for the expression with HiveParserTypeCheckCtx.
    public ExprNodeDesc genExprNodeDesc(HiveParserASTNode expr, HiveParserRowResolver input)
            throws SemanticException {
        // Since the user didn't supply a customized type-checking context,
        // use default settings.
        return genExprNodeDesc(expr, input, true, false);
    }

    public ExprNodeDesc genExprNodeDesc(
            HiveParserASTNode expr,
            HiveParserRowResolver input,
            HiveParserRowResolver outerRR,
            Map<HiveParserASTNode, RelNode> subqueryToRelNode,
            boolean useCaching)
            throws SemanticException {

        HiveParserTypeCheckCtx tcCtx =
                new HiveParserTypeCheckCtx(input, useCaching, false, frameworkConfig, cluster);
        tcCtx.setOuterRR(outerRR);
        tcCtx.setSubqueryToRelNode(subqueryToRelNode);
        return genExprNodeDesc(expr, input, tcCtx);
    }

    private ExprNodeDesc genExprNodeDesc(
            HiveParserASTNode expr,
            HiveParserRowResolver input,
            boolean useCaching,
            boolean foldExpr)
            throws SemanticException {
        HiveParserTypeCheckCtx tcCtx =
                new HiveParserTypeCheckCtx(input, useCaching, foldExpr, frameworkConfig, cluster);
        return genExprNodeDesc(expr, input, tcCtx);
    }

    /**
     * Generates an expression node descriptors for the expression and children of it with default
     * HiveParserTypeCheckCtx.
     */
    public Map<HiveParserASTNode, ExprNodeDesc> genAllExprNodeDesc(
            HiveParserASTNode expr, HiveParserRowResolver input) throws SemanticException {
        HiveParserTypeCheckCtx tcCtx = new HiveParserTypeCheckCtx(input, frameworkConfig, cluster);
        return genAllExprNodeDesc(expr, input, tcCtx);
    }

    /**
     * Returns expression node descriptor for the expression. If it's evaluated already in previous
     * operator, it can be retrieved from cache.
     */
    public ExprNodeDesc genExprNodeDesc(
            HiveParserASTNode expr, HiveParserRowResolver input, HiveParserTypeCheckCtx tcCtx)
            throws SemanticException {
        // We recursively create the exprNodeDesc. Base cases: when we encounter
        // a column ref, we convert that into an exprNodeColumnDesc; when we
        // encounter
        // a constant, we convert that into an exprNodeConstantDesc. For others we
        // just
        // build the exprNodeFuncDesc with recursively built children.

        // If the current subExpression is pre-calculated, as in Group-By etc.
        ExprNodeDesc cached = null;
        if (tcCtx.isUseCaching()) {
            cached = getExprNodeDescCached(expr, input);
        }
        if (cached == null) {
            Map<HiveParserASTNode, ExprNodeDesc> allExprs = genAllExprNodeDesc(expr, input, tcCtx);
            return allExprs.get(expr);
        }
        return cached;
    }

    // Find ExprNodeDesc for the expression cached in the HiveParserRowResolver. Returns null if not
    // exists.
    private ExprNodeDesc getExprNodeDescCached(HiveParserASTNode expr, HiveParserRowResolver input)
            throws SemanticException {
        ColumnInfo colInfo = input.getExpression(expr);
        if (colInfo != null) {
            HiveParserASTNode source = input.getExpressionSource(expr);
            if (source != null) {
                unparseTranslator.addCopyTranslation(expr, source);
            }
            return new ExprNodeColumnDesc(
                    colInfo.getType(),
                    colInfo.getInternalName(),
                    colInfo.getTabAlias(),
                    colInfo.getIsVirtualCol(),
                    colInfo.isSkewedCol());
        }
        return null;
    }

    /**
     * Generates all of the expression node descriptors for the expression and children of it passed
     * in the arguments. This function uses the row resolver and the metadata information that are
     * passed as arguments to resolve the column names to internal names.
     */
    @SuppressWarnings("nls")
    public Map<HiveParserASTNode, ExprNodeDesc> genAllExprNodeDesc(
            HiveParserASTNode expr, HiveParserRowResolver input, HiveParserTypeCheckCtx tcCtx)
            throws SemanticException {
        // Create the walker and  the rules dispatcher.
        tcCtx.setUnparseTranslator(unparseTranslator);

        Map<HiveParserASTNode, ExprNodeDesc> nodeOutputs =
                HiveParserTypeCheckProcFactory.genExprNode(expr, tcCtx);
        ExprNodeDesc desc = nodeOutputs.get(expr);
        if (desc == null) {
            String errMsg = tcCtx.getError();
            if (errMsg == null) {
                errMsg = "Error in parsing ";
            }
            throw new SemanticException(errMsg);
        }
        if (desc instanceof HiveParserExprNodeColumnListDesc) {
            throw new SemanticException("TOK_ALLCOLREF is not supported in current context");
        }

        if (!unparseTranslator.isEnabled()) {
            // Not creating a view, so no need to track view expansions.
            return nodeOutputs;
        }

        Map<ExprNodeDesc, String> nodeToText = new HashMap<>();
        List<HiveParserASTNode> fieldDescList = new ArrayList<>();

        for (Map.Entry<HiveParserASTNode, ExprNodeDesc> entry : nodeOutputs.entrySet()) {
            if (!(entry.getValue() instanceof ExprNodeColumnDesc)) {
                // we need to translate the ExprNodeFieldDesc too, e.g., identifiers in
                // struct<>.
                if (entry.getValue() instanceof ExprNodeFieldDesc) {
                    fieldDescList.add(entry.getKey());
                }
                continue;
            }
            HiveParserASTNode node = entry.getKey();
            ExprNodeColumnDesc columnDesc = (ExprNodeColumnDesc) entry.getValue();
            if ((columnDesc.getTabAlias() == null) || (columnDesc.getTabAlias().length() == 0)) {
                // These aren't real column refs; instead, they are special
                // internal expressions used in the representation of aggregation.
                continue;
            }
            String[] tmp = input.reverseLookup(columnDesc.getColumn());
            // in subquery case, tmp may be from outside.
            if (tmp[0] != null
                    && columnDesc.getTabAlias() != null
                    && !tmp[0].equals(columnDesc.getTabAlias())
                    && tcCtx.getOuterRR() != null) {
                tmp = tcCtx.getOuterRR().reverseLookup(columnDesc.getColumn());
            }
            StringBuilder replacementText = new StringBuilder();
            replacementText.append(HiveUtils.unparseIdentifier(tmp[0], conf));
            replacementText.append(".");
            replacementText.append(HiveUtils.unparseIdentifier(tmp[1], conf));
            nodeToText.put(columnDesc, replacementText.toString());
            unparseTranslator.addTranslation(node, replacementText.toString());
        }

        for (HiveParserASTNode node : fieldDescList) {
            Map<HiveParserASTNode, String> map = translateFieldDesc(node);
            for (Entry<HiveParserASTNode, String> entry : map.entrySet()) {
                unparseTranslator.addTranslation(entry.getKey(), entry.getValue());
            }
        }

        return nodeOutputs;
    }

    private Map<HiveParserASTNode, String> translateFieldDesc(HiveParserASTNode node) {
        Map<HiveParserASTNode, String> map = new HashMap<>();
        if (node.getType() == HiveASTParser.DOT) {
            for (Node child : node.getChildren()) {
                map.putAll(translateFieldDesc((HiveParserASTNode) child));
            }
        } else if (node.getType() == HiveASTParser.Identifier) {
            map.put(node, HiveUtils.unparseIdentifier(node.getText(), conf));
        }
        return map;
    }

    public HiveParserQB getQB() {
        return qb;
    }

    public void setQB(HiveParserQB qb) {
        this.qb = qb;
    }

    // --------------------------- PTF handling -----------------------------------

    /*
     * - a partitionTableFunctionSource can be a tableReference, a SubQuery or another
     *   PTF invocation.
     * - For a TABLEREF: set the source to the alias returned by processTable
     * - For a SubQuery: set the source to the alias returned by processSubQuery
     * - For a PTF invocation: recursively call processPTFChain.
     */
    private PTFInputSpec processPTFSource(HiveParserQB qb, HiveParserASTNode inputNode)
            throws SemanticException {

        PTFInputSpec qInSpec = null;
        int type = inputNode.getType();
        String alias;
        switch (type) {
            case HiveASTParser.TOK_TABREF:
                alias = processTable(qb, inputNode);
                qInSpec = new PTFQueryInputSpec();
                ((PTFQueryInputSpec) qInSpec).setType(PTFQueryInputType.TABLE);
                ((PTFQueryInputSpec) qInSpec).setSource(alias);
                break;
            case HiveASTParser.TOK_SUBQUERY:
                alias = processSubQuery(qb, inputNode);
                qInSpec = new PTFQueryInputSpec();
                ((PTFQueryInputSpec) qInSpec).setType(PTFQueryInputType.SUBQUERY);
                ((PTFQueryInputSpec) qInSpec).setSource(alias);
                break;
            case HiveASTParser.TOK_PTBLFUNCTION:
                qInSpec = processPTFChain(qb, inputNode);
                break;
            default:
                throw new SemanticException(
                        HiveParserUtils.generateErrorMessage(
                                inputNode, "Unknown input type to PTF"));
        }

        qInSpec.setAstNode(inputNode);
        return qInSpec;
    }

    /*
     * - tree form is
     *   ^(TOK_PTBLFUNCTION name alias? partitionTableFunctionSource partitioningSpec? arguments*)
     * - a partitionTableFunctionSource can be a tableReference, a SubQuery or another
     *   PTF invocation.
     */
    private PartitionedTableFunctionSpec processPTFChain(HiveParserQB qb, HiveParserASTNode ptf)
            throws SemanticException {
        int childCount = ptf.getChildCount();
        if (childCount < 2) {
            throw new SemanticException(
                    HiveParserUtils.generateErrorMessage(ptf, "Not enough Children " + childCount));
        }

        PartitionedTableFunctionSpec ptfSpec = new PartitionedTableFunctionSpec();
        ptfSpec.setAstNode(ptf);

        // name
        HiveParserASTNode nameNode = (HiveParserASTNode) ptf.getChild(0);
        ptfSpec.setName(nameNode.getText());

        int inputIdx = 1;

        // alias
        HiveParserASTNode secondChild = (HiveParserASTNode) ptf.getChild(1);
        if (secondChild.getType() == HiveASTParser.Identifier) {
            ptfSpec.setAlias(secondChild.getText());
            inputIdx++;
        }

        // input
        HiveParserASTNode inputNode = (HiveParserASTNode) ptf.getChild(inputIdx);
        ptfSpec.setInput(processPTFSource(qb, inputNode));

        int argStartIdx = inputIdx + 1;

        // partitioning Spec
        int pSpecIdx = inputIdx + 1;
        HiveParserASTNode pSpecNode =
                ptf.getChildCount() > inputIdx ? (HiveParserASTNode) ptf.getChild(pSpecIdx) : null;
        if (pSpecNode != null && pSpecNode.getType() == HiveASTParser.TOK_PARTITIONINGSPEC) {
            PartitioningSpec partitioning = processPTFPartitionSpec(pSpecNode);
            ptfSpec.setPartitioning(partitioning);
            argStartIdx++;
        }

        // arguments
        for (int i = argStartIdx; i < ptf.getChildCount(); i++) {
            ptfSpec.addArg((HiveParserASTNode) ptf.getChild(i));
        }
        return ptfSpec;
    }

    /*
     * - invoked during FROM AST tree processing, on encountering a PTF invocation.
     * - tree form is
     *   ^(TOK_PTBLFUNCTION name partitionTableFunctionSource partitioningSpec? arguments*)
     * - setup a HiveParserPTFInvocationSpec for this top level PTF invocation.
     */
    private void processPTF(HiveParserQB qb, HiveParserASTNode ptf) throws SemanticException {

        PartitionedTableFunctionSpec ptfSpec = processPTFChain(qb, ptf);

        if (ptfSpec.getAlias() != null) {
            qb.addAlias(ptfSpec.getAlias());
        }

        HiveParserPTFInvocationSpec spec = new HiveParserPTFInvocationSpec();
        spec.setFunction(ptfSpec);
        qb.addPTFNodeToSpec(ptf, spec);
    }
}
