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

import org.apache.flink.connectors.hive.HiveInternalOptions;
import org.apache.flink.table.api.SqlParserException;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.catalog.hive.client.HiveShim;
import org.apache.flink.table.catalog.hive.client.HiveShimLoader;
import org.apache.flink.table.module.hive.udf.generic.HiveGenericUDFGrouping;
import org.apache.flink.table.operations.ExplainOperation;
import org.apache.flink.table.operations.HiveSetOperation;
import org.apache.flink.table.operations.ModifyOperation;
import org.apache.flink.table.operations.NopOperation;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.StatementSetOperation;
import org.apache.flink.table.operations.command.AddJarOperation;
import org.apache.flink.table.planner.calcite.FlinkPlannerImpl;
import org.apache.flink.table.planner.delegation.ParserImpl;
import org.apache.flink.table.planner.delegation.PlannerContext;
import org.apache.flink.table.planner.delegation.hive.copy.HiveASTParseException;
import org.apache.flink.table.planner.delegation.hive.copy.HiveASTParseUtils;
import org.apache.flink.table.planner.delegation.hive.copy.HiveParserASTNode;
import org.apache.flink.table.planner.delegation.hive.copy.HiveParserContext;
import org.apache.flink.table.planner.delegation.hive.copy.HiveParserQueryState;
import org.apache.flink.table.planner.delegation.hive.parse.HiveASTParser;
import org.apache.flink.table.planner.delegation.hive.parse.HiveParserCreateViewInfo;
import org.apache.flink.table.planner.delegation.hive.parse.HiveParserDDLSemanticAnalyzer;
import org.apache.flink.table.planner.delegation.hive.parse.HiveParserLoadSemanticAnalyzer;
import org.apache.flink.table.planner.operations.PlannerQueryOperation;
import org.apache.flink.table.planner.parse.CalciteParser;
import org.apache.flink.table.planner.plan.FlinkCalciteCatalogReader;
import org.apache.flink.util.Preconditions;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.VariableSubstitution;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.processors.HiveCommand;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

import static org.apache.flink.table.planner.delegation.hive.copy.HiveSetProcessor.startWithHiveSpecialVariablePrefix;

/** A Parser that uses Hive's planner to parse a statement. */
public class HiveParser extends ParserImpl {

    private static final Logger LOG = LoggerFactory.getLogger(HiveParser.class);

    // need to maintain the HiveParserASTNode types for DDLs
    private static final Set<Integer> DDL_NODES;

    static {
        DDL_NODES =
                new HashSet<>(
                        Arrays.asList(
                                HiveASTParser.TOK_ALTERTABLE,
                                HiveASTParser.TOK_ALTERVIEW,
                                HiveASTParser.TOK_CREATEDATABASE,
                                HiveASTParser.TOK_DROPDATABASE,
                                HiveASTParser.TOK_SWITCHDATABASE,
                                HiveASTParser.TOK_DROPTABLE,
                                HiveASTParser.TOK_DROPVIEW,
                                HiveASTParser.TOK_DROP_MATERIALIZED_VIEW,
                                HiveASTParser.TOK_DESCDATABASE,
                                HiveASTParser.TOK_DESCTABLE,
                                HiveASTParser.TOK_DESCFUNCTION,
                                HiveASTParser.TOK_MSCK,
                                HiveASTParser.TOK_ALTERINDEX_REBUILD,
                                HiveASTParser.TOK_ALTERINDEX_PROPERTIES,
                                HiveASTParser.TOK_SHOWDATABASES,
                                HiveASTParser.TOK_SHOWTABLES,
                                HiveASTParser.TOK_SHOWCOLUMNS,
                                HiveASTParser.TOK_SHOW_TABLESTATUS,
                                HiveASTParser.TOK_SHOW_TBLPROPERTIES,
                                HiveASTParser.TOK_SHOW_CREATEDATABASE,
                                HiveASTParser.TOK_SHOW_CREATETABLE,
                                HiveASTParser.TOK_SHOWFUNCTIONS,
                                HiveASTParser.TOK_SHOWPARTITIONS,
                                HiveASTParser.TOK_SHOWINDEXES,
                                HiveASTParser.TOK_SHOWLOCKS,
                                HiveASTParser.TOK_SHOWDBLOCKS,
                                HiveASTParser.TOK_SHOW_COMPACTIONS,
                                HiveASTParser.TOK_SHOW_TRANSACTIONS,
                                HiveASTParser.TOK_ABORT_TRANSACTIONS,
                                HiveASTParser.TOK_SHOWCONF,
                                HiveASTParser.TOK_SHOWVIEWS,
                                HiveASTParser.TOK_CREATEINDEX,
                                HiveASTParser.TOK_DROPINDEX,
                                HiveASTParser.TOK_ALTERTABLE_CLUSTER_SORT,
                                HiveASTParser.TOK_LOCKTABLE,
                                HiveASTParser.TOK_UNLOCKTABLE,
                                HiveASTParser.TOK_LOCKDB,
                                HiveASTParser.TOK_UNLOCKDB,
                                HiveASTParser.TOK_CREATEROLE,
                                HiveASTParser.TOK_DROPROLE,
                                HiveASTParser.TOK_GRANT,
                                HiveASTParser.TOK_REVOKE,
                                HiveASTParser.TOK_SHOW_GRANT,
                                HiveASTParser.TOK_GRANT_ROLE,
                                HiveASTParser.TOK_REVOKE_ROLE,
                                HiveASTParser.TOK_SHOW_ROLE_GRANT,
                                HiveASTParser.TOK_SHOW_ROLE_PRINCIPALS,
                                HiveASTParser.TOK_SHOW_ROLE_PRINCIPALS,
                                HiveASTParser.TOK_ALTERDATABASE_PROPERTIES,
                                HiveASTParser.TOK_ALTERDATABASE_OWNER,
                                HiveASTParser.TOK_TRUNCATETABLE,
                                HiveASTParser.TOK_SHOW_SET_ROLE,
                                HiveASTParser.TOK_CACHE_METADATA,
                                HiveASTParser.TOK_CREATEMACRO,
                                HiveASTParser.TOK_DROPMACRO,
                                HiveASTParser.TOK_CREATETABLE,
                                HiveASTParser.TOK_CREATEFUNCTION,
                                HiveASTParser.TOK_DROPFUNCTION,
                                HiveASTParser.TOK_RELOADFUNCTION,
                                HiveASTParser.TOK_CREATEVIEW,
                                HiveASTParser.TOK_ALTERDATABASE_LOCATION,
                                HiveASTParser.TOK_CREATE_MATERIALIZED_VIEW));
    }

    private final PlannerContext plannerContext;
    private final FlinkCalciteCatalogReader catalogReader;
    private final FrameworkConfig frameworkConfig;
    private final SqlFunctionConverter funcConverter;
    private final HiveParserDMLHelper dmlHelper;
    private final TableConfig tableConfig;
    private final Map<String, String> hiveVariables;

    HiveParser(
            CatalogManager catalogManager,
            Supplier<FlinkPlannerImpl> validatorSupplier,
            Supplier<CalciteParser> calciteParserSupplier,
            PlannerContext plannerContext) {
        super(
                catalogManager,
                validatorSupplier,
                calciteParserSupplier,
                plannerContext.getRexFactory());
        this.plannerContext = plannerContext;
        this.catalogReader = plannerContext.createCatalogReader(false);
        this.frameworkConfig = plannerContext.createFrameworkConfig();
        this.funcConverter =
                new SqlFunctionConverter(
                        plannerContext.getCluster(),
                        frameworkConfig.getOperatorTable(),
                        catalogReader.nameMatcher());
        this.dmlHelper = new HiveParserDMLHelper(plannerContext, funcConverter, catalogManager);
        this.tableConfig = plannerContext.getFlinkContext().getTableConfig();
        this.hiveVariables = tableConfig.get(HiveInternalOptions.HIVE_VARIABLES);
    }

    @Override
    public List<Operation> parse(String statement) {
        CatalogManager catalogManager = getCatalogManager();
        Catalog currentCatalog =
                catalogManager.getCatalog(catalogManager.getCurrentCatalog()).orElse(null);
        if (!(currentCatalog instanceof HiveCatalog)) {
            LOG.warn("Current catalog is not HiveCatalog. Falling back to Flink's planner.");
            return super.parse(statement);
        }

        Optional<Operation> nonSqlOperation =
                tryProcessHiveNonSqlStatement(
                        ((HiveCatalog) currentCatalog).getHiveConf(), statement);
        if (nonSqlOperation.isPresent()) {
            return Collections.singletonList(nonSqlOperation.get());
        }
        HiveConf hiveConf = new HiveConf(((HiveCatalog) currentCatalog).getHiveConf());
        hiveConf.setVar(HiveConf.ConfVars.DYNAMICPARTITIONINGMODE, "nonstrict");
        hiveConf.set("hive.allow.udf.load.on.demand", "false");
        hiveConf.setVar(HiveConf.ConfVars.HIVE_EXECUTION_ENGINE, "mr");
        HiveShim hiveShim =
                HiveShimLoader.loadHiveShim(((HiveCatalog) currentCatalog).getHiveVersion());
        try {
            // substitute variables for the statement
            statement = substituteVariables(hiveConf, statement);
            // creates SessionState
            HiveSessionState.startSessionState(hiveConf, catalogManager);
            // We override Hive's grouping function. Refer to the implementation for more details.
            hiveShim.registerTemporaryFunction("grouping", HiveGenericUDFGrouping.class);
            return processCmd(statement, hiveConf, hiveShim, (HiveCatalog) currentCatalog);
        } finally {
            HiveSessionState.clearSessionState();
        }
    }

    private Optional<Operation> tryProcessHiveNonSqlStatement(HiveConf hiveConf, String statement) {
        String[] commandTokens = statement.split("\\s+");
        HiveCommand hiveCommand = HiveCommand.find(commandTokens);
        if (hiveCommand != null) {
            String cmdArgs = statement.substring(commandTokens[0].length()).trim();
            // the command may end with ";" since it won't be removed by Flink SQL CLI,
            // so, we need to remove ";"
            if (cmdArgs.endsWith(";")) {
                cmdArgs = cmdArgs.substring(0, cmdArgs.length() - 1);
            }
            if (hiveCommand == HiveCommand.SET) {
                return Optional.of(processSetCmd(statement, cmdArgs));
            } else if (hiveCommand == HiveCommand.RESET) {
                return Optional.of(super.parse(statement).get(0));
            } else if (hiveCommand == HiveCommand.ADD) {
                return Optional.of(processAddCmd(substituteVariables(hiveConf, cmdArgs)));
            } else {
                throw new UnsupportedOperationException(
                        String.format("The Hive command %s is not supported.", hiveCommand));
            }
        }
        return Optional.empty();
    }

    private Operation processSetCmd(String originCmd, String setCmdArgs) {
        if (setCmdArgs.equals("")) {
            return new HiveSetOperation();
        }
        if (setCmdArgs.equals("-v")) {
            return new HiveSetOperation(true);
        }

        String[] part = new String[2];
        int eqIndex = setCmdArgs.indexOf('=');
        if (setCmdArgs.contains("=")) {
            if (eqIndex == setCmdArgs.length() - 1) { // x=
                part[0] = setCmdArgs.substring(0, setCmdArgs.length() - 1);
                part[1] = "";
            } else { // x=y
                part[0] = setCmdArgs.substring(0, eqIndex).trim();
                part[1] = setCmdArgs.substring(eqIndex + 1).trim();
                if (!startWithHiveSpecialVariablePrefix(part[0])) {
                    // TODO:
                    // currently, for the command set key=value, we will fall to
                    // Flink's implementation, otherwise, user will have no way to switch dialect.
                    // need to figure out whether we should also set the value in HiveConf which is
                    // Hive's implementation
                    LOG.warn(
                            "The command 'set {}={}' will only set Flink's table config,"
                                    + " and if you want to set the variable to Hive's conf, please use the command like 'set hiveconf:{}={}'.",
                            part[0],
                            part[1],
                            part[0],
                            part[1]);
                    return super.parse(originCmd).get(0);
                }
            }
            if (part[0].equals("silent")) {
                throw new UnsupportedOperationException("Unsupported command 'set silent'.");
            }
            return new HiveSetOperation(part[0], part[1]);
        }
        return new HiveSetOperation(setCmdArgs);
    }

    /**
     * Substitute the variables in the statement. For statement 'select ${hiveconf:foo}', the
     * variable '${hiveconf:foo}' will be replaced with the actual value with key 'foo' in hive
     * conf.
     */
    private String substituteVariables(HiveConf conf, String statement) {
        return new VariableSubstitution(() -> hiveVariables).substitute(conf, statement);
    }

    private Operation processAddCmd(String addCmdArgs) {
        String[] tokens = addCmdArgs.split("\\s+");
        SessionState.ResourceType resourceType = SessionState.find_resource_type(tokens[0]);
        if (resourceType == SessionState.ResourceType.FILE) {
            throw new UnsupportedOperationException(
                    "ADD FILE is not supported yet. Usage: ADD JAR <file_path>");
        } else if (resourceType == SessionState.ResourceType.ARCHIVE) {
            throw new UnsupportedOperationException(
                    "ADD ARCHIVE is not supported yet. Usage: ADD JAR <file_path>");
        } else if (resourceType == SessionState.ResourceType.JAR) {
            if (tokens.length != 2) {
                throw new UnsupportedOperationException(
                        "Add multiple jar in one single statement is not supported yet. Usage: ADD JAR <file_path>");
            }
            return new AddJarOperation(tokens[1]);
        } else {
            throw new IllegalArgumentException(
                    String.format("Unknown resource type: %s.", tokens[0]));
        }
    }

    private List<Operation> processCmd(
            String cmd, HiveConf hiveConf, HiveShim hiveShim, HiveCatalog hiveCatalog) {
        try {
            HiveParserContext context = new HiveParserContext(hiveConf);
            // parse statement to get AST
            HiveParserASTNode node = HiveASTParseUtils.parse(cmd, context);
            if (DDL_NODES.contains(node.getType())) {
                HiveParserQueryState queryState = new HiveParserQueryState(hiveConf);
                HiveParserDDLSemanticAnalyzer ddlAnalyzer =
                        new HiveParserDDLSemanticAnalyzer(
                                queryState,
                                hiveCatalog,
                                getCatalogManager(),
                                this,
                                hiveShim,
                                context,
                                dmlHelper,
                                frameworkConfig,
                                plannerContext.getCluster(),
                                plannerContext.getFlinkContext().getClassLoader());
                return Collections.singletonList(ddlAnalyzer.convertToOperation(node));
            } else {
                return processQuery(context, hiveConf, hiveShim, node);
            }
        } catch (HiveASTParseException e) {
            // ParseException can happen for flink-specific statements, e.g. catalog DDLs
            try {
                return super.parse(cmd);
            } catch (SqlParserException parserException) {
                throw new SqlParserException("SQL parse failed", e);
            }
        } catch (SemanticException e) {
            throw new ValidationException("HiveParser failed to parse " + cmd, e);
        }
    }

    private List<Operation> processQuery(
            HiveParserContext context, HiveConf hiveConf, HiveShim hiveShim, HiveParserASTNode node)
            throws SemanticException {
        final boolean explain = node.getType() == HiveASTParser.TOK_EXPLAIN;
        // first child is the underlying explicandum
        HiveParserASTNode input = explain ? (HiveParserASTNode) node.getChild(0) : node;
        if (explain) {
            Operation operation = convertASTNodeToOperation(context, hiveConf, hiveShim, input);
            // explain a nop is also considered nop
            return Collections.singletonList(
                    operation instanceof NopOperation
                            ? operation
                            : new ExplainOperation(operation));
        }
        return Collections.singletonList(
                convertASTNodeToOperation(context, hiveConf, hiveShim, input));
    }

    private Operation convertASTNodeToOperation(
            HiveParserContext context,
            HiveConf hiveConf,
            HiveShim hiveShim,
            HiveParserASTNode input)
            throws SemanticException {
        if (isLoadData(input)) {
            HiveParserLoadSemanticAnalyzer loadSemanticAnalyzer =
                    new HiveParserLoadSemanticAnalyzer(
                            hiveConf, frameworkConfig, plannerContext.getCluster());
            return loadSemanticAnalyzer.convertToOperation(input);
        }
        if (isMultiDestQuery(input)) {
            return processMultiDestQuery(context, hiveConf, hiveShim, input);
        } else {
            return analyzeSql(context, hiveConf, hiveShim, input);
        }
    }

    private boolean isLoadData(HiveParserASTNode input) {
        return input.getType() == HiveASTParser.TOK_LOAD;
    }

    private boolean isMultiDestQuery(HiveParserASTNode astNode) {
        // Hive's multi dest insert will always be [FROM, INSERT+]
        // so, if it's children count is more than 2, and the first one
        // is FROM, others are INSERT nodes, it should be multi dest query
        if (astNode.getChildCount() > 2) {
            if (astNode.getChild(0).getType() == HiveASTParser.TOK_FROM) {
                // the others should be insert tokens
                for (int i = 1; i < astNode.getChildCount(); i++) {
                    if (astNode.getChild(i).getType() != HiveASTParser.TOK_INSERT) {
                        return false;
                    }
                }
                return true;
            }
        }
        return false;
    }

    private Operation processMultiDestQuery(
            HiveParserContext context,
            HiveConf hiveConf,
            HiveShim hiveShim,
            HiveParserASTNode astNode)
            throws SemanticException {
        List<Operation> operations = new ArrayList<>();
        // multi-insert statement may contain multi insert nodes,
        // the children nodes of the root AST will always be like [FROM, INSERT+]
        // we pop each insert node and process one by one to construct a list of insert operations
        List<HiveParserASTNode> insertASTNodes = new ArrayList<>();
        // pop the insert node one by one
        while (astNode.getChildCount() > 1) {
            insertASTNodes.add((HiveParserASTNode) astNode.deleteChild(1));
        }
        for (HiveParserASTNode insertASTNode : insertASTNodes) {
            // mount the insert node to the root AST, consider it as a normal AST and convert it to
            // operation
            astNode.addChild(insertASTNode);
            operations.add(analyzeSql(context, hiveConf, hiveShim, astNode));
            astNode.deleteChild(astNode.getChildCount() - 1);
        }
        // then we wrap them to StatementSetOperation
        List<ModifyOperation> modifyOperations = new ArrayList<>();
        for (Operation operation : operations) {
            Preconditions.checkArgument(
                    operation instanceof ModifyOperation,
                    "Encounter an non-ModifyOperation, "
                            + "only support insert when it contains multiple operations in one single SQL statement.");
            modifyOperations.add((ModifyOperation) operation);
        }
        return new StatementSetOperation(modifyOperations);
    }

    public HiveParserCalcitePlanner createCalcitePlanner(
            HiveParserContext context, HiveParserQueryState queryState, HiveShim hiveShim)
            throws SemanticException {
        HiveParserCalcitePlanner calciteAnalyzer =
                new HiveParserCalcitePlanner(
                        queryState,
                        plannerContext,
                        catalogReader,
                        frameworkConfig,
                        getCatalogManager(),
                        hiveShim);
        calciteAnalyzer.initCtx(context);
        calciteAnalyzer.init(false);
        return calciteAnalyzer;
    }

    public void analyzeCreateView(
            HiveParserCreateViewInfo createViewInfo,
            HiveParserContext context,
            HiveParserQueryState queryState,
            HiveShim hiveShim)
            throws SemanticException {
        HiveParserCalcitePlanner calciteAnalyzer =
                createCalcitePlanner(context, queryState, hiveShim);
        calciteAnalyzer.setCreatViewInfo(createViewInfo);
        calciteAnalyzer.genLogicalPlan(createViewInfo.getQuery());
    }

    private Operation analyzeSql(
            HiveParserContext context, HiveConf hiveConf, HiveShim hiveShim, HiveParserASTNode node)
            throws SemanticException {
        HiveParserCalcitePlanner analyzer =
                createCalcitePlanner(context, new HiveParserQueryState(hiveConf), hiveShim);
        RelNode relNode = analyzer.genLogicalPlan(node);
        if (relNode == null) {
            return new NopOperation();
        }

        // if not a query, treat it as an insert
        if (!analyzer.getQB().getIsQuery()) {
            return dmlHelper.createInsertOperation(analyzer, relNode);
        } else {
            return new PlannerQueryOperation(relNode);
        }
    }
}
