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
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.calcite.bridge.CalciteContext;
import org.apache.flink.table.calcite.bridge.PlannerExternalQueryOperation;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogRegistry;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UnresolvedIdentifier;
import org.apache.flink.table.catalog.hive.client.HiveShim;
import org.apache.flink.table.catalog.hive.client.HiveShimLoader;
import org.apache.flink.table.delegation.Parser;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.module.hive.udf.generic.HiveGenericUDFGrouping;
import org.apache.flink.table.operations.ExplainOperation;
import org.apache.flink.table.operations.HiveSetOperation;
import org.apache.flink.table.operations.ModifyOperation;
import org.apache.flink.table.operations.NopOperation;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.StatementSetOperation;
import org.apache.flink.table.operations.command.AddJarOperation;
import org.apache.flink.table.operations.command.SetOperation;
import org.apache.flink.table.planner.delegation.hive.copy.HiveASTParseException;
import org.apache.flink.table.planner.delegation.hive.copy.HiveASTParseUtils;
import org.apache.flink.table.planner.delegation.hive.copy.HiveParserASTNode;
import org.apache.flink.table.planner.delegation.hive.copy.HiveParserContext;
import org.apache.flink.table.planner.delegation.hive.copy.HiveParserQueryState;
import org.apache.flink.table.planner.delegation.hive.operations.HiveExecutableOperation;
import org.apache.flink.table.planner.delegation.hive.parse.FlinkExtendedParser;
import org.apache.flink.table.planner.delegation.hive.parse.HiveASTParser;
import org.apache.flink.table.planner.delegation.hive.parse.HiveParserCreateViewInfo;
import org.apache.flink.table.planner.delegation.hive.parse.HiveParserDDLSemanticAnalyzer;
import org.apache.flink.table.planner.delegation.hive.parse.HiveParserLoadSemanticAnalyzer;
import org.apache.flink.table.planner.utils.HiveCatalogUtils;
import org.apache.flink.table.planner.utils.TableSchemaUtils;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.advise.SqlAdvisor;
import org.apache.calcite.sql.advise.SqlAdvisorValidator;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.VariableSubstitution;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.processors.HiveCommand;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.apache.flink.table.planner.delegation.hive.copy.HiveSetProcessor.startWithHiveSpecialVariablePrefix;

/** A Parser that uses Hive's planner to parse a statement. */
public class HiveParser implements Parser {

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

    private final CalciteContext calciteContext;
    private final CatalogRegistry catalogRegistry;
    private final CalciteCatalogReader catalogReader;
    private final FrameworkConfig frameworkConfig;
    private final HiveParserDMLHelper dmlHelper;
    private final Map<String, String> hiveVariables;

    HiveParser(CalciteContext calciteContext) {
        this.catalogRegistry = calciteContext.getCatalogRegistry();
        this.calciteContext = calciteContext;
        this.catalogReader = calciteContext.createCatalogReader(false);
        this.frameworkConfig = calciteContext.createFrameworkConfig();
        SqlFunctionConverter funcConverter =
                new SqlFunctionConverter(
                        calciteContext.getCluster(),
                        frameworkConfig.getOperatorTable(),
                        catalogReader.nameMatcher());
        this.dmlHelper = new HiveParserDMLHelper(calciteContext, funcConverter, catalogRegistry);
        TableConfig tableConfig = calciteContext.getTableConfig();
        this.hiveVariables = tableConfig.get(HiveInternalOptions.HIVE_VARIABLES);
    }

    @Override
    public List<Operation> parse(String statement) {
        // first try to use flink extended parser to parse some special commands
        Optional<Operation> flinkExtendedOperation =
                FlinkExtendedParser.parseFlinkExtendedCommand(trimSemicolon(statement));
        if (flinkExtendedOperation.isPresent()) {
            return Collections.singletonList(flinkExtendedOperation.get());
        }

        Catalog currentCatalog =
                catalogRegistry.getCatalogOrError(catalogRegistry.getCurrentCatalog());
        if (!HiveCatalogUtils.isHiveCatalog(currentCatalog)) {
            // current, if it's not a hive catalog, we can't use hive dialect;
            // but we try to parse it to set command, if it's a set command, we can return
            // the SetOperation directly which enables users to switch dialect when hive dialect
            // is not available in the case of current catalog is not a HiveCatalog
            Optional<Operation> optionalOperation = FlinkExtendedParser.parseSet(statement);
            if (optionalOperation.isPresent()) {
                return Collections.singletonList(optionalOperation.get());
            } else {
                throw new TableException(
                        String.format(
                                "Current catalog is %s, which not a HiveCatalog, "
                                        + "but Hive dialect is only supported when the current catalog is HiveCatalog.",
                                catalogRegistry.getCurrentCatalog()));
            }
        }

        // Note: it's equal to HiveCatalog#getHiveConf, but we can't use HiveCatalog#getHiveConf
        // directly for classloader issue.
        // For more detail, please see class HiveCatalogUtils.
        HiveConf hiveConf = HiveCatalogUtils.getHiveConf(currentCatalog);
        Optional<Operation> nonSqlOperation = tryProcessHiveNonSqlStatement(hiveConf, statement);
        if (nonSqlOperation.isPresent()) {
            return Collections.singletonList(nonSqlOperation.get());
        }
        HiveConf hiveConfCopy = new HiveConf(hiveConf);
        hiveConfCopy.setVar(HiveConf.ConfVars.DYNAMICPARTITIONINGMODE, "nonstrict");
        hiveConfCopy.set("hive.allow.udf.load.on.demand", "false");
        hiveConfCopy.setVar(HiveConf.ConfVars.HIVE_EXECUTION_ENGINE, "mr");
        // Note: it's equal to HiveCatalog#getHiveVersion, but we can't use
        // HiveCatalog#getHiveVersion directly for classloader issue.
        // For more detail, please see class HiveCatalogUtils.
        HiveShim hiveShim =
                HiveShimLoader.loadHiveShim(HiveCatalogUtils.getHiveVersion(currentCatalog));
        try {
            // substitute variables for the statement
            statement = substituteVariables(hiveConfCopy, statement);
            // creates SessionState
            HiveSessionState.startSessionState(hiveConfCopy, catalogRegistry);
            // We override Hive's grouping function. Refer to the implementation for more details.
            hiveShim.registerTemporaryFunction("grouping", HiveGenericUDFGrouping.class);
            return processCmd(statement, hiveConfCopy, hiveShim, currentCatalog);
        } finally {
            HiveSessionState.clearSessionState();
        }
    }

    @Override
    public UnresolvedIdentifier parseIdentifier(String identifier) {
        return UnresolvedIdentifier.of(identifier.split("\\."));
    }

    @Override
    public ResolvedExpression parseSqlExpression(
            String sqlExpression, RowType inputRowType, @Nullable LogicalType outputType) {
        // shouldn't arrive in here with Hive parser
        throw new TableException("The method parseSqlExpression shouldn't be called.");
    }

    @Override
    public String[] getCompletionHints(String statement, int cursor) {
        // Copied from ParserImpl
        SqlAdvisorValidator validator =
                new SqlAdvisorValidator(
                        frameworkConfig.getOperatorTable(),
                        catalogReader,
                        calciteContext.getTypeFactory(),
                        SqlValidator.Config.DEFAULT.withConformance(SqlConformanceEnum.DEFAULT));
        SqlAdvisor advisor = new SqlAdvisor(validator, frameworkConfig.getParserConfig());
        String[] replaced = new String[1];

        return advisor.getCompletionHints(statement, cursor, replaced).stream()
                .map(item -> item.toIdentifier().toString())
                .toArray(String[]::new);
    }

    private String trimSemicolon(String statement) {
        statement = statement.trim();
        if (statement.endsWith(";")) {
            // the command may end with ";" since it won't be removed by Flink SQL CLI,
            // so, we need to remove ";"
            statement = statement.substring(0, statement.length() - 1);
        }
        return statement;
    }

    private Optional<Operation> tryProcessHiveNonSqlStatement(HiveConf hiveConf, String statement) {
        statement = trimSemicolon(statement);
        String[] commandTokens = statement.split("\\s+");
        HiveCommand hiveCommand = HiveCommand.find(commandTokens);
        if (hiveCommand != null) {
            String cmdArgs = statement.substring(commandTokens[0].length()).trim();
            if (hiveCommand == HiveCommand.SET) {
                return Optional.of(processSetCmd(cmdArgs));
            } else if (hiveCommand == HiveCommand.ADD) {
                return Optional.of(processAddCmd(substituteVariables(hiveConf, cmdArgs)));
            } else {
                throw new UnsupportedOperationException(
                        String.format("The Hive command %s is not supported.", hiveCommand));
            }
        }
        return Optional.empty();
    }

    private Operation processSetCmd(String setCmdArgs) {
        if (setCmdArgs.equals("")) {
            // the command is "set", if we follow Hive's behavior, it will output all configurations
            // including hiveconf, hivevar, env, ... which are too many.
            // So in here, for this case, just delegate to Flink's own behavior
            // which will only output the flink configuration.
            return new SetOperation();
        }
        if (setCmdArgs.equals("-v")) {
            // the command is "set -v", for such case, we will follow Hive's behavior.
            return new HiveExecutableOperation(new HiveSetOperation(true));
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
                    return new SetOperation(part[0], part[1]);
                }
            }
            if (part[0].equals("silent")) {
                throw new UnsupportedOperationException("Unsupported command 'set silent'.");
            }
            return new HiveExecutableOperation(new HiveSetOperation(part[0], part[1]));
        }
        return new HiveExecutableOperation(new HiveSetOperation(setCmdArgs));
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
            String cmd, HiveConf hiveConf, HiveShim hiveShim, Catalog hiveCatalog) {
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
                                catalogRegistry,
                                this,
                                hiveShim,
                                context,
                                dmlHelper,
                                frameworkConfig,
                                calciteContext.getCluster(),
                                calciteContext);
                return Collections.singletonList(ddlAnalyzer.convertToOperation(node));
            } else {
                return processQuery(context, hiveConf, hiveShim, node);
            }
        } catch (HiveASTParseException e) {
            // ParseException can happen for flink-specific statements, e.g. catalog DDLs
            String additionErrorMsg =
                    "If the SQL statement belongs to Flink's dialect,"
                            + " please use command `SET table.sql-dialect = default` "
                            + "to switch to Flink's default dialect and then execute the SQL "
                            + "statement again.\n";
            throw new TableException("SQL parse failed.\n" + additionErrorMsg, e);
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
            if (operation instanceof NopOperation) {
                return Collections.singletonList(operation);
            } else {
                if (operation instanceof HiveExecutableOperation) {
                    Operation innerOperation =
                            ((HiveExecutableOperation) operation).getInnerOperation();
                    return Collections.singletonList(
                            new HiveExecutableOperation(new ExplainOperation(innerOperation)));
                } else {
                    return Collections.singletonList(new ExplainOperation(operation));
                }
            }
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
                            hiveConf,
                            frameworkConfig,
                            calciteContext.getCluster(),
                            catalogRegistry);
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
            HiveParserContext context, HiveParserQueryState queryState) throws SemanticException {
        HiveParserCalcitePlanner calciteAnalyzer =
                new HiveParserCalcitePlanner(
                        queryState,
                        calciteContext,
                        catalogReader,
                        frameworkConfig,
                        catalogRegistry);
        calciteAnalyzer.initCtx(context);
        calciteAnalyzer.init(false);
        return calciteAnalyzer;
    }

    public void analyzeCreateView(
            HiveParserCreateViewInfo createViewInfo,
            HiveParserContext context,
            HiveParserQueryState queryState)
            throws SemanticException {
        HiveParserCalcitePlanner calciteAnalyzer = createCalcitePlanner(context, queryState);
        calciteAnalyzer.setCreatViewInfo(createViewInfo);
        calciteAnalyzer.genLogicalPlan(createViewInfo.getQuery());
    }

    private Operation analyzeSql(
            HiveParserContext context, HiveConf hiveConf, HiveShim hiveShim, HiveParserASTNode node)
            throws SemanticException {
        HiveParserCalcitePlanner analyzer =
                createCalcitePlanner(context, new HiveParserQueryState(hiveConf));
        RelNode relNode = analyzer.genLogicalPlan(node);
        if (relNode == null) {
            return new NopOperation();
        }

        // if not a query, treat it as an insert
        if (!analyzer.getQB().getIsQuery()) {
            return dmlHelper.createInsertOperation(analyzer, relNode);
        } else {
            ResolvedSchema resolvedSchema = TableSchemaUtils.resolvedSchema(relNode);
            return new PlannerExternalQueryOperation(relNode, resolvedSchema);
        }
    }
}
