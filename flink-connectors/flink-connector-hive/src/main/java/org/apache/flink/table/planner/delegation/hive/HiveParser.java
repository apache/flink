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

import org.apache.flink.connectors.hive.FlinkHiveException;
import org.apache.flink.table.api.SqlParserException;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.catalog.hive.client.HiveShim;
import org.apache.flink.table.catalog.hive.client.HiveShimLoader;
import org.apache.flink.table.catalog.hive.util.HiveReflectionUtils;
import org.apache.flink.table.module.hive.udf.generic.HiveGenericUDFGrouping;
import org.apache.flink.table.operations.ExplainOperation;
import org.apache.flink.table.operations.NopOperation;
import org.apache.flink.table.operations.Operation;
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
import org.apache.flink.table.planner.operations.PlannerQueryOperation;
import org.apache.flink.table.planner.parse.CalciteParser;
import org.apache.flink.table.planner.plan.FlinkCalciteCatalogReader;
import org.apache.flink.util.FileUtils;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.lockmgr.LockException;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

/** A Parser that uses Hive's planner to parse a statement. */
public class HiveParser extends ParserImpl {

    private static final Logger LOG = LoggerFactory.getLogger(HiveParser.class);

    private static final Method setCurrentTSMethod =
            HiveReflectionUtils.tryGetMethod(
                    SessionState.class, "setupQueryCurrentTimestamp", new Class[0]);
    private static final Method getCurrentTSMethod =
            HiveReflectionUtils.tryGetMethod(
                    SessionState.class, "getQueryCurrentTimestamp", new Class[0]);

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

    HiveParser(
            CatalogManager catalogManager,
            Supplier<FlinkPlannerImpl> validatorSupplier,
            Supplier<CalciteParser> calciteParserSupplier,
            PlannerContext plannerContext) {
        super(
                catalogManager,
                validatorSupplier,
                calciteParserSupplier,
                plannerContext.getSqlExprToRexConverterFactory());
        this.plannerContext = plannerContext;
        this.catalogReader =
                plannerContext.createCatalogReader(
                        false,
                        catalogManager.getCurrentCatalog(),
                        catalogManager.getCurrentDatabase());
        this.frameworkConfig = plannerContext.createFrameworkConfig();
        this.funcConverter =
                new SqlFunctionConverter(
                        plannerContext.getCluster(),
                        frameworkConfig.getOperatorTable(),
                        catalogReader.nameMatcher());
        this.dmlHelper = new HiveParserDMLHelper(plannerContext, funcConverter, catalogManager);
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
        HiveConf hiveConf = new HiveConf(((HiveCatalog) currentCatalog).getHiveConf());
        hiveConf.setVar(HiveConf.ConfVars.DYNAMICPARTITIONINGMODE, "nonstrict");
        hiveConf.set("hive.allow.udf.load.on.demand", "false");
        hiveConf.setVar(HiveConf.ConfVars.HIVE_EXECUTION_ENGINE, "mr");
        HiveShim hiveShim =
                HiveShimLoader.loadHiveShim(((HiveCatalog) currentCatalog).getHiveVersion());
        try {
            // creates SessionState
            startSessionState(hiveConf, catalogManager);
            // We override Hive's grouping function. Refer to the implementation for more details.
            hiveShim.registerTemporaryFunction("grouping", HiveGenericUDFGrouping.class);
            return processCmd(statement, hiveConf, hiveShim, (HiveCatalog) currentCatalog);
        } finally {
            clearSessionState();
        }
    }

    private List<Operation> processCmd(
            String cmd, HiveConf hiveConf, HiveShim hiveShim, HiveCatalog hiveCatalog) {
        try {
            final HiveParserContext context = new HiveParserContext(hiveConf);
            // parse statement to get AST
            final HiveParserASTNode node = HiveASTParseUtils.parse(cmd, context);
            Operation operation;
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
                                dmlHelper);
                operation = ddlAnalyzer.convertToOperation(node);
                return Collections.singletonList(operation);
            } else {
                final boolean explain = node.getType() == HiveASTParser.TOK_EXPLAIN;
                // first child is the underlying explicandum
                HiveParserASTNode input = explain ? (HiveParserASTNode) node.getChild(0) : node;
                operation = analyzeSql(context, hiveConf, hiveShim, input);
                // explain an nop is also considered nop
                if (explain && !(operation instanceof NopOperation)) {
                    operation = new ExplainOperation(operation);
                }
            }
            return Collections.singletonList(operation);
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

    private void startSessionState(HiveConf hiveConf, CatalogManager catalogManager) {
        final ClassLoader contextCL = Thread.currentThread().getContextClassLoader();
        try {
            HiveParserSessionState sessionState = new HiveParserSessionState(hiveConf, contextCL);
            sessionState.initTxnMgr(hiveConf);
            sessionState.setCurrentDatabase(catalogManager.getCurrentDatabase());
            // some Hive functions needs the timestamp
            setCurrentTimestamp(sessionState);
            SessionState.setCurrentSessionState(sessionState);
        } catch (LockException e) {
            throw new FlinkHiveException("Failed to init SessionState", e);
        } finally {
            // don't let SessionState mess up with our context classloader
            Thread.currentThread().setContextClassLoader(contextCL);
        }
    }

    private static void setCurrentTimestamp(HiveParserSessionState sessionState) {
        if (setCurrentTSMethod != null) {
            try {
                setCurrentTSMethod.invoke(sessionState);
                Object currentTs = getCurrentTSMethod.invoke(sessionState);
                if (currentTs instanceof Instant) {
                    sessionState.hiveParserCurrentTS = Timestamp.from((Instant) currentTs);
                } else {
                    sessionState.hiveParserCurrentTS = (Timestamp) currentTs;
                }
            } catch (IllegalAccessException | InvocationTargetException e) {
                throw new FlinkHiveException("Failed to set current timestamp for session", e);
            }
        } else {
            sessionState.hiveParserCurrentTS = new Timestamp(System.currentTimeMillis());
        }
    }

    private void clearSessionState() {
        SessionState sessionState = SessionState.get();
        if (sessionState != null) {
            try {
                sessionState.close();
            } catch (Exception e) {
                LOG.warn("Error closing SessionState", e);
            }
        }
    }

    /** Sub-class of SessionState to meet our needs. */
    public static class HiveParserSessionState extends SessionState {

        private static final Class registryClz;
        private static final Method getRegistry;
        private static final Method clearRegistry;
        private static final Method closeRegistryLoaders;

        private Timestamp hiveParserCurrentTS;

        static {
            registryClz =
                    HiveReflectionUtils.tryGetClass("org.apache.hadoop.hive.ql.exec.Registry");
            if (registryClz != null) {
                getRegistry =
                        HiveReflectionUtils.tryGetMethod(
                                SessionState.class, "getRegistry", new Class[0]);
                clearRegistry =
                        HiveReflectionUtils.tryGetMethod(registryClz, "clear", new Class[0]);
                closeRegistryLoaders =
                        HiveReflectionUtils.tryGetMethod(
                                registryClz, "closeCUDFLoaders", new Class[0]);
            } else {
                getRegistry = null;
                clearRegistry = null;
                closeRegistryLoaders = null;
            }
        }

        private final ClassLoader originContextLoader;
        private final ClassLoader hiveLoader;

        public HiveParserSessionState(HiveConf conf, ClassLoader contextLoader) {
            super(conf);
            this.originContextLoader = contextLoader;
            this.hiveLoader = getConf().getClassLoader();
            // added jars are handled by context class loader, so we always use it as the session
            // class loader
            getConf().setClassLoader(contextLoader);
        }

        @Override
        public void close() throws IOException {
            clearSessionRegistry();
            if (getTxnMgr() != null) {
                getTxnMgr().closeTxnManager();
            }
            // close the classloader created in hive
            JavaUtils.closeClassLoadersTo(hiveLoader, originContextLoader);
            File resourceDir =
                    new File(getConf().getVar(HiveConf.ConfVars.DOWNLOADED_RESOURCES_DIR));
            LOG.debug("Removing resource dir " + resourceDir);
            FileUtils.deleteDirectoryQuietly(resourceDir);
            Hive.closeCurrent();
            detachSession();
        }

        public Timestamp getHiveParserCurrentTS() {
            return hiveParserCurrentTS;
        }

        private void clearSessionRegistry() {
            if (getRegistry != null) {
                try {
                    Object registry = getRegistry.invoke(this);
                    if (registry != null) {
                        clearRegistry.invoke(registry);
                        closeRegistryLoaders.invoke(registry);
                    }
                } catch (IllegalAccessException | InvocationTargetException e) {
                    LOG.warn("Failed to clear session registry", e);
                }
            }
        }
    }
}
