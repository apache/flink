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
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.catalog.hive.client.HiveShim;
import org.apache.flink.table.catalog.hive.client.HiveShimLoader;
import org.apache.flink.table.catalog.hive.util.HiveReflectionUtils;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.planner.calcite.CalciteParser;
import org.apache.flink.table.planner.calcite.FlinkPlannerImpl;
import org.apache.flink.table.planner.calcite.SqlExprToRexConverter;
import org.apache.flink.table.planner.delegation.ParserImpl;
import org.apache.flink.table.planner.delegation.PlannerContext;
import org.apache.flink.table.planner.delegation.hive.desc.CreateTableASDesc;
import org.apache.flink.table.planner.delegation.hive.desc.HiveParserCreateViewDesc;
import org.apache.flink.table.planner.delegation.hive.parse.HiveASTParseException;
import org.apache.flink.table.planner.delegation.hive.parse.HiveASTParseUtils;
import org.apache.flink.table.planner.delegation.hive.parse.HiveASTParser;
import org.apache.flink.table.planner.delegation.hive.parse.HiveParserDDLSemanticAnalyzer;
import org.apache.flink.table.planner.plan.FlinkCalciteCatalogReader;
import org.apache.flink.util.FileUtils;

import org.apache.calcite.tools.FrameworkConfig;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.lockmgr.LockException;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;

/** A Parser that uses Hive's planner to parse a statement. */
public class HiveParser extends ParserImpl {

    private static final Logger LOG = LoggerFactory.getLogger(HiveParser.class);

    private static final Method setCurrentTSMethod =
            HiveReflectionUtils.tryGetMethod(
                    SessionState.class, "setupQueryCurrentTimestamp", new Class[0]);

    // need to maintain the ASTNode types for DDLs
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
                                HiveASTParser.TOK_ALTERDATABASE_LOCATION));
    }

    private final PlannerContext plannerContext;
    private final FlinkCalciteCatalogReader catalogReader;
    private final FrameworkConfig frameworkConfig;

    HiveParser(
            CatalogManager catalogManager,
            Supplier<FlinkPlannerImpl> validatorSupplier,
            Supplier<CalciteParser> calciteParserSupplier,
            Function<TableSchema, SqlExprToRexConverter> sqlExprToRexConverterCreator,
            PlannerContext plannerContext) {
        super(
                catalogManager,
                validatorSupplier,
                calciteParserSupplier,
                sqlExprToRexConverterCreator);
        this.plannerContext = plannerContext;
        this.catalogReader =
                plannerContext.createCatalogReader(
                        false,
                        catalogManager.getCurrentCatalog(),
                        catalogManager.getCurrentDatabase());
        this.frameworkConfig = plannerContext.createFrameworkConfig();
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
            return processCmd(statement, hiveConf, hiveShim, (HiveCatalog) currentCatalog);
        } finally {
            clearSessionState(hiveConf);
        }
    }

    private List<Operation> processCmd(
            String cmd, HiveConf hiveConf, HiveShim hiveShim, HiveCatalog hiveCatalog) {
        try {
            final HiveParserContext context = new HiveParserContext(hiveConf);
            // parse statement to get AST
            final ASTNode node = HiveASTParseUtils.parse(cmd, context);
            if (DDL_NODES.contains(node.getType())) {
                HiveParserQueryState queryState = new HiveParserQueryState(hiveConf);
                HiveParserDDLSemanticAnalyzer ddlAnalyzer =
                        new HiveParserDDLSemanticAnalyzer(
                                queryState,
                                context,
                                hiveCatalog,
                                getCatalogManager().getCurrentDatabase());
                Serializable work = ddlAnalyzer.analyzeInternal(node);
                DDLOperationConverter ddlConverter =
                        new DDLOperationConverter(this, getCatalogManager(), hiveShim);
                if (work instanceof HiveParserCreateViewDesc) {
                    return super.parse(cmd);
                } else if (work instanceof CreateTableASDesc) {
                    throw new SemanticException("CREATE TABLE AS not supported yet");
                }
                return Collections.singletonList(ddlConverter.convert(work));
            } else {
                return super.parse(cmd);
            }
        } catch (HiveASTParseException e) {
            // ParseException can happen for flink-specific statements, e.g. catalog DDLs
            try {
                return super.parse(cmd);
            } catch (SqlParserException parserException) {
                throw new SqlParserException("SQL parse failed", e);
            }
        } catch (SemanticException e) {
            throw new FlinkHiveException("HiveParser failed to parse " + cmd, e);
        }
    }

    private void startSessionState(HiveConf hiveConf, CatalogManager catalogManager) {
        final ClassLoader contextCL = Thread.currentThread().getContextClassLoader();
        try {
            SessionState sessionState = new HiveParserSessionState(hiveConf, contextCL);
            sessionState.initTxnMgr(hiveConf);
            sessionState.setCurrentDatabase(catalogManager.getCurrentDatabase());
            // some Hive functions needs the timestamp
            setCurrentTimestamp(sessionState);
            SessionState.start(sessionState);
        } catch (LockException e) {
            throw new FlinkHiveException("Failed to init SessionState", e);
        } finally {
            // don't let SessionState mess up with our context classloader
            Thread.currentThread().setContextClassLoader(contextCL);
        }
    }

    private static void setCurrentTimestamp(SessionState sessionState) {
        if (setCurrentTSMethod != null) {
            try {
                setCurrentTSMethod.invoke(sessionState);
            } catch (IllegalAccessException | InvocationTargetException e) {
                throw new FlinkHiveException("Failed to set current timestamp for session", e);
            }
        }
    }

    private void clearSessionState(HiveConf hiveConf) {
        SessionState sessionState = SessionState.get();
        if (sessionState != null) {
            try {
                sessionState.close();
                List<Path> toDelete = new ArrayList<>();
                toDelete.add(SessionState.getHDFSSessionPath(hiveConf));
                toDelete.add(SessionState.getLocalSessionPath(hiveConf));
                for (Path path : toDelete) {
                    FileSystem fs = path.getFileSystem(hiveConf);
                    fs.delete(path, true);
                }
            } catch (IOException e) {
                LOG.warn("Error closing SessionState", e);
            }
        }
    }

    /** Sub-class of SessionState to meet our needs. */
    private static class HiveParserSessionState extends SessionState {

        private static final Class registryClz;
        private static final Method getRegistry;
        private static final Method clearRegistry;
        private static final Method closeRegistryLoaders;

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
            detachSession();
            Hive.closeCurrent();
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
