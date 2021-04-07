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

package org.apache.flink.table.planner.delegation.hive.parse;

import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.PartitionNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.planner.delegation.hive.HiveParserConstants;
import org.apache.flink.table.planner.delegation.hive.copy.HiveASTParseUtils;
import org.apache.flink.table.planner.delegation.hive.copy.HiveParserASTNode;
import org.apache.flink.table.planner.delegation.hive.copy.HiveParserAuthorizationParseUtils;
import org.apache.flink.table.planner.delegation.hive.copy.HiveParserBaseSemanticAnalyzer;
import org.apache.flink.table.planner.delegation.hive.copy.HiveParserQueryState;
import org.apache.flink.table.planner.delegation.hive.copy.HiveParserStorageFormat;
import org.apache.flink.table.planner.delegation.hive.desc.CreateTableASDesc;
import org.apache.flink.table.planner.delegation.hive.desc.DropPartitionDesc;
import org.apache.flink.table.planner.delegation.hive.desc.HiveParserAlterDatabaseDesc;
import org.apache.flink.table.planner.delegation.hive.desc.HiveParserAlterTableDesc;
import org.apache.flink.table.planner.delegation.hive.desc.HiveParserCreateTableDesc;
import org.apache.flink.table.planner.delegation.hive.desc.HiveParserCreateTableDesc.NotNullConstraint;
import org.apache.flink.table.planner.delegation.hive.desc.HiveParserCreateTableDesc.PrimaryKey;
import org.apache.flink.table.planner.delegation.hive.desc.HiveParserCreateViewDesc;
import org.apache.flink.table.planner.delegation.hive.desc.HiveParserDropDatabaseDesc;
import org.apache.flink.table.planner.delegation.hive.desc.HiveParserDropFunctionDesc;
import org.apache.flink.table.planner.delegation.hive.desc.HiveParserDropTableDesc;
import org.apache.flink.table.planner.delegation.hive.desc.HiveParserShowTablesDesc;

import org.antlr.runtime.tree.CommonTree;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.SkewedInfo;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.exec.FunctionUtils;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.EximUtil;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.AddPartitionDesc;
import org.apache.hadoop.hive.ql.plan.AlterTableDesc;
import org.apache.hadoop.hive.ql.plan.CreateDatabaseDesc;
import org.apache.hadoop.hive.ql.plan.CreateFunctionDesc;
import org.apache.hadoop.hive.ql.plan.DDLWork;
import org.apache.hadoop.hive.ql.plan.DescDatabaseDesc;
import org.apache.hadoop.hive.ql.plan.DescFunctionDesc;
import org.apache.hadoop.hive.ql.plan.DescTableDesc;
import org.apache.hadoop.hive.ql.plan.DropFunctionDesc;
import org.apache.hadoop.hive.ql.plan.FunctionWork;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.apache.hadoop.hive.ql.plan.PrincipalDesc;
import org.apache.hadoop.hive.ql.plan.ShowDatabasesDesc;
import org.apache.hadoop.hive.ql.plan.ShowFunctionsDesc;
import org.apache.hadoop.hive.ql.plan.ShowPartitionsDesc;
import org.apache.hadoop.hive.ql.plan.SwitchDatabaseDesc;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.typeinfo.CharTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.VarcharTypeInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Ported hive's org.apache.hadoop.hive.ql.parse.DDLSemanticAnalyzer, and also incorporated
 * functionalities from SemanticAnalyzer and FunctionSemanticAnalyzer.
 */
public class HiveParserDDLSemanticAnalyzer {
    private static final Logger LOG = LoggerFactory.getLogger(HiveParserDDLSemanticAnalyzer.class);
    private static final Map<Integer, String> TokenToTypeName = new HashMap<>();

    private final Set<String> reservedPartitionValues;
    private final HiveConf conf;
    private final HiveParserQueryState queryState;
    private final HiveCatalog hiveCatalog;
    private final String currentDB;

    static {
        TokenToTypeName.put(HiveASTParser.TOK_BOOLEAN, serdeConstants.BOOLEAN_TYPE_NAME);
        TokenToTypeName.put(HiveASTParser.TOK_TINYINT, serdeConstants.TINYINT_TYPE_NAME);
        TokenToTypeName.put(HiveASTParser.TOK_SMALLINT, serdeConstants.SMALLINT_TYPE_NAME);
        TokenToTypeName.put(HiveASTParser.TOK_INT, serdeConstants.INT_TYPE_NAME);
        TokenToTypeName.put(HiveASTParser.TOK_BIGINT, serdeConstants.BIGINT_TYPE_NAME);
        TokenToTypeName.put(HiveASTParser.TOK_FLOAT, serdeConstants.FLOAT_TYPE_NAME);
        TokenToTypeName.put(HiveASTParser.TOK_DOUBLE, serdeConstants.DOUBLE_TYPE_NAME);
        TokenToTypeName.put(HiveASTParser.TOK_STRING, serdeConstants.STRING_TYPE_NAME);
        TokenToTypeName.put(HiveASTParser.TOK_CHAR, serdeConstants.CHAR_TYPE_NAME);
        TokenToTypeName.put(HiveASTParser.TOK_VARCHAR, serdeConstants.VARCHAR_TYPE_NAME);
        TokenToTypeName.put(HiveASTParser.TOK_BINARY, serdeConstants.BINARY_TYPE_NAME);
        TokenToTypeName.put(HiveASTParser.TOK_DATE, serdeConstants.DATE_TYPE_NAME);
        TokenToTypeName.put(HiveASTParser.TOK_DATETIME, serdeConstants.DATETIME_TYPE_NAME);
        TokenToTypeName.put(HiveASTParser.TOK_TIMESTAMP, serdeConstants.TIMESTAMP_TYPE_NAME);
        TokenToTypeName.put(
                HiveASTParser.TOK_INTERVAL_YEAR_MONTH,
                HiveParserConstants.INTERVAL_YEAR_MONTH_TYPE_NAME);
        TokenToTypeName.put(
                HiveASTParser.TOK_INTERVAL_DAY_TIME,
                HiveParserConstants.INTERVAL_DAY_TIME_TYPE_NAME);
        TokenToTypeName.put(HiveASTParser.TOK_DECIMAL, serdeConstants.DECIMAL_TYPE_NAME);
    }

    public static String getTypeName(HiveParserASTNode node) throws SemanticException {
        int token = node.getType();
        String typeName;

        // datetime type isn't currently supported
        if (token == HiveASTParser.TOK_DATETIME) {
            throw new ValidationException(ErrorMsg.UNSUPPORTED_TYPE.getMsg());
        }

        switch (token) {
            case HiveASTParser.TOK_CHAR:
                CharTypeInfo charTypeInfo = HiveASTParseUtils.getCharTypeInfo(node);
                typeName = charTypeInfo.getQualifiedName();
                break;
            case HiveASTParser.TOK_VARCHAR:
                VarcharTypeInfo varcharTypeInfo = HiveASTParseUtils.getVarcharTypeInfo(node);
                typeName = varcharTypeInfo.getQualifiedName();
                break;
            case HiveASTParser.TOK_DECIMAL:
                DecimalTypeInfo decTypeInfo = HiveASTParseUtils.getDecimalTypeTypeInfo(node);
                typeName = decTypeInfo.getQualifiedName();
                break;
            default:
                typeName = TokenToTypeName.get(token);
        }
        return typeName;
    }

    public HiveParserDDLSemanticAnalyzer(
            HiveParserQueryState queryState, HiveCatalog hiveCatalog, String currentDB)
            throws SemanticException {
        this.queryState = queryState;
        this.conf = queryState.getConf();
        this.hiveCatalog = hiveCatalog;
        this.currentDB = currentDB;
        reservedPartitionValues = new HashSet<>();
        // Partition can't have this name
        reservedPartitionValues.add(HiveConf.getVar(conf, HiveConf.ConfVars.DEFAULTPARTITIONNAME));
        reservedPartitionValues.add(
                HiveConf.getVar(conf, HiveConf.ConfVars.DEFAULT_ZOOKEEPER_PARTITION_NAME));
        // Partition value can't end in this suffix
        reservedPartitionValues.add(
                HiveConf.getVar(conf, HiveConf.ConfVars.METASTORE_INT_ORIGINAL));
        reservedPartitionValues.add(
                HiveConf.getVar(conf, HiveConf.ConfVars.METASTORE_INT_ARCHIVED));
        reservedPartitionValues.add(
                HiveConf.getVar(conf, HiveConf.ConfVars.METASTORE_INT_EXTRACTED));
    }

    private Table getTable(String tableName) throws SemanticException {
        return getTable(toObjectPath(tableName));
    }

    private Table getTable(ObjectPath tablePath) {
        try {
            return new Table(hiveCatalog.getHiveTable(tablePath));
        } catch (TableNotExistException e) {
            throw new ValidationException("Table not found", e);
        }
    }

    private ObjectPath toObjectPath(String name) throws SemanticException {
        String[] parts = Utilities.getDbTableName(currentDB, name);
        return new ObjectPath(parts[0], parts[1]);
    }

    private HashSet<ReadEntity> getInputs() {
        return new HashSet<>();
    }

    private HashSet<WriteEntity> getOutputs() {
        return new HashSet<>();
    }

    public Serializable analyzeInternal(HiveParserASTNode input) throws SemanticException {

        HiveParserASTNode ast = input;
        Serializable res = null;
        switch (ast.getType()) {
            case HiveASTParser.TOK_ALTERTABLE:
                {
                    ast = (HiveParserASTNode) input.getChild(1);
                    String[] qualified =
                            HiveParserBaseSemanticAnalyzer.getQualifiedTableName(
                                    (HiveParserASTNode) input.getChild(0));
                    String tableName = HiveParserBaseSemanticAnalyzer.getDotName(qualified);
                    HashMap<String, String> partSpec = null;
                    HiveParserASTNode partSpecNode = (HiveParserASTNode) input.getChild(2);
                    if (partSpecNode != null) {
                        partSpec = getPartSpec(partSpecNode);
                    }

                    if (ast.getType() == HiveASTParser.TOK_ALTERTABLE_RENAME) {
                        res = analyzeAlterTableRename(qualified, ast, false);
                    } else if (ast.getType() == HiveASTParser.TOK_ALTERTABLE_TOUCH) {
                        handleUnsupportedOperation(ast);
                    } else if (ast.getType() == HiveASTParser.TOK_ALTERTABLE_ARCHIVE) {
                        handleUnsupportedOperation(ast);
                    } else if (ast.getType() == HiveASTParser.TOK_ALTERTABLE_UNARCHIVE) {
                        handleUnsupportedOperation(ast);
                    } else if (ast.getType() == HiveASTParser.TOK_ALTERTABLE_ADDCOLS) {
                        res = analyzeAlterTableModifyCols(qualified, ast, partSpec, false);
                    } else if (ast.getType() == HiveASTParser.TOK_ALTERTABLE_REPLACECOLS) {
                        res = analyzeAlterTableModifyCols(qualified, ast, partSpec, true);
                    } else if (ast.getType() == HiveASTParser.TOK_ALTERTABLE_RENAMECOL) {
                        res = analyzeAlterTableRenameCol(qualified, ast, partSpec);
                    } else if (ast.getType() == HiveASTParser.TOK_ALTERTABLE_ADDPARTS) {
                        res = analyzeAlterTableAddParts(qualified, ast, false);
                    } else if (ast.getType() == HiveASTParser.TOK_ALTERTABLE_DROPPARTS) {
                        res = analyzeAlterTableDropParts(qualified, ast, false);
                    } else if (ast.getType() == HiveASTParser.TOK_ALTERTABLE_PARTCOLTYPE) {
                        handleUnsupportedOperation(ast);
                    } else if (ast.getType() == HiveASTParser.TOK_ALTERTABLE_PROPERTIES) {
                        res = analyzeAlterTableProps(qualified, null, ast, false, false);
                    } else if (ast.getType() == HiveASTParser.TOK_ALTERTABLE_DROPPROPERTIES) {
                        res = analyzeAlterTableProps(qualified, null, ast, false, true);
                    } else if (ast.getType() == HiveASTParser.TOK_ALTERTABLE_UPDATESTATS) {
                        res = analyzeAlterTableProps(qualified, partSpec, ast, false, false);
                    } else if (ast.getType() == HiveASTParser.TOK_ALTERTABLE_SKEWED) {
                        handleUnsupportedOperation(ast);
                    } else if (ast.getType() == HiveASTParser.TOK_ALTERTABLE_EXCHANGEPARTITION) {
                        handleUnsupportedOperation(ast);
                    } else if (ast.getToken().getType()
                            == HiveASTParser.TOK_ALTERTABLE_FILEFORMAT) {
                        res = analyzeAlterTableFileFormat(ast, tableName, partSpec);
                    } else if (ast.getToken().getType() == HiveASTParser.TOK_ALTERTABLE_LOCATION) {
                        res = analyzeAlterTableLocation(ast, tableName, partSpec);
                    } else if (ast.getToken().getType()
                            == HiveASTParser.TOK_ALTERTABLE_MERGEFILES) {
                        handleUnsupportedOperation(ast);
                    } else if (ast.getToken().getType()
                            == HiveASTParser.TOK_ALTERTABLE_SERIALIZER) {
                        res = analyzeAlterTableSerde(ast, tableName, partSpec);
                    } else if (ast.getToken().getType()
                            == HiveASTParser.TOK_ALTERTABLE_SERDEPROPERTIES) {
                        res = analyzeAlterTableSerdeProps(ast, tableName, partSpec);
                    } else if (ast.getToken().getType()
                            == HiveASTParser.TOK_ALTERTABLE_RENAMEPART) {
                        handleUnsupportedOperation(ast);
                    } else if (ast.getToken().getType()
                            == HiveASTParser.TOK_ALTERTABLE_SKEWED_LOCATION) {
                        handleUnsupportedOperation(ast);
                    } else if (ast.getToken().getType() == HiveASTParser.TOK_ALTERTABLE_BUCKETS) {
                        handleUnsupportedOperation(ast);
                    } else if (ast.getToken().getType()
                            == HiveASTParser.TOK_ALTERTABLE_CLUSTER_SORT) {
                        handleUnsupportedOperation(ast);
                    } else if (ast.getToken().getType() == HiveASTParser.TOK_ALTERTABLE_COMPACT) {
                        handleUnsupportedOperation(ast);
                    } else if (ast.getToken().getType()
                            == HiveASTParser.TOK_ALTERTABLE_UPDATECOLSTATS) {
                        handleUnsupportedOperation(ast);
                    } else if (ast.getToken().getType()
                            == HiveASTParser.TOK_ALTERTABLE_DROPCONSTRAINT) {
                        handleUnsupportedOperation(ast);
                    } else if (ast.getToken().getType()
                            == HiveASTParser.TOK_ALTERTABLE_ADDCONSTRAINT) {
                        handleUnsupportedOperation(ast);
                    } else {
                        throw new ValidationException("Unknown AST node for ALTER TABLE: " + ast);
                    }
                    break;
                }
            case HiveASTParser.TOK_DROPTABLE:
                res = analyzeDropTable(ast, null);
                break;
            case HiveASTParser.TOK_DESCTABLE:
                res = analyzeDescribeTable(ast);
                break;
            case HiveASTParser.TOK_SHOWDATABASES:
                res = analyzeShowDatabases(ast);
                break;
            case HiveASTParser.TOK_SHOWTABLES:
                res = analyzeShowTables(ast, false);
                break;
            case HiveASTParser.TOK_SHOWFUNCTIONS:
                res = analyzeShowFunctions(ast);
                break;
            case HiveASTParser.TOK_SHOWVIEWS:
                res = analyzeShowTables(ast, true);
                break;
            case HiveASTParser.TOK_DESCFUNCTION:
                res = analyzeDescFunction(ast);
                break;
            case HiveASTParser.TOK_DESCDATABASE:
                res = analyzeDescDatabase(ast);
                break;
            case HiveASTParser.TOK_DROPVIEW:
                res = analyzeDropTable(ast, TableType.VIRTUAL_VIEW);
                break;
            case HiveASTParser.TOK_ALTERVIEW:
                {
                    if (ast.getChild(1).getType() == HiveASTParser.TOK_QUERY) {
                        // alter view as
                        res = analyzeCreateView(ast);
                    } else {
                        String[] qualified =
                                HiveParserBaseSemanticAnalyzer.getQualifiedTableName(
                                        (HiveParserASTNode) ast.getChild(0));
                        ast = (HiveParserASTNode) ast.getChild(1);
                        if (ast.getType() == HiveASTParser.TOK_ALTERVIEW_PROPERTIES) {
                            res = analyzeAlterTableProps(qualified, null, ast, true, false);
                        } else if (ast.getType() == HiveASTParser.TOK_ALTERVIEW_DROPPROPERTIES) {
                            res = analyzeAlterTableProps(qualified, null, ast, true, true);
                        } else if (ast.getType() == HiveASTParser.TOK_ALTERVIEW_ADDPARTS) {
                            handleUnsupportedOperation("ADD PARTITION for view is not supported");
                        } else if (ast.getType() == HiveASTParser.TOK_ALTERVIEW_DROPPARTS) {
                            handleUnsupportedOperation("DROP PARTITION for view is not supported");
                        } else if (ast.getType() == HiveASTParser.TOK_ALTERVIEW_RENAME) {
                            res = analyzeAlterTableRename(qualified, ast, true);
                        } else {
                            throw new ValidationException(
                                    "Unknown AST node for ALTER VIEW: " + ast);
                        }
                    }
                    break;
                }
            case HiveASTParser.TOK_SHOWPARTITIONS:
                res = analyzeShowPartitions(ast);
                break;
            case HiveASTParser.TOK_CREATEDATABASE:
                res = analyzeCreateDatabase(ast);
                break;
            case HiveASTParser.TOK_DROPDATABASE:
                res = analyzeDropDatabase(ast);
                break;
            case HiveASTParser.TOK_SWITCHDATABASE:
                res = analyzeSwitchDatabase(ast);
                break;
            case HiveASTParser.TOK_ALTERDATABASE_PROPERTIES:
                res = analyzeAlterDatabaseProperties(ast);
                break;
            case HiveASTParser.TOK_ALTERDATABASE_OWNER:
                res = analyzeAlterDatabaseOwner(ast);
                break;
            case HiveASTParser.TOK_ALTERDATABASE_LOCATION:
                res = analyzeAlterDatabaseLocation(ast);
                break;
            case HiveASTParser.TOK_CREATETABLE:
                res = analyzeCreateTable(ast);
                break;
            case HiveASTParser.TOK_CREATEVIEW:
                res = analyzeCreateView(ast);
                break;
            case HiveASTParser.TOK_CREATEFUNCTION:
                res = analyzerCreateFunction(ast);
                break;
            case HiveASTParser.TOK_DROPFUNCTION:
                res = analyzeDropFunction(ast);
                break;
            case HiveASTParser.TOK_TRUNCATETABLE:
            case HiveASTParser.TOK_CREATEINDEX:
            case HiveASTParser.TOK_DROPINDEX:
            case HiveASTParser.TOK_SHOWLOCKS:
            case HiveASTParser.TOK_SHOWDBLOCKS:
            case HiveASTParser.TOK_SHOW_COMPACTIONS:
            case HiveASTParser.TOK_SHOW_TRANSACTIONS:
            case HiveASTParser.TOK_ABORT_TRANSACTIONS:
            case HiveASTParser.TOK_MSCK:
            case HiveASTParser.TOK_ALTERINDEX_REBUILD:
            case HiveASTParser.TOK_ALTERINDEX_PROPERTIES:
            case HiveASTParser.TOK_SHOWINDEXES:
            case HiveASTParser.TOK_LOCKTABLE:
            case HiveASTParser.TOK_UNLOCKTABLE:
            case HiveASTParser.TOK_LOCKDB:
            case HiveASTParser.TOK_UNLOCKDB:
            case HiveASTParser.TOK_CREATEROLE:
            case HiveASTParser.TOK_DROPROLE:
            case HiveASTParser.TOK_SHOW_ROLE_GRANT:
            case HiveASTParser.TOK_SHOW_ROLE_PRINCIPALS:
            case HiveASTParser.TOK_SHOW_ROLES:
            case HiveASTParser.TOK_GRANT_ROLE:
            case HiveASTParser.TOK_REVOKE_ROLE:
            case HiveASTParser.TOK_GRANT:
            case HiveASTParser.TOK_SHOW_GRANT:
            case HiveASTParser.TOK_REVOKE:
            case HiveASTParser.TOK_SHOW_SET_ROLE:
            case HiveASTParser.TOK_CACHE_METADATA:
            case HiveASTParser.TOK_DROP_MATERIALIZED_VIEW:
            case HiveASTParser.TOK_SHOW_CREATEDATABASE:
            case HiveASTParser.TOK_SHOWCOLUMNS:
            case HiveASTParser.TOK_SHOW_TABLESTATUS:
            case HiveASTParser.TOK_SHOW_TBLPROPERTIES:
            case HiveASTParser.TOK_SHOWCONF:
            case HiveASTParser.TOK_SHOW_CREATETABLE:
            default:
                handleUnsupportedOperation(ast);
        }
        return res;
    }

    private Serializable analyzeDropFunction(HiveParserASTNode ast) {
        // ^(TOK_DROPFUNCTION identifier ifExists? $temp?)
        String functionName = ast.getChild(0).getText();
        boolean ifExists = (ast.getFirstChildWithType(HiveASTParser.TOK_IFEXISTS) != null);

        boolean isTemporaryFunction =
                (ast.getFirstChildWithType(HiveASTParser.TOK_TEMPORARY) != null);
        DropFunctionDesc desc = new DropFunctionDesc();
        desc.setFunctionName(functionName);
        desc.setTemp(isTemporaryFunction);
        return new HiveParserDropFunctionDesc(desc, ifExists);
    }

    private Serializable analyzerCreateFunction(HiveParserASTNode ast) {
        // ^(TOK_CREATEFUNCTION identifier StringLiteral ({isTempFunction}? => TOK_TEMPORARY))
        String functionName = ast.getChild(0).getText().toLowerCase();
        boolean isTemporaryFunction =
                (ast.getFirstChildWithType(HiveASTParser.TOK_TEMPORARY) != null);
        String className =
                HiveParserBaseSemanticAnalyzer.unescapeSQLString(ast.getChild(1).getText());

        // Temp functions are not allowed to have qualified names.
        if (isTemporaryFunction && FunctionUtils.isQualifiedFunctionName(functionName)) {
            throw new ValidationException(
                    "Temporary function cannot be created with a qualified name.");
        }

        CreateFunctionDesc desc = new CreateFunctionDesc();
        desc.setFunctionName(functionName);
        desc.setTemp(isTemporaryFunction);
        desc.setClassName(className);
        desc.setResources(Collections.emptyList());
        return new FunctionWork(desc);
    }

    private Serializable analyzeCreateView(HiveParserASTNode ast) throws SemanticException {
        String[] qualTabName =
                HiveParserBaseSemanticAnalyzer.getQualifiedTableName(
                        (HiveParserASTNode) ast.getChild(0));
        String dbDotTable = HiveParserBaseSemanticAnalyzer.getDotName(qualTabName);
        List<FieldSchema> cols = null;
        boolean ifNotExists = false;
        boolean isAlterViewAs = false;
        String comment = null;
        HiveParserASTNode selectStmt = null;
        Map<String, String> tblProps = null;
        boolean isMaterialized =
                ast.getToken().getType() == HiveASTParser.TOK_CREATE_MATERIALIZED_VIEW;
        if (isMaterialized) {
            handleUnsupportedOperation("MATERIALIZED VIEW is not supported");
        }
        HiveParserBaseSemanticAnalyzer.HiveParserRowFormatParams rowFormatParams =
                new HiveParserBaseSemanticAnalyzer.HiveParserRowFormatParams();
        HiveParserStorageFormat storageFormat = new HiveParserStorageFormat(conf);

        LOG.info("Creating view " + dbDotTable + " position=" + ast.getCharPositionInLine());
        int numCh = ast.getChildCount();
        for (int num = 1; num < numCh; num++) {
            HiveParserASTNode child = (HiveParserASTNode) ast.getChild(num);
            if (storageFormat.fillStorageFormat(child)) {
                handleUnsupportedOperation("FILE FORMAT for view is not supported");
            }
            switch (child.getToken().getType()) {
                case HiveASTParser.TOK_IFNOTEXISTS:
                    ifNotExists = true;
                    break;
                case HiveASTParser.TOK_REWRITE_ENABLED:
                    handleUnsupportedOperation("MATERIALIZED VIEW REWRITE is not supported");
                    break;
                case HiveASTParser.TOK_ORREPLACE:
                    handleUnsupportedOperation("CREATE OR REPLACE VIEW is not supported");
                    break;
                case HiveASTParser.TOK_QUERY:
                    selectStmt = child;
                    break;
                case HiveASTParser.TOK_TABCOLNAME:
                    cols = HiveParserBaseSemanticAnalyzer.getColumns(child);
                    break;
                case HiveASTParser.TOK_TABLECOMMENT:
                    comment =
                            HiveParserBaseSemanticAnalyzer.unescapeSQLString(
                                    child.getChild(0).getText());
                    break;
                case HiveASTParser.TOK_TABLEPROPERTIES:
                    tblProps = getProps((HiveParserASTNode) child.getChild(0));
                    break;
                case HiveASTParser.TOK_TABLEROWFORMAT:
                    handleUnsupportedOperation("ROW FORMAT for view is not supported");
                    break;
                case HiveASTParser.TOK_TABLESERIALIZER:
                    handleUnsupportedOperation("SERDE for view is not supported");
                    break;
                case HiveASTParser.TOK_TABLELOCATION:
                    handleUnsupportedOperation("LOCATION for view is not supported");
                    break;
                case HiveASTParser.TOK_VIEWPARTCOLS:
                    handleUnsupportedOperation("PARTITION COLUMN for view is not supported");
                    break;
                default:
                    throw new ValidationException(
                            "Unknown AST node for CREATE/ALTER VIEW: " + child);
            }
        }

        if (ast.getToken().getType() == HiveASTParser.TOK_ALTERVIEW
                && ast.getChild(1).getType() == HiveASTParser.TOK_QUERY) {
            isAlterViewAs = true;
        }

        queryState.setCommandType(HiveOperation.CREATEVIEW);
        return new HiveParserCreateViewDesc(
                dbDotTable, cols, comment, tblProps, ifNotExists, isAlterViewAs, selectStmt);
    }

    private Serializable analyzeCreateTable(HiveParserASTNode ast) throws SemanticException {
        String[] qualifiedTabName =
                HiveParserBaseSemanticAnalyzer.getQualifiedTableName(
                        (HiveParserASTNode) ast.getChild(0));
        String dbDotTab = HiveParserBaseSemanticAnalyzer.getDotName(qualifiedTabName);

        String likeTableName;
        List<FieldSchema> cols = new ArrayList<>();
        List<FieldSchema> partCols = new ArrayList<>();
        List<PrimaryKey> primaryKeys = new ArrayList<>();
        List<NotNullConstraint> notNulls = new ArrayList<>();
        String comment = null;
        String location = null;
        Map<String, String> tblProps = null;
        boolean ifNotExists = false;
        boolean isExt = false;
        boolean isTemporary = false;
        HiveParserASTNode selectStmt = null;
        final int createTable = 0; // regular CREATE TABLE
        final int ctlt = 1; // CREATE TABLE LIKE ... (CTLT)
        final int ctas = 2; // CREATE TABLE AS SELECT ... (CTAS)
        int commandType = createTable;

        HiveParserBaseSemanticAnalyzer.HiveParserRowFormatParams rowFormatParams =
                new HiveParserBaseSemanticAnalyzer.HiveParserRowFormatParams();
        HiveParserStorageFormat storageFormat = new HiveParserStorageFormat(conf);

        LOG.info("Creating table " + dbDotTab + " position=" + ast.getCharPositionInLine());
        int numCh = ast.getChildCount();

        // Check the 1st-level children and do simple semantic checks: 1) CTLT and CTAS should not
        // coexists.
        // 2) CTLT or CTAS should not coexists with column list (target table schema).
        // 3) CTAS does not support partitioning (for now).
        for (int num = 1; num < numCh; num++) {
            HiveParserASTNode child = (HiveParserASTNode) ast.getChild(num);
            if (storageFormat.fillStorageFormat(child)) {
                continue;
            }
            switch (child.getToken().getType()) {
                case HiveASTParser.TOK_IFNOTEXISTS:
                    ifNotExists = true;
                    break;
                case HiveASTParser.KW_EXTERNAL:
                    isExt = true;
                    break;
                case HiveASTParser.KW_TEMPORARY:
                    isTemporary = true;
                    break;
                case HiveASTParser.TOK_LIKETABLE:
                    if (child.getChildCount() > 0) {
                        likeTableName =
                                HiveParserBaseSemanticAnalyzer.getUnescapedName(
                                        (HiveParserASTNode) child.getChild(0));
                        if (likeTableName != null) {
                            if (commandType == ctas) {
                                throw new ValidationException(
                                        ErrorMsg.CTAS_CTLT_COEXISTENCE.getMsg());
                            }
                            if (cols.size() != 0) {
                                throw new ValidationException(
                                        ErrorMsg.CTLT_COLLST_COEXISTENCE.getMsg());
                            }
                        }
                        commandType = ctlt;
                        handleUnsupportedOperation("CREATE TABLE LIKE is not supported");
                    }
                    break;

                case HiveASTParser.TOK_QUERY: // CTAS
                    if (commandType == ctlt) {
                        throw new ValidationException(ErrorMsg.CTAS_CTLT_COEXISTENCE.getMsg());
                    }
                    if (cols.size() != 0) {
                        throw new ValidationException(ErrorMsg.CTAS_COLLST_COEXISTENCE.getMsg());
                    }
                    if (partCols.size() != 0) {
                        throw new ValidationException(ErrorMsg.CTAS_PARCOL_COEXISTENCE.getMsg());
                    }
                    if (isExt) {
                        throw new ValidationException(ErrorMsg.CTAS_EXTTBL_COEXISTENCE.getMsg());
                    }
                    commandType = ctas;
                    selectStmt = child;
                    break;
                case HiveASTParser.TOK_TABCOLLIST:
                    cols =
                            HiveParserBaseSemanticAnalyzer.getColumns(
                                    child, true, primaryKeys, notNulls);
                    break;
                case HiveASTParser.TOK_TABLECOMMENT:
                    comment =
                            HiveParserBaseSemanticAnalyzer.unescapeSQLString(
                                    child.getChild(0).getText());
                    break;
                case HiveASTParser.TOK_TABLEPARTCOLS:
                    partCols =
                            HiveParserBaseSemanticAnalyzer.getColumns(
                                    (HiveParserASTNode) child.getChild(0), false);
                    break;
                case HiveASTParser.TOK_TABLEROWFORMAT:
                    rowFormatParams.analyzeRowFormat(child);
                    break;
                case HiveASTParser.TOK_TABLELOCATION:
                    location =
                            HiveParserBaseSemanticAnalyzer.unescapeSQLString(
                                    child.getChild(0).getText());
                    location = EximUtil.relativeToAbsolutePath(conf, location);
                    break;
                case HiveASTParser.TOK_TABLEPROPERTIES:
                    tblProps = getProps((HiveParserASTNode) child.getChild(0));
                    break;
                case HiveASTParser.TOK_TABLESERIALIZER:
                    child = (HiveParserASTNode) child.getChild(0);
                    storageFormat.setSerde(
                            HiveParserBaseSemanticAnalyzer.unescapeSQLString(
                                    child.getChild(0).getText()));
                    if (child.getChildCount() == 2) {
                        HiveParserBaseSemanticAnalyzer.readProps(
                                (HiveParserASTNode) (child.getChild(1).getChild(0)),
                                storageFormat.getSerdeProps());
                    }
                    break;
                case HiveASTParser.TOK_ALTERTABLE_BUCKETS:
                    handleUnsupportedOperation("Bucketed table is not supported");
                    break;
                case HiveASTParser.TOK_TABLESKEWED:
                    handleUnsupportedOperation("Skewed table is not supported");
                    break;
                default:
                    throw new ValidationException("Unknown AST node for CREATE TABLE: " + child);
            }
        }

        if (storageFormat.getStorageHandler() != null) {
            handleUnsupportedOperation("Storage handler table is not supported");
        }

        if (commandType == createTable || commandType == ctlt) {
            queryState.setCommandType(HiveOperation.CREATETABLE);
        } else {
            queryState.setCommandType(HiveOperation.CREATETABLE_AS_SELECT);
        }

        storageFormat.fillDefaultStorageFormat(isExt, false);

        if (isTemporary) {
            if (partCols.size() > 0) {
                handleUnsupportedOperation(
                        "Partition columns are not supported on temporary tables");
            }
            handleUnsupportedOperation("Temporary hive table is not supported");
        }

        // Handle different types of CREATE TABLE command
        switch (commandType) {
            case createTable: // REGULAR CREATE TABLE DDL
                tblProps = addDefaultProperties(tblProps);
                return new HiveParserCreateTableDesc(
                        dbDotTab,
                        isExt,
                        ifNotExists,
                        isTemporary,
                        cols,
                        partCols,
                        comment,
                        location,
                        tblProps,
                        rowFormatParams,
                        storageFormat,
                        primaryKeys,
                        notNulls);

            case ctlt: // create table like <tbl_name>
                tblProps = addDefaultProperties(tblProps);
                throw new SemanticException("CREATE TABLE LIKE is not supported yet");

            case ctas: // create table as select
                tblProps = addDefaultProperties(tblProps);

                HiveParserCreateTableDesc createTableDesc =
                        new HiveParserCreateTableDesc(
                                dbDotTab,
                                isExt,
                                ifNotExists,
                                isTemporary,
                                cols,
                                partCols,
                                comment,
                                location,
                                tblProps,
                                rowFormatParams,
                                storageFormat,
                                primaryKeys,
                                notNulls);
                return new CreateTableASDesc(createTableDesc, selectStmt);
            default:
                throw new ValidationException("Unrecognized command.");
        }
    }

    private Serializable analyzeAlterDatabaseProperties(HiveParserASTNode ast) {

        String dbName =
                HiveParserBaseSemanticAnalyzer.unescapeIdentifier(ast.getChild(0).getText());
        Map<String, String> dbProps = null;

        for (int i = 1; i < ast.getChildCount(); i++) {
            HiveParserASTNode childNode = (HiveParserASTNode) ast.getChild(i);
            switch (childNode.getToken().getType()) {
                case HiveASTParser.TOK_DATABASEPROPERTIES:
                    dbProps = getProps((HiveParserASTNode) childNode.getChild(0));
                    break;
                default:
                    throw new ValidationException(
                            "Unknown AST node for ALTER DATABASE PROPERTIES: " + childNode);
            }
        }
        return HiveParserAlterDatabaseDesc.alterProps(dbName, dbProps);
    }

    private Serializable analyzeAlterDatabaseOwner(HiveParserASTNode ast) {
        String dbName =
                HiveParserBaseSemanticAnalyzer.getUnescapedName(
                        (HiveParserASTNode) ast.getChild(0));
        PrincipalDesc principalDesc =
                HiveParserAuthorizationParseUtils.getPrincipalDesc(
                        (HiveParserASTNode) ast.getChild(1));

        // The syntax should not allow these fields to be null, but lets verify
        String nullCmdMsg = "can't be null in alter database set owner command";
        if (principalDesc.getName() == null) {
            throw new ValidationException("Owner name " + nullCmdMsg);
        }
        if (principalDesc.getType() == null) {
            throw new ValidationException("Owner type " + nullCmdMsg);
        }

        return HiveParserAlterDatabaseDesc.alterOwner(dbName, principalDesc);
    }

    private Serializable analyzeAlterDatabaseLocation(HiveParserASTNode ast) {
        String dbName =
                HiveParserBaseSemanticAnalyzer.getUnescapedName(
                        (HiveParserASTNode) ast.getChild(0));
        String newLocation =
                HiveParserBaseSemanticAnalyzer.unescapeSQLString(ast.getChild(1).getText());
        return HiveParserAlterDatabaseDesc.alterLocation(dbName, newLocation);
    }

    private Serializable analyzeCreateDatabase(HiveParserASTNode ast) {
        String dbName =
                HiveParserBaseSemanticAnalyzer.unescapeIdentifier(ast.getChild(0).getText());
        boolean ifNotExists = false;
        String dbComment = null;
        String dbLocation = null;
        Map<String, String> dbProps = null;

        for (int i = 1; i < ast.getChildCount(); i++) {
            HiveParserASTNode childNode = (HiveParserASTNode) ast.getChild(i);
            switch (childNode.getToken().getType()) {
                case HiveASTParser.TOK_IFNOTEXISTS:
                    ifNotExists = true;
                    break;
                case HiveASTParser.TOK_DATABASECOMMENT:
                    dbComment =
                            HiveParserBaseSemanticAnalyzer.unescapeSQLString(
                                    childNode.getChild(0).getText());
                    break;
                case HiveASTParser.TOK_DATABASEPROPERTIES:
                    dbProps = getProps((HiveParserASTNode) childNode.getChild(0));
                    break;
                case HiveASTParser.TOK_DATABASELOCATION:
                    dbLocation =
                            HiveParserBaseSemanticAnalyzer.unescapeSQLString(
                                    childNode.getChild(0).getText());
                    break;
                default:
                    throw new ValidationException(
                            "Unknown AST node for CREATE DATABASE: " + childNode);
            }
        }

        CreateDatabaseDesc createDatabaseDesc =
                new CreateDatabaseDesc(dbName, dbComment, dbLocation, ifNotExists);
        if (dbProps != null) {
            createDatabaseDesc.setDatabaseProperties(dbProps);
        }
        return new DDLWork(getInputs(), getOutputs(), createDatabaseDesc);
    }

    private Serializable analyzeDropDatabase(HiveParserASTNode ast) {
        String dbName =
                HiveParserBaseSemanticAnalyzer.unescapeIdentifier(ast.getChild(0).getText());
        boolean ifExists = false;
        boolean ifCascade = false;

        if (null != ast.getFirstChildWithType(HiveASTParser.TOK_IFEXISTS)) {
            ifExists = true;
        }

        if (null != ast.getFirstChildWithType(HiveASTParser.TOK_CASCADE)) {
            ifCascade = true;
        }

        return new HiveParserDropDatabaseDesc(dbName, ifExists, ifCascade);
    }

    private Serializable analyzeSwitchDatabase(HiveParserASTNode ast) {
        String dbName =
                HiveParserBaseSemanticAnalyzer.unescapeIdentifier(ast.getChild(0).getText());
        SwitchDatabaseDesc switchDatabaseDesc = new SwitchDatabaseDesc(dbName);
        return new DDLWork(new HashSet<>(), new HashSet<>(), switchDatabaseDesc);
    }

    private Serializable analyzeDropTable(HiveParserASTNode ast, TableType expectedType) {
        String tableName =
                HiveParserBaseSemanticAnalyzer.getUnescapedName(
                        (HiveParserASTNode) ast.getChild(0));
        boolean ifExists = (ast.getFirstChildWithType(HiveASTParser.TOK_IFEXISTS) != null);

        boolean ifPurge = (ast.getFirstChildWithType(HiveASTParser.KW_PURGE) != null);
        return new HiveParserDropTableDesc(
                tableName, expectedType == TableType.VIRTUAL_VIEW, ifExists, ifPurge);
    }

    private void validateAlterTableType(
            Table tbl, AlterTableDesc.AlterTableTypes op, boolean expectView) {
        if (tbl.isView()) {
            if (!expectView) {
                throw new ValidationException(ErrorMsg.ALTER_COMMAND_FOR_VIEWS.getMsg());
            }

            switch (op) {
                case ADDPARTITION:
                case DROPPARTITION:
                case RENAMEPARTITION:
                case ADDPROPS:
                case DROPPROPS:
                case RENAME:
                    // allow this form
                    break;
                default:
                    throw new ValidationException(
                            ErrorMsg.ALTER_VIEW_DISALLOWED_OP.getMsg(op.toString()));
            }
        } else {
            if (expectView) {
                throw new ValidationException(ErrorMsg.ALTER_COMMAND_FOR_TABLES.getMsg());
            }
        }
        if (tbl.isNonNative()) {
            throw new ValidationException(
                    ErrorMsg.ALTER_TABLE_NON_NATIVE.getMsg(tbl.getTableName()));
        }
    }

    private Serializable analyzeAlterTableProps(
            String[] qualified,
            HashMap<String, String> partSpec,
            HiveParserASTNode ast,
            boolean expectView,
            boolean isUnset)
            throws SemanticException {

        String tableName = HiveParserBaseSemanticAnalyzer.getDotName(qualified);
        HashMap<String, String> mapProp =
                getProps((HiveParserASTNode) (ast.getChild(0)).getChild(0));
        // we need to check if the properties are valid, especially for stats.
        // they might be changed via alter table .. update statistics or alter table .. set
        // tblproperties.
        // If the property is not row_count or raw_data_size, it could not be changed through update
        // statistics
        for (Map.Entry<String, String> entry : mapProp.entrySet()) {
            // we make sure that we do not change anything if there is anything wrong.
            if (entry.getKey().equals(StatsSetupConst.ROW_COUNT)
                    || entry.getKey().equals(StatsSetupConst.RAW_DATA_SIZE)) {
                try {
                    Long.parseLong(entry.getValue());
                } catch (Exception e) {
                    throw new ValidationException(
                            "AlterTable "
                                    + entry.getKey()
                                    + " failed with value "
                                    + entry.getValue());
                }
            } else {
                if (HiveOperation.ALTERTABLE_UPDATETABLESTATS
                                .getOperationName()
                                .equals(queryState.getCommandType())
                        || HiveOperation.ALTERTABLE_UPDATEPARTSTATS
                                .getOperationName()
                                .equals(queryState.getCommandType())) {
                    throw new ValidationException(
                            "AlterTable UpdateStats "
                                    + entry.getKey()
                                    + " failed because the only valid keys are "
                                    + StatsSetupConst.ROW_COUNT
                                    + " and "
                                    + StatsSetupConst.RAW_DATA_SIZE);
                }
            }
        }
        HiveParserAlterTableDesc alterTblDesc = null;
        if (isUnset) {
            handleUnsupportedOperation("Unset properties not supported");
        } else {
            alterTblDesc =
                    HiveParserAlterTableDesc.alterTableProps(
                            tableName, partSpec, mapProp, expectView);
        }

        return alterTblDesc;
    }

    private Serializable analyzeAlterTableSerdeProps(
            HiveParserASTNode ast, String tableName, HashMap<String, String> partSpec) {
        HashMap<String, String> mapProp =
                getProps((HiveParserASTNode) (ast.getChild(0)).getChild(0));
        return HiveParserAlterTableDesc.alterSerDe(tableName, partSpec, null, mapProp);
    }

    private Serializable analyzeAlterTableSerde(
            HiveParserASTNode ast, String tableName, HashMap<String, String> partSpec) {
        String serdeName =
                HiveParserBaseSemanticAnalyzer.unescapeSQLString(ast.getChild(0).getText());
        HashMap<String, String> mapProp = null;
        if (ast.getChildCount() > 1) {
            mapProp = getProps((HiveParserASTNode) (ast.getChild(1)).getChild(0));
        }
        return HiveParserAlterTableDesc.alterSerDe(tableName, partSpec, serdeName, mapProp);
    }

    private Serializable analyzeAlterTableFileFormat(
            HiveParserASTNode ast, String tableName, HashMap<String, String> partSpec)
            throws SemanticException {

        HiveParserStorageFormat format = new HiveParserStorageFormat(conf);
        HiveParserASTNode child = (HiveParserASTNode) ast.getChild(0);

        if (!format.fillStorageFormat(child)) {
            throw new ValidationException("Unknown AST node for ALTER TABLE FILEFORMAT: " + child);
        }

        HiveParserAlterTableDesc alterTblDesc =
                HiveParserAlterTableDesc.alterFileFormat(tableName, partSpec);
        alterTblDesc.setGenericFileFormatName(format.getGenericName());

        return alterTblDesc;
    }

    private Serializable analyzeAlterTableLocation(
            HiveParserASTNode ast, String tableName, HashMap<String, String> partSpec) {
        String newLocation =
                HiveParserBaseSemanticAnalyzer.unescapeSQLString(ast.getChild(0).getText());
        return HiveParserAlterTableDesc.alterLocation(tableName, partSpec, newLocation);
    }

    public static HashMap<String, String> getProps(HiveParserASTNode prop) {
        // Must be deterministic order map for consistent q-test output across Java versions
        HashMap<String, String> mapProp = new LinkedHashMap<>();
        HiveParserBaseSemanticAnalyzer.readProps(prop, mapProp);
        return mapProp;
    }

    /** Utility class to resolve QualifiedName. */
    private static class QualifiedNameUtil {

        // Get the fully qualified name in the ast. e.g. the ast of the form ^(DOT^(DOT a b) c) will
        // generate a name of the form a.b.c
        public static String getFullyQualifiedName(HiveParserASTNode ast) {
            if (ast.getChildCount() == 0) {
                return ast.getText();
            } else if (ast.getChildCount() == 2) {
                return getFullyQualifiedName((HiveParserASTNode) ast.getChild(0))
                        + "."
                        + getFullyQualifiedName((HiveParserASTNode) ast.getChild(1));
            } else if (ast.getChildCount() == 3) {
                return getFullyQualifiedName((HiveParserASTNode) ast.getChild(0))
                        + "."
                        + getFullyQualifiedName((HiveParserASTNode) ast.getChild(1))
                        + "."
                        + getFullyQualifiedName((HiveParserASTNode) ast.getChild(2));
            } else {
                return null;
            }
        }

        // get the column path
        // return column name if exists, column could be DOT separated.
        // example: lintString.$elem$.myint
        // return table name for column name if no column has been specified.
        public static String getColPath(
                HiveParserASTNode node,
                String dbName,
                String tableName,
                Map<String, String> partSpec) {

            // if this ast has only one child, then no column name specified.
            if (node.getChildCount() == 1) {
                return tableName;
            }

            HiveParserASTNode columnNode = null;
            // Second child node could be partitionspec or column
            if (node.getChildCount() > 1) {
                if (partSpec == null) {
                    columnNode = (HiveParserASTNode) node.getChild(1);
                } else {
                    columnNode = (HiveParserASTNode) node.getChild(2);
                }
            }

            if (columnNode != null) {
                if (dbName == null) {
                    return tableName + "." + QualifiedNameUtil.getFullyQualifiedName(columnNode);
                } else {
                    return tableName.substring(dbName.length() + 1, tableName.length())
                            + "."
                            + QualifiedNameUtil.getFullyQualifiedName(columnNode);
                }
            } else {
                return tableName;
            }
        }

        // get partition metadata
        public static Map<String, String> getPartitionSpec(
                HiveCatalog db, HiveParserASTNode ast, ObjectPath tablePath)
                throws SemanticException {
            HiveParserASTNode partNode = null;
            // if this ast has only one child, then no partition spec specified.
            if (ast.getChildCount() == 1) {
                return null;
            }

            // if ast has two children
            // the 2nd child could be partition spec or columnName
            // if the ast has 3 children, the second *has to* be partition spec
            if (ast.getChildCount() > 2
                    && (ast.getChild(1).getType() != HiveASTParser.TOK_PARTSPEC)) {
                throw new ValidationException(
                        ast.getChild(1).getType() + " is not a partition specification");
            }

            if (ast.getChild(1).getType() == HiveASTParser.TOK_PARTSPEC) {
                partNode = (HiveParserASTNode) ast.getChild(1);
            }

            if (partNode != null) {
                return getPartSpec(partNode);
            }

            return null;
        }
    }

    private void validateTable(String tableName, Map<String, String> partSpec)
            throws SemanticException {
        Table tab = getTable(tableName);
        if (partSpec != null) {
            getPartition(tab, partSpec);
        }
    }

    private void getPartition(Table table, Map<String, String> partSpec) {
        try {
            hiveCatalog.getPartition(
                    new ObjectPath(table.getDbName(), table.getTableName()),
                    new CatalogPartitionSpec(partSpec));
        } catch (PartitionNotExistException e) {
            throw new ValidationException("Partition not found", e);
        }
    }

    /**
     * A query like this will generate a tree as follows "describe formatted default.maptable
     * partition (b=100) id;" TOK_TABTYPE TOK_TABNAME --> root for tablename, 2 child nodes mean DB
     * specified default maptable TOK_PARTSPEC --> root node for partition spec. else columnName
     * TOK_PARTVAL b 100 id --> root node for columnName formatted
     */
    private Serializable analyzeDescribeTable(HiveParserASTNode ast) throws SemanticException {
        HiveParserASTNode tableTypeExpr = (HiveParserASTNode) ast.getChild(0);

        String dbName = null;
        String tableName;
        String colPath;
        Map<String, String> partSpec;

        HiveParserASTNode tableNode;

        // process the first node to extract tablename
        // tablename is either TABLENAME or DBNAME.TABLENAME if db is given
        if (tableTypeExpr.getChild(0).getType() == HiveASTParser.TOK_TABNAME) {
            tableNode = (HiveParserASTNode) tableTypeExpr.getChild(0);
            if (tableNode.getChildCount() == 1) {
                tableName = tableNode.getChild(0).getText();
            } else {
                dbName = tableNode.getChild(0).getText();
                tableName = dbName + "." + tableNode.getChild(1).getText();
            }
        } else {
            throw new ValidationException(
                    tableTypeExpr.getChild(0).getText() + " is not an expected token type");
        }

        // process the second child,if exists, node to get partition spec(s)
        partSpec =
                QualifiedNameUtil.getPartitionSpec(
                        hiveCatalog, tableTypeExpr, toObjectPath(tableName));

        // process the third child node,if exists, to get partition spec(s)
        colPath = QualifiedNameUtil.getColPath(tableTypeExpr, dbName, tableName, partSpec);

        if (partSpec != null) {
            handleUnsupportedOperation("DESCRIBE PARTITION is not supported");
        }
        if (colPath != null) {
            handleUnsupportedOperation("DESCRIBE COLUMNS is not supported");
        }

        DescTableDesc descTblDesc = new DescTableDesc(getResFile(), tableName, partSpec, colPath);

        if (ast.getChildCount() == 2) {
            int descOptions = ast.getChild(1).getType();
            descTblDesc.setFormatted(descOptions == HiveASTParser.KW_FORMATTED);
            descTblDesc.setExt(descOptions == HiveASTParser.KW_EXTENDED);
            if (descOptions == HiveASTParser.KW_PRETTY) {
                handleUnsupportedOperation("DESCRIBE PRETTY is not supported.");
            }
        }

        return new DDLWork(getInputs(), getOutputs(), descTblDesc);
    }

    private Serializable analyzeDescDatabase(HiveParserASTNode ast) {

        boolean isExtended;
        String dbName;

        if (ast.getChildCount() == 1) {
            dbName = HiveParserBaseSemanticAnalyzer.stripQuotes(ast.getChild(0).getText());
            isExtended = false;
        } else if (ast.getChildCount() == 2) {
            dbName = HiveParserBaseSemanticAnalyzer.stripQuotes(ast.getChild(0).getText());
            isExtended = true;
        } else {
            throw new ValidationException("Unexpected Tokens at DESCRIBE DATABASE");
        }

        DescDatabaseDesc descDbDesc = new DescDatabaseDesc(getResFile(), dbName, isExtended);
        return new DDLWork(getInputs(), getOutputs(), descDbDesc);
    }

    public static HashMap<String, String> getPartSpec(HiveParserASTNode partspec) {
        if (partspec == null) {
            return null;
        }
        HashMap<String, String> partSpec = new LinkedHashMap<>();
        for (int i = 0; i < partspec.getChildCount(); ++i) {
            HiveParserASTNode partVal = (HiveParserASTNode) partspec.getChild(i);
            String key = partVal.getChild(0).getText();
            String val = null;
            if (partVal.getChildCount() == 3) {
                val = HiveParserBaseSemanticAnalyzer.stripQuotes(partVal.getChild(2).getText());
            } else if (partVal.getChildCount() == 2) {
                val = HiveParserBaseSemanticAnalyzer.stripQuotes(partVal.getChild(1).getText());
            }
            partSpec.put(key.toLowerCase(), val);
        }
        return partSpec;
    }

    private Serializable analyzeShowPartitions(HiveParserASTNode ast) throws SemanticException {
        ShowPartitionsDesc showPartsDesc;
        String tableName =
                HiveParserBaseSemanticAnalyzer.getUnescapedName(
                        (HiveParserASTNode) ast.getChild(0));
        List<Map<String, String>> partSpecs = getPartitionSpecs(ast);
        // We only can have a single partition spec
        assert (partSpecs.size() <= 1);
        Map<String, String> partSpec = null;
        if (partSpecs.size() > 0) {
            partSpec = partSpecs.get(0);
        }

        validateTable(tableName, null);

        showPartsDesc = new ShowPartitionsDesc(tableName, getResFile(), partSpec);
        return new DDLWork(getInputs(), getOutputs(), showPartsDesc);
    }

    private Serializable analyzeShowDatabases(HiveParserASTNode ast) {
        ShowDatabasesDesc showDatabasesDesc;
        if (ast.getChildCount() == 1) {
            String databasePattern =
                    HiveParserBaseSemanticAnalyzer.unescapeSQLString(ast.getChild(0).getText());
            showDatabasesDesc = new ShowDatabasesDesc(getResFile(), databasePattern);
        } else {
            showDatabasesDesc = new ShowDatabasesDesc(getResFile());
        }
        return new DDLWork(getInputs(), getOutputs(), showDatabasesDesc);
    }

    private Serializable analyzeShowTables(HiveParserASTNode ast, boolean expectView) {
        String dbName = currentDB;
        String pattern = null;

        if (ast.getChildCount() > 3) {
            throw new ValidationException("Internal error : Invalid AST " + ast.toStringTree());
        }

        switch (ast.getChildCount()) {
            case 1: // Uses a pattern
                pattern =
                        HiveParserBaseSemanticAnalyzer.unescapeSQLString(ast.getChild(0).getText());
                break;
            case 2: // Specifies a DB
                assert (ast.getChild(0).getType() == HiveASTParser.TOK_FROM);
                dbName =
                        HiveParserBaseSemanticAnalyzer.unescapeIdentifier(
                                ast.getChild(1).getText());
                break;
            case 3: // Uses a pattern and specifies a DB
                assert (ast.getChild(0).getType() == HiveASTParser.TOK_FROM);
                dbName =
                        HiveParserBaseSemanticAnalyzer.unescapeIdentifier(
                                ast.getChild(1).getText());
                pattern =
                        HiveParserBaseSemanticAnalyzer.unescapeSQLString(ast.getChild(2).getText());
                break;
            default: // No pattern or DB
                break;
        }
        if (!dbName.equalsIgnoreCase(currentDB)) {
            handleUnsupportedOperation("SHOW TABLES/VIEWS IN DATABASE is not supported");
        }
        if (pattern != null) {
            handleUnsupportedOperation("SHOW TABLES/VIEWS LIKE is not supported");
        }
        return new HiveParserShowTablesDesc(null, dbName, expectView);
    }

    /**
     * Add the task according to the parsed command tree. This is used for the CLI command "SHOW
     * FUNCTIONS;".
     *
     * @param ast The parsed command tree.
     */
    private Serializable analyzeShowFunctions(HiveParserASTNode ast) {
        ShowFunctionsDesc showFuncsDesc;
        if (ast.getChildCount() == 1) {
            String funcNames =
                    HiveParserBaseSemanticAnalyzer.stripQuotes(ast.getChild(0).getText());
            showFuncsDesc = new ShowFunctionsDesc(getResFile(), funcNames);
        } else if (ast.getChildCount() == 2) {
            assert (ast.getChild(0).getType() == HiveASTParser.KW_LIKE);
            throw new ValidationException("SHOW FUNCTIONS LIKE is not supported yet");
        } else {
            showFuncsDesc = new ShowFunctionsDesc(getResFile());
        }
        return new DDLWork(getInputs(), getOutputs(), showFuncsDesc);
    }

    /**
     * Add the task according to the parsed command tree. This is used for the CLI command "DESCRIBE
     * FUNCTION;".
     *
     * @param ast The parsed command tree.
     */
    private Serializable analyzeDescFunction(HiveParserASTNode ast) {
        String funcName;
        boolean isExtended;

        if (ast.getChildCount() == 1) {
            funcName = HiveParserBaseSemanticAnalyzer.stripQuotes(ast.getChild(0).getText());
            isExtended = false;
        } else if (ast.getChildCount() == 2) {
            funcName = HiveParserBaseSemanticAnalyzer.stripQuotes(ast.getChild(0).getText());
            isExtended = true;
        } else {
            throw new ValidationException("Unexpected Tokens at DESCRIBE FUNCTION");
        }

        DescFunctionDesc descFuncDesc = new DescFunctionDesc(getResFile(), funcName, isExtended);
        return new DDLWork(getInputs(), getOutputs(), descFuncDesc);
    }

    private Serializable analyzeAlterTableRename(
            String[] source, HiveParserASTNode ast, boolean expectView) throws SemanticException {
        String[] target =
                HiveParserBaseSemanticAnalyzer.getQualifiedTableName(
                        (HiveParserASTNode) ast.getChild(0));

        String sourceName = HiveParserBaseSemanticAnalyzer.getDotName(source);
        String targetName = HiveParserBaseSemanticAnalyzer.getDotName(target);

        return HiveParserAlterTableDesc.rename(sourceName, targetName, expectView);
    }

    private Serializable analyzeAlterTableRenameCol(
            String[] qualified, HiveParserASTNode ast, HashMap<String, String> partSpec)
            throws SemanticException {
        String newComment = null;
        boolean first = false;
        String flagCol = null;
        boolean isCascade = false;
        // col_old_name col_new_name column_type [COMMENT col_comment] [FIRST|AFTER column_name]
        // [CASCADE|RESTRICT]
        String oldColName = ast.getChild(0).getText();
        String newColName = ast.getChild(1).getText();
        String newType =
                HiveParserBaseSemanticAnalyzer.getTypeStringFromAST(
                        (HiveParserASTNode) ast.getChild(2));
        int childCount = ast.getChildCount();
        for (int i = 3; i < childCount; i++) {
            HiveParserASTNode child = (HiveParserASTNode) ast.getChild(i);
            switch (child.getToken().getType()) {
                case HiveASTParser.StringLiteral:
                    newComment = HiveParserBaseSemanticAnalyzer.unescapeSQLString(child.getText());
                    break;
                case HiveASTParser.TOK_ALTERTABLE_CHANGECOL_AFTER_POSITION:
                    flagCol =
                            HiveParserBaseSemanticAnalyzer.unescapeIdentifier(
                                    child.getChild(0).getText());
                    break;
                case HiveASTParser.KW_FIRST:
                    first = true;
                    break;
                case HiveASTParser.TOK_CASCADE:
                    isCascade = true;
                    break;
                case HiveASTParser.TOK_RESTRICT:
                    break;
                default:
                    throw new ValidationException(
                            "Unsupported token: " + child.getToken() + " for alter table");
            }
        }

        // Validate the operation of renaming a column name.
        Table tab = getTable(new ObjectPath(qualified[0], qualified[1]));

        SkewedInfo skewInfo = tab.getTTable().getSd().getSkewedInfo();
        if ((null != skewInfo)
                && (null != skewInfo.getSkewedColNames())
                && skewInfo.getSkewedColNames().contains(oldColName)) {
            throw new ValidationException(
                    oldColName + ErrorMsg.ALTER_TABLE_NOT_ALLOWED_RENAME_SKEWED_COLUMN.getMsg());
        }

        String tblName = HiveParserBaseSemanticAnalyzer.getDotName(qualified);
        return HiveParserAlterTableDesc.changeColumn(
                tblName,
                HiveParserBaseSemanticAnalyzer.unescapeIdentifier(oldColName),
                HiveParserBaseSemanticAnalyzer.unescapeIdentifier(newColName),
                newType,
                newComment,
                first,
                flagCol,
                isCascade);
    }

    private Serializable analyzeAlterTableModifyCols(
            String[] qualified,
            HiveParserASTNode ast,
            HashMap<String, String> partSpec,
            boolean replace)
            throws SemanticException {

        String tblName = HiveParserBaseSemanticAnalyzer.getDotName(qualified);
        List<FieldSchema> newCols =
                HiveParserBaseSemanticAnalyzer.getColumns((HiveParserASTNode) ast.getChild(0));
        boolean isCascade = false;
        if (null != ast.getFirstChildWithType(HiveASTParser.TOK_CASCADE)) {
            isCascade = true;
        }

        return HiveParserAlterTableDesc.addReplaceColumns(tblName, newCols, replace, isCascade);
    }

    private Serializable analyzeAlterTableDropParts(
            String[] qualified, HiveParserASTNode ast, boolean expectView) {

        boolean ifExists = ast.getFirstChildWithType(HiveASTParser.TOK_IFEXISTS) != null;
        // If the drop has to fail on non-existent partitions, we cannot batch expressions.
        // That is because we actually have to check each separate expression for existence.
        // We could do a small optimization for the case where expr has all columns and all
        // operators are equality, if we assume those would always match one partition (which
        // may not be true with legacy, non-normalized column values). This is probably a
        // popular case but that's kinda hacky. Let's not do it for now.

        Table tab = getTable(new ObjectPath(qualified[0], qualified[1]));
        // hive represents drop partition specs with generic func desc, but what we need is just
        // spec maps
        List<Map<String, String>> partSpecs = new ArrayList<>();
        for (int i = 0; i < ast.getChildCount(); i++) {
            HiveParserASTNode child = (HiveParserASTNode) ast.getChild(i);
            if (child.getType() == HiveASTParser.TOK_PARTSPEC) {
                partSpecs.add(getPartSpec(child));
            }
        }

        validateAlterTableType(tab, AlterTableDesc.AlterTableTypes.DROPPARTITION, expectView);

        return new DropPartitionDesc(qualified[0], qualified[1], partSpecs, ifExists);
    }

    /**
     * Add one or more partitions to a table. Useful when the data has been copied to the right
     * location by some other process.
     */
    private Serializable analyzeAlterTableAddParts(
            String[] qualified, CommonTree ast, boolean expectView) throws SemanticException {

        // ^(TOK_ALTERTABLE_ADDPARTS identifier ifNotExists?
        // alterStatementSuffixAddPartitionsElement+)
        boolean ifNotExists = ast.getChild(0).getType() == HiveASTParser.TOK_IFNOTEXISTS;

        Table tab = getTable(new ObjectPath(qualified[0], qualified[1]));
        boolean isView = tab.isView();
        validateAlterTableType(tab, AlterTableDesc.AlterTableTypes.ADDPARTITION, expectView);

        int numCh = ast.getChildCount();
        int start = ifNotExists ? 1 : 0;

        String currentLocation = null;
        Map<String, String> currentPart = null;
        // Parser has done some verification, so the order of tokens doesn't need to be verified
        // here.
        AddPartitionDesc addPartitionDesc =
                new AddPartitionDesc(tab.getDbName(), tab.getTableName(), ifNotExists);
        for (int num = start; num < numCh; num++) {
            HiveParserASTNode child = (HiveParserASTNode) ast.getChild(num);
            switch (child.getToken().getType()) {
                case HiveASTParser.TOK_PARTSPEC:
                    if (currentPart != null) {
                        addPartitionDesc.addPartition(currentPart, currentLocation);
                        currentLocation = null;
                    }
                    currentPart = getPartSpec(child);
                    validatePartitionValues(currentPart); // validate reserved values
                    break;
                case HiveASTParser.TOK_PARTITIONLOCATION:
                    // if location specified, set in partition
                    if (isView) {
                        throw new ValidationException("LOCATION clause illegal for view partition");
                    }
                    currentLocation =
                            HiveParserBaseSemanticAnalyzer.unescapeSQLString(
                                    child.getChild(0).getText());
                    break;
                default:
                    throw new ValidationException("Unknown child: " + child);
            }
        }

        // add the last one
        if (currentPart != null) {
            addPartitionDesc.addPartition(currentPart, currentLocation);
        }

        return new DDLWork(getInputs(), getOutputs(), addPartitionDesc);
    }

    // Get the partition specs from the tree
    private List<Map<String, String>> getPartitionSpecs(CommonTree ast) {
        List<Map<String, String>> partSpecs = new ArrayList<>();
        // get partition metadata if partition specified
        for (int childIndex = 0; childIndex < ast.getChildCount(); childIndex++) {
            HiveParserASTNode partSpecNode = (HiveParserASTNode) ast.getChild(childIndex);
            // sanity check
            if (partSpecNode.getType() == HiveASTParser.TOK_PARTSPEC) {
                Map<String, String> partSpec = getPartSpec(partSpecNode);
                partSpecs.add(partSpec);
            }
        }
        return partSpecs;
    }

    /**
     * Certain partition values are are used by hive. e.g. the default partition in dynamic
     * partitioning and the intermediate partition values used in the archiving process. Naturally,
     * prohibit the user from creating partitions with these reserved values. The check that this
     * function is more restrictive than the actual limitation, but it's simpler. Should be okay
     * since the reserved names are fairly long and uncommon.
     */
    private void validatePartitionValues(Map<String, String> partSpec) {
        for (Map.Entry<String, String> e : partSpec.entrySet()) {
            for (String s : reservedPartitionValues) {
                String value = e.getValue();
                if (value != null && value.contains(s)) {
                    throw new ValidationException(
                            ErrorMsg.RESERVED_PART_VAL.getMsg(
                                    "(User value: "
                                            + e.getValue()
                                            + " Reserved substring: "
                                            + s
                                            + ")"));
                }
            }
        }
    }

    private Map<String, String> addDefaultProperties(Map<String, String> tblProp) {
        Map<String, String> retValue;
        if (tblProp == null) {
            retValue = new HashMap<>();
        } else {
            retValue = tblProp;
        }
        String paraString = HiveConf.getVar(conf, HiveConf.ConfVars.NEWTABLEDEFAULTPARA);
        if (paraString != null && !paraString.isEmpty()) {
            for (String keyValuePair : paraString.split(",")) {
                String[] keyValue = keyValuePair.split("=", 2);
                if (keyValue.length != 2) {
                    continue;
                }
                if (!retValue.containsKey(keyValue[0])) {
                    retValue.put(keyValue[0], keyValue[1]);
                }
            }
        }
        return retValue;
    }

    private Path getResFile() {
        return SessionState.getLocalSessionPath(conf);
    }

    private static void handleUnsupportedOperation(HiveParserASTNode astNode) {
        throw new ValidationException(
                null, new UnsupportedOperationException("Unsupported operation: " + astNode));
    }

    private static void handleUnsupportedOperation(String message) {
        throw new ValidationException(null, new UnsupportedOperationException(message));
    }
}
