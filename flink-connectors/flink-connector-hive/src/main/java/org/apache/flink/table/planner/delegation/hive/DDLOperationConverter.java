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
import org.apache.flink.sql.parser.hive.ddl.HiveDDLUtils;
import org.apache.flink.sql.parser.hive.ddl.SqlAlterHiveDatabase.AlterHiveDatabaseOp;
import org.apache.flink.table.api.TableColumn;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.WatermarkSpec;
import org.apache.flink.table.api.constraints.UniqueConstraint;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.CatalogFunction;
import org.apache.flink.table.catalog.CatalogFunctionImpl;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.CatalogPartition;
import org.apache.flink.table.catalog.CatalogPartitionImpl;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.CatalogPropertiesUtil;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.CatalogView;
import org.apache.flink.table.catalog.CatalogViewImpl;
import org.apache.flink.table.catalog.FunctionLanguage;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.UnresolvedIdentifier;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.catalog.hive.client.HiveShim;
import org.apache.flink.table.catalog.hive.factories.HiveFunctionDefinitionFactory;
import org.apache.flink.table.catalog.hive.util.HiveTableUtil;
import org.apache.flink.table.catalog.hive.util.HiveTypeUtil;
import org.apache.flink.table.delegation.Parser;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.operations.DescribeTableOperation;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.ShowDatabasesOperation;
import org.apache.flink.table.operations.ShowFunctionsOperation;
import org.apache.flink.table.operations.ShowPartitionsOperation;
import org.apache.flink.table.operations.ShowTablesOperation;
import org.apache.flink.table.operations.ShowViewsOperation;
import org.apache.flink.table.operations.UseDatabaseOperation;
import org.apache.flink.table.operations.ddl.AddPartitionsOperation;
import org.apache.flink.table.operations.ddl.AlterDatabaseOperation;
import org.apache.flink.table.operations.ddl.AlterPartitionPropertiesOperation;
import org.apache.flink.table.operations.ddl.AlterTableOptionsOperation;
import org.apache.flink.table.operations.ddl.AlterTableRenameOperation;
import org.apache.flink.table.operations.ddl.AlterTableSchemaOperation;
import org.apache.flink.table.operations.ddl.AlterViewAsOperation;
import org.apache.flink.table.operations.ddl.AlterViewPropertiesOperation;
import org.apache.flink.table.operations.ddl.AlterViewRenameOperation;
import org.apache.flink.table.operations.ddl.CreateCatalogFunctionOperation;
import org.apache.flink.table.operations.ddl.CreateDatabaseOperation;
import org.apache.flink.table.operations.ddl.CreateTableOperation;
import org.apache.flink.table.operations.ddl.CreateTempSystemFunctionOperation;
import org.apache.flink.table.operations.ddl.CreateViewOperation;
import org.apache.flink.table.operations.ddl.DropCatalogFunctionOperation;
import org.apache.flink.table.operations.ddl.DropDatabaseOperation;
import org.apache.flink.table.operations.ddl.DropPartitionsOperation;
import org.apache.flink.table.operations.ddl.DropTableOperation;
import org.apache.flink.table.operations.ddl.DropTempSystemFunctionOperation;
import org.apache.flink.table.operations.ddl.DropViewOperation;
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
import org.apache.flink.table.planner.delegation.hive.parse.HiveParserBaseSemanticAnalyzer;
import org.apache.flink.table.planner.delegation.hive.parse.HiveParserStorageFormat;
import org.apache.flink.table.planner.utils.OperationConverterUtils;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.plan.AddPartitionDesc;
import org.apache.hadoop.hive.ql.plan.CreateDatabaseDesc;
import org.apache.hadoop.hive.ql.plan.CreateFunctionDesc;
import org.apache.hadoop.hive.ql.plan.DDLWork;
import org.apache.hadoop.hive.ql.plan.DescTableDesc;
import org.apache.hadoop.hive.ql.plan.FunctionWork;
import org.apache.hadoop.hive.ql.plan.PrincipalDesc;
import org.apache.hadoop.hive.ql.plan.ShowDatabasesDesc;
import org.apache.hadoop.hive.ql.plan.ShowFunctionsDesc;
import org.apache.hadoop.hive.ql.plan.ShowPartitionsDesc;
import org.apache.hadoop.hive.ql.plan.SwitchDatabaseDesc;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.flink.sql.parser.hive.ddl.HiveDDLUtils.COL_DELIMITER;
import static org.apache.flink.sql.parser.hive.ddl.SqlAlterHiveDatabase.ALTER_DATABASE_OP;
import static org.apache.flink.sql.parser.hive.ddl.SqlAlterHiveDatabaseOwner.DATABASE_OWNER_NAME;
import static org.apache.flink.sql.parser.hive.ddl.SqlAlterHiveDatabaseOwner.DATABASE_OWNER_TYPE;
import static org.apache.flink.sql.parser.hive.ddl.SqlAlterHiveTable.ALTER_COL_CASCADE;
import static org.apache.flink.sql.parser.hive.ddl.SqlAlterHiveTable.ALTER_TABLE_OP;
import static org.apache.flink.sql.parser.hive.ddl.SqlAlterHiveTable.AlterTableOp.ALTER_COLUMNS;
import static org.apache.flink.sql.parser.hive.ddl.SqlAlterHiveTable.AlterTableOp.CHANGE_FILE_FORMAT;
import static org.apache.flink.sql.parser.hive.ddl.SqlAlterHiveTable.AlterTableOp.CHANGE_LOCATION;
import static org.apache.flink.sql.parser.hive.ddl.SqlAlterHiveTable.AlterTableOp.CHANGE_SERDE_PROPS;
import static org.apache.flink.sql.parser.hive.ddl.SqlAlterHiveTable.AlterTableOp.CHANGE_TBL_PROPS;
import static org.apache.flink.sql.parser.hive.ddl.SqlCreateHiveDatabase.DATABASE_LOCATION_URI;
import static org.apache.flink.sql.parser.hive.ddl.SqlCreateHiveTable.HiveTableRowFormat.COLLECTION_DELIM;
import static org.apache.flink.sql.parser.hive.ddl.SqlCreateHiveTable.HiveTableRowFormat.ESCAPE_CHAR;
import static org.apache.flink.sql.parser.hive.ddl.SqlCreateHiveTable.HiveTableRowFormat.FIELD_DELIM;
import static org.apache.flink.sql.parser.hive.ddl.SqlCreateHiveTable.HiveTableRowFormat.LINE_DELIM;
import static org.apache.flink.sql.parser.hive.ddl.SqlCreateHiveTable.HiveTableRowFormat.MAPKEY_DELIM;
import static org.apache.flink.sql.parser.hive.ddl.SqlCreateHiveTable.HiveTableRowFormat.SERDE_INFO_PROP_PREFIX;
import static org.apache.flink.sql.parser.hive.ddl.SqlCreateHiveTable.HiveTableRowFormat.SERDE_LIB_CLASS_NAME;
import static org.apache.flink.sql.parser.hive.ddl.SqlCreateHiveTable.HiveTableRowFormat.SERIALIZATION_NULL_FORMAT;
import static org.apache.flink.sql.parser.hive.ddl.SqlCreateHiveTable.HiveTableStoredAs.STORED_AS_FILE_FORMAT;
import static org.apache.flink.sql.parser.hive.ddl.SqlCreateHiveTable.HiveTableStoredAs.STORED_AS_INPUT_FORMAT;
import static org.apache.flink.sql.parser.hive.ddl.SqlCreateHiveTable.HiveTableStoredAs.STORED_AS_OUTPUT_FORMAT;
import static org.apache.flink.sql.parser.hive.ddl.SqlCreateHiveTable.NOT_NULL_COLS;
import static org.apache.flink.sql.parser.hive.ddl.SqlCreateHiveTable.NOT_NULL_CONSTRAINT_TRAITS;
import static org.apache.flink.sql.parser.hive.ddl.SqlCreateHiveTable.PK_CONSTRAINT_TRAIT;
import static org.apache.flink.sql.parser.hive.ddl.SqlCreateHiveTable.TABLE_IS_EXTERNAL;
import static org.apache.flink.sql.parser.hive.ddl.SqlCreateHiveTable.TABLE_LOCATION_URI;

/** A converter to generate DDL operations. */
public class DDLOperationConverter {

    private final Parser parser;
    private final CatalogManager catalogManager;
    private final HiveFunctionDefinitionFactory funcDefFactory;

    public DDLOperationConverter(Parser parser, CatalogManager catalogManager, HiveShim hiveShim) {
        this.parser = parser;
        this.catalogManager = catalogManager;
        this.funcDefFactory = new HiveFunctionDefinitionFactory(hiveShim);
    }

    public Operation convert(Serializable work) {
        if (work instanceof DDLWork) {
            DDLWork ddlWork = (DDLWork) work;
            if (ddlWork.getCreateDatabaseDesc() != null) {
                return convertCreateDatabase(ddlWork.getCreateDatabaseDesc());
            } else if (ddlWork.getShowDatabasesDesc() != null) {
                return convertShowDatabases(ddlWork.getShowDatabasesDesc());
            } else if (ddlWork.getSwitchDatabaseDesc() != null) {
                return convertUseDatabase(ddlWork.getSwitchDatabaseDesc());
            } else if (ddlWork.getAddPartitionDesc() != null) {
                return convertAddPartitions(ddlWork.getAddPartitionDesc());
            } else if (ddlWork.getShowPartsDesc() != null) {
                return convertShowPartitions(ddlWork.getShowPartsDesc());
            } else if (ddlWork.getShowFuncsDesc() != null) {
                return convertShowFunctions(ddlWork.getShowFuncsDesc());
            } else if (ddlWork.getDescTblDesc() != null) {
                return convertDescTable(ddlWork.getDescTblDesc());
            } else {
                throw new FlinkHiveException("Unsupported DDLWork");
            }
        } else if (work instanceof HiveParserShowTablesDesc) {
            if (((HiveParserShowTablesDesc) work).isExpectView()) {
                return convertShowViews((HiveParserShowTablesDesc) work);
            } else {
                return convertShowTables((HiveParserShowTablesDesc) work);
            }
        } else if (work instanceof HiveParserAlterTableDesc) {
            HiveParserAlterTableDesc alterTableDesc = (HiveParserAlterTableDesc) work;
            if (alterTableDesc.expectView()) {
                return convertAlterView(alterTableDesc);
            } else {
                return convertAlterTable(alterTableDesc);
            }
        } else if (work instanceof HiveParserCreateTableDesc) {
            return convertCreateTable((HiveParserCreateTableDesc) work);
        } else if (work instanceof HiveParserDropTableDesc) {
            HiveParserDropTableDesc dropTableDesc = (HiveParserDropTableDesc) work;
            if (dropTableDesc.isExpectView()) {
                return convertDropView(dropTableDesc);
            } else {
                return convertDropTable(dropTableDesc);
            }
        } else if (work instanceof DropPartitionDesc) {
            return convertDropPartitions((DropPartitionDesc) work);
        } else if (work instanceof HiveParserCreateViewDesc) {
            return convertCreateAlterView((HiveParserCreateViewDesc) work);
        } else if (work instanceof HiveParserDropFunctionDesc) {
            return convertDropFunction((HiveParserDropFunctionDesc) work);
        } else if (work instanceof FunctionWork) {
            FunctionWork functionWork = (FunctionWork) work;
            if (functionWork.getCreateFunctionDesc() != null) {
                return convertCreateFunction(functionWork.getCreateFunctionDesc());
            }
            throw new FlinkHiveException("Unsupported FunctionWork");
        } else if (work instanceof HiveParserAlterDatabaseDesc) {
            return convertAlterDatabase((HiveParserAlterDatabaseDesc) work);
        } else if (work instanceof HiveParserDropDatabaseDesc) {
            return convertDropDatabase((HiveParserDropDatabaseDesc) work);
        } else {
            throw new FlinkHiveException("Unsupported work class " + work.getClass().getName());
        }
    }

    private Operation convertDescTable(DescTableDesc desc) {
        ObjectIdentifier tableIdentifier = parseObjectIdentifier(desc.getTableName());
        return new DescribeTableOperation(tableIdentifier, desc.isExt() || desc.isFormatted());
    }

    private Operation convertShowFunctions(ShowFunctionsDesc desc) {
        return new ShowFunctionsOperation();
    }

    private Operation convertShowPartitions(ShowPartitionsDesc desc) {
        ObjectIdentifier tableIdentifier = parseObjectIdentifier(desc.getTabName());
        CatalogPartitionSpec spec = null;
        if (desc.getPartSpec() != null && !desc.getPartSpec().isEmpty()) {
            spec = new CatalogPartitionSpec(new HashMap<>(desc.getPartSpec()));
        }
        return new ShowPartitionsOperation(tableIdentifier, spec);
    }

    private Operation convertCreateFunction(CreateFunctionDesc desc) {
        if (desc.isTemp()) {
            // hive's temporary function is more like flink's temp system function, e.g. doesn't
            // belong to a catalog/db
            // the DDL analyzer makes sure temp function name is not a compound one
            FunctionDefinition funcDefinition =
                    funcDefFactory.createFunctionDefinition(
                            desc.getFunctionName(),
                            new CatalogFunctionImpl(desc.getClassName(), FunctionLanguage.JAVA));
            return new CreateTempSystemFunctionOperation(
                    desc.getFunctionName(), false, funcDefinition);
        } else {
            ObjectIdentifier identifier = parseObjectIdentifier(desc.getFunctionName());
            CatalogFunction catalogFunction =
                    new CatalogFunctionImpl(desc.getClassName(), FunctionLanguage.JAVA);
            return new CreateCatalogFunctionOperation(
                    identifier, catalogFunction, false, desc.isTemp());
        }
    }

    private Operation convertDropFunction(HiveParserDropFunctionDesc desc) {
        if (desc.getDesc().isTemp()) {
            return new DropTempSystemFunctionOperation(
                    desc.getDesc().getFunctionName(), desc.ifExists());
        } else {
            ObjectIdentifier identifier = parseObjectIdentifier(desc.getDesc().getFunctionName());
            return new DropCatalogFunctionOperation(
                    identifier, desc.ifExists(), desc.getDesc().isTemp());
        }
    }

    private Operation convertDropView(HiveParserDropTableDesc desc) {
        ObjectIdentifier identifier = parseObjectIdentifier(desc.getCompoundName());
        CatalogBaseTable baseTable = getCatalogBaseTable(identifier, true);
        if (baseTable instanceof CatalogTable) {
            throw new ValidationException("DROP VIEW for a table is not allowed");
        }
        return new DropViewOperation(identifier, desc.ifExists(), false);
    }

    private Operation convertDropTable(HiveParserDropTableDesc desc) {
        ObjectIdentifier identifier = parseObjectIdentifier(desc.getCompoundName());
        CatalogBaseTable baseTable = getCatalogBaseTable(identifier, true);
        if (baseTable instanceof CatalogView) {
            throw new ValidationException("DROP TABLE for a view is not allowed");
        }
        return new DropTableOperation(identifier, desc.ifExists(), false);
    }

    // handles both create view and alter view as
    private Operation convertCreateAlterView(HiveParserCreateViewDesc desc) {
        ObjectIdentifier viewIdentifier = parseObjectIdentifier(desc.getCompoundName());
        TableSchema schema =
                HiveTableUtil.createTableSchema(
                        desc.getSchema(), Collections.emptyList(), Collections.emptySet(), null);
        Map<String, String> props = new HashMap<>();
        String comment;
        if (desc.isAlterViewAs()) {
            CatalogBaseTable baseTable = getCatalogBaseTable(viewIdentifier);
            if (baseTable instanceof CatalogTable) {
                throw new ValidationException("ALTER VIEW for a table is not allowed");
            }
            props.putAll(baseTable.getOptions());
            comment = baseTable.getComment();
        } else {
            markNonGeneric(props);
            comment = desc.getComment();
            if (desc.getTblProps() != null) {
                props.putAll(desc.getTblProps());
            }
        }
        CatalogView catalogView =
                new CatalogViewImpl(
                        desc.getOriginalText(), desc.getExpandedText(), schema, props, comment);
        if (desc.isAlterViewAs()) {
            return new AlterViewAsOperation(viewIdentifier, catalogView);
        } else {
            return new CreateViewOperation(viewIdentifier, catalogView, desc.ifNotExists(), false);
        }
    }

    private Operation convertDropPartitions(DropPartitionDesc desc) {
        ObjectIdentifier tableIdentifier =
                catalogManager.qualifyIdentifier(
                        UnresolvedIdentifier.of(desc.getDbName(), desc.getTableName()));
        CatalogBaseTable catalogBaseTable = getCatalogBaseTable(tableIdentifier);
        if (catalogBaseTable instanceof CatalogView) {
            throw new ValidationException("DROP PARTITION for a view is not supported");
        }
        List<CatalogPartitionSpec> specs =
                desc.getSpecs().stream()
                        .map(CatalogPartitionSpec::new)
                        .collect(Collectors.toList());
        return new DropPartitionsOperation(tableIdentifier, desc.ifExists(), specs);
    }

    private Operation convertAddPartitions(AddPartitionDesc desc) {
        ObjectIdentifier tableIdentifier =
                desc.getDbName() == null
                        ? parseObjectIdentifier(desc.getTableName())
                        : catalogManager.qualifyIdentifier(
                                UnresolvedIdentifier.of(desc.getDbName(), desc.getTableName()));
        CatalogBaseTable catalogBaseTable = getCatalogBaseTable(tableIdentifier);
        if (catalogBaseTable instanceof CatalogView) {
            throw new ValidationException("ADD PARTITION for a view is not supported");
        }
        List<CatalogPartitionSpec> specs = new ArrayList<>();
        List<CatalogPartition> partitions = new ArrayList<>();
        for (int i = 0; i < desc.getPartitionCount(); i++) {
            specs.add(new CatalogPartitionSpec(desc.getPartition(i).getPartSpec()));
            Map<String, String> props = new HashMap<>();
            String location = desc.getPartition(i).getLocation();
            if (location != null) {
                props.put(TABLE_LOCATION_URI, location);
            }
            partitions.add(new CatalogPartitionImpl(props, null));
        }
        return new AddPartitionsOperation(tableIdentifier, desc.isIfNotExists(), specs, partitions);
    }

    private Operation convertCreateTable(HiveParserCreateTableDesc desc) {
        Map<String, String> props = new HashMap<>();
        if (desc.getTblProps() != null) {
            props.putAll(desc.getTblProps());
        }
        markNonGeneric(props);
        // external
        if (desc.isExternal()) {
            props.put(TABLE_IS_EXTERNAL, "true");
        }
        // PK trait
        UniqueConstraint uniqueConstraint = null;
        if (desc.getPrimaryKeys() != null && !desc.getPrimaryKeys().isEmpty()) {
            PrimaryKey primaryKey = desc.getPrimaryKeys().get(0);
            byte trait = 0;
            if (primaryKey.isEnable()) {
                trait = HiveDDLUtils.enableConstraint(trait);
            }
            if (primaryKey.isValidate()) {
                trait = HiveDDLUtils.validateConstraint(trait);
            }
            if (primaryKey.isRely()) {
                trait = HiveDDLUtils.relyConstraint(trait);
            }
            props.put(PK_CONSTRAINT_TRAIT, String.valueOf(trait));
            uniqueConstraint =
                    UniqueConstraint.primaryKey(
                            primaryKey.getConstraintName(),
                            desc.getPrimaryKeys().stream()
                                    .map(PrimaryKey::getPk)
                                    .collect(Collectors.toList()));
        }
        // NOT NULL constraints
        List<String> notNullCols = new ArrayList<>();
        if (!desc.getNotNullConstraints().isEmpty()) {
            List<String> traits = new ArrayList<>();
            for (NotNullConstraint notNull : desc.getNotNullConstraints()) {
                byte trait = 0;
                if (notNull.isEnable()) {
                    trait = HiveDDLUtils.enableConstraint(trait);
                }
                if (notNull.isValidate()) {
                    trait = HiveDDLUtils.validateConstraint(trait);
                }
                if (notNull.isRely()) {
                    trait = HiveDDLUtils.relyConstraint(trait);
                }
                traits.add(String.valueOf(trait));
                notNullCols.add(notNull.getColName());
            }
            props.put(NOT_NULL_CONSTRAINT_TRAITS, String.join(COL_DELIMITER, traits));
            props.put(NOT_NULL_COLS, String.join(COL_DELIMITER, notNullCols));
        }
        // row format
        if (desc.getRowFormatParams() != null) {
            encodeRowFormat(desc.getRowFormatParams(), props);
        }
        // storage format
        if (desc.getStorageFormat() != null) {
            encodeStorageFormat(desc.getStorageFormat(), props);
        }
        // location
        if (desc.getLocation() != null) {
            props.put(TABLE_LOCATION_URI, desc.getLocation());
        }
        ObjectIdentifier identifier = parseObjectIdentifier(desc.getCompoundName());
        TableSchema tableSchema =
                HiveTableUtil.createTableSchema(
                        desc.getCols(),
                        desc.getPartCols(),
                        new HashSet<>(notNullCols),
                        uniqueConstraint);
        return new CreateTableOperation(
                identifier,
                new CatalogTableImpl(
                        tableSchema,
                        HiveCatalog.getFieldNames(desc.getPartCols()),
                        props,
                        desc.getComment()),
                desc.ifNotExists(),
                desc.isTemporary());
    }

    private Operation convertAlterView(HiveParserAlterTableDesc desc) {
        ObjectIdentifier viewIdentifier = parseObjectIdentifier(desc.getCompoundName());
        CatalogBaseTable baseTable = getCatalogBaseTable(viewIdentifier);
        if (baseTable instanceof CatalogTable) {
            throw new ValidationException("ALTER VIEW for a table is not allowed");
        }
        CatalogView oldView = (CatalogView) baseTable;
        switch (desc.getOp()) {
            case RENAME:
                return new AlterViewRenameOperation(
                        viewIdentifier, parseObjectIdentifier(desc.getNewName()));
            case ADDPROPS:
                Map<String, String> props = new HashMap<>(oldView.getOptions());
                props.putAll(desc.getProps());
                CatalogView newView =
                        new CatalogViewImpl(
                                oldView.getOriginalQuery(),
                                oldView.getExpandedQuery(),
                                oldView.getSchema(),
                                props,
                                oldView.getComment());
                return new AlterViewPropertiesOperation(viewIdentifier, newView);
            default:
                throw new FlinkHiveException("Unsupported alter view operation " + desc.getOp());
        }
    }

    private Operation convertAlterTable(HiveParserAlterTableDesc desc) {
        ObjectIdentifier tableIdentifier = parseObjectIdentifier(desc.getCompoundName());
        CatalogBaseTable catalogBaseTable = getCatalogBaseTable(tableIdentifier);
        if (catalogBaseTable instanceof CatalogView) {
            throw new ValidationException("ALTER TABLE for a view is not allowed");
        }
        CatalogTable oldTable = (CatalogTable) catalogBaseTable;
        CatalogPartitionSpec partSpec =
                desc.getPartSpec() != null ? new CatalogPartitionSpec(desc.getPartSpec()) : null;
        CatalogPartition catalogPartition =
                partSpec != null ? getPartition(tableIdentifier, partSpec) : null;
        Map<String, String> newProps = new HashMap<>();
        switch (desc.getOp()) {
            case RENAME:
                return new AlterTableRenameOperation(
                        tableIdentifier, parseObjectIdentifier(desc.getNewName()));
            case ADDPROPS:
                newProps.put(ALTER_TABLE_OP, CHANGE_TBL_PROPS.name());
                newProps.putAll(desc.getProps());
                return convertAlterTableProps(
                        tableIdentifier, oldTable, partSpec, catalogPartition, newProps);
            case ALTERLOCATION:
                newProps.put(ALTER_TABLE_OP, CHANGE_LOCATION.name());
                newProps.put(TABLE_LOCATION_URI, desc.getNewLocation());
                return convertAlterTableProps(
                        tableIdentifier, oldTable, partSpec, catalogPartition, newProps);
            case ADDFILEFORMAT:
                newProps.put(ALTER_TABLE_OP, CHANGE_FILE_FORMAT.name());
                newProps.put(STORED_AS_FILE_FORMAT, desc.getGenericFileFormatName());
                return convertAlterTableProps(
                        tableIdentifier, oldTable, partSpec, catalogPartition, newProps);
            case ADDSERDE:
            case ADDSERDEPROPS:
                newProps.put(ALTER_TABLE_OP, CHANGE_SERDE_PROPS.name());
                if (desc.getSerdeName() != null) {
                    newProps.put(SERDE_LIB_CLASS_NAME, desc.getSerdeName());
                }
                if (desc.getProps() != null) {
                    for (String key : desc.getProps().keySet()) {
                        newProps.put(SERDE_INFO_PROP_PREFIX + key, desc.getProps().get(key));
                    }
                }
                return convertAlterTableProps(
                        tableIdentifier, oldTable, partSpec, catalogPartition, newProps);
            case REPLACECOLS:
                return convertAddReplaceColumns(
                        tableIdentifier, oldTable, desc.getNewCols(), true, desc.isCascade());
            case ADDCOLS:
                return convertAddReplaceColumns(
                        tableIdentifier, oldTable, desc.getNewCols(), false, desc.isCascade());
            case RENAMECOLUMN:
                return convertChangeColumn(
                        tableIdentifier,
                        oldTable,
                        desc.getOldColName(),
                        desc.getNewColName(),
                        desc.getNewColType(),
                        desc.getNewColComment(),
                        desc.getAfter(),
                        desc.isFirst(),
                        desc.isCascade());
            default:
                throw new FlinkHiveException("Unsupported alter table operation " + desc.getOp());
        }
    }

    private Operation convertChangeColumn(
            ObjectIdentifier tableIdentifier,
            CatalogTable oldTable,
            String oldName,
            String newName,
            String newType,
            String newComment,
            String after,
            boolean first,
            boolean cascade) {
        if (oldTable.getPartitionKeys().contains(oldName)) {
            // disallow changing partition columns
            throw new ValidationException("CHANGE COLUMN cannot be applied to partition columns");
        }
        TableSchema oldSchema = oldTable.getSchema();
        TableColumn newTableColumn =
                TableColumn.physical(
                        newName,
                        HiveTypeUtil.toFlinkType(TypeInfoUtils.getTypeInfoFromTypeString(newType)));
        TableSchema newSchema =
                OperationConverterUtils.changeColumn(
                        oldSchema, oldName, newTableColumn, first, after);
        Map<String, String> props = new HashMap<>(oldTable.getOptions());
        props.put(ALTER_TABLE_OP, ALTER_COLUMNS.name());
        if (cascade) {
            props.put(ALTER_COL_CASCADE, "true");
        }
        return new AlterTableSchemaOperation(
                tableIdentifier,
                new CatalogTableImpl(
                        newSchema, oldTable.getPartitionKeys(), props, oldTable.getComment()));
    }

    private Operation convertAddReplaceColumns(
            ObjectIdentifier tableIdentifier,
            CatalogTable oldTable,
            List<FieldSchema> newCols,
            boolean replace,
            boolean cascade) {
        // prepare properties
        Map<String, String> props = new HashMap<>(oldTable.getOptions());
        props.put(ALTER_TABLE_OP, ALTER_COLUMNS.name());
        if (cascade) {
            props.put(ALTER_COL_CASCADE, "true");
        }
        TableSchema oldSchema = oldTable.getSchema();
        final int numPartCol = oldTable.getPartitionKeys().size();
        TableSchema.Builder builder = TableSchema.builder();
        // add existing non-part col if we're not replacing
        if (!replace) {
            List<TableColumn> nonPartCols =
                    oldSchema.getTableColumns().subList(0, oldSchema.getFieldCount() - numPartCol);
            for (TableColumn column : nonPartCols) {
                builder.add(column);
            }
            setWatermarkAndPK(builder, oldSchema);
        }
        // add new cols
        for (FieldSchema col : newCols) {
            builder.add(
                    TableColumn.physical(
                            col.getName(),
                            HiveTypeUtil.toFlinkType(
                                    TypeInfoUtils.getTypeInfoFromTypeString(col.getType()))));
        }
        // add part cols
        List<TableColumn> partCols =
                oldSchema
                        .getTableColumns()
                        .subList(oldSchema.getFieldCount() - numPartCol, oldSchema.getFieldCount());
        for (TableColumn column : partCols) {
            builder.add(column);
        }
        return new AlterTableSchemaOperation(
                tableIdentifier,
                new CatalogTableImpl(
                        builder.build(),
                        oldTable.getPartitionKeys(),
                        props,
                        oldTable.getComment()));
    }

    private Operation convertAlterTableProps(
            ObjectIdentifier tableIdentifier,
            CatalogTable oldTable,
            CatalogPartitionSpec partSpec,
            CatalogPartition catalogPartition,
            Map<String, String> newProps) {
        Map<String, String> props = new HashMap<>();
        if (catalogPartition != null) {
            props.putAll(catalogPartition.getProperties());
            props.putAll(newProps);
            return new AlterPartitionPropertiesOperation(
                    tableIdentifier,
                    partSpec,
                    new CatalogPartitionImpl(props, catalogPartition.getComment()));
        } else {
            props.putAll(oldTable.getOptions());
            props.putAll(newProps);
            return new AlterTableOptionsOperation(tableIdentifier, oldTable.copy(props));
        }
    }

    private Operation convertShowViews(HiveParserShowTablesDesc desc) {
        return new ShowViewsOperation();
    }

    private Operation convertShowTables(HiveParserShowTablesDesc desc) {
        return new ShowTablesOperation();
    }

    private Operation convertUseDatabase(SwitchDatabaseDesc desc) {
        return new UseDatabaseOperation(catalogManager.getCurrentCatalog(), desc.getDatabaseName());
    }

    private Operation convertShowDatabases(ShowDatabasesDesc desc) {
        return new ShowDatabasesOperation();
    }

    private Operation convertDropDatabase(HiveParserDropDatabaseDesc desc) {
        return new DropDatabaseOperation(
                catalogManager.getCurrentCatalog(),
                desc.getDatabaseName(),
                desc.ifExists(),
                desc.cascade());
    }

    private Operation convertAlterDatabase(HiveParserAlterDatabaseDesc desc) {
        Catalog catalog = catalogManager.getCatalog(catalogManager.getCurrentCatalog()).get();
        CatalogDatabase originDB;
        try {
            originDB = catalog.getDatabase(desc.getDatabaseName());
        } catch (DatabaseNotExistException e) {
            throw new ValidationException(
                    String.format("Database %s not exists", desc.getDatabaseName()), e);
        }
        Map<String, String> props = new HashMap<>(originDB.getProperties());
        switch (desc.getAlterType()) {
            case ALTER_PROPERTY:
                props.put(ALTER_DATABASE_OP, AlterHiveDatabaseOp.CHANGE_PROPS.name());
                props.putAll(desc.getDbProperties());
                break;
            case ALTER_OWNER:
                props.put(ALTER_DATABASE_OP, AlterHiveDatabaseOp.CHANGE_OWNER.name());
                PrincipalDesc principalDesc = desc.getOwnerPrincipal();
                props.put(DATABASE_OWNER_NAME, principalDesc.getName());
                props.put(DATABASE_OWNER_TYPE, principalDesc.getType().name().toLowerCase());
                break;
            case ALTER_LOCATION:
                props.put(ALTER_DATABASE_OP, AlterHiveDatabaseOp.CHANGE_LOCATION.name());
                props.put(DATABASE_LOCATION_URI, desc.getLocation());
                break;
            default:
                throw new FlinkHiveException("Unsupported alter database operation");
        }
        CatalogDatabase newDB = new CatalogDatabaseImpl(props, originDB.getComment());
        return new AlterDatabaseOperation(
                catalogManager.getCurrentCatalog(), desc.getDatabaseName(), newDB);
    }

    private Operation convertCreateDatabase(CreateDatabaseDesc desc) {
        Map<String, String> props = new HashMap<>();
        if (desc.getDatabaseProperties() != null) {
            props.putAll(desc.getDatabaseProperties());
        }
        markNonGeneric(props);
        if (desc.getLocationUri() != null) {
            props.put(DATABASE_LOCATION_URI, desc.getLocationUri());
        }
        CatalogDatabase catalogDatabase = new CatalogDatabaseImpl(props, desc.getComment());
        return new CreateDatabaseOperation(
                catalogManager.getCurrentCatalog(),
                desc.getName(),
                catalogDatabase,
                desc.getIfNotExists());
    }

    private void markNonGeneric(Map<String, String> props) {
        props.put(CatalogPropertiesUtil.IS_GENERIC, "false");
    }

    private CatalogBaseTable getCatalogBaseTable(ObjectIdentifier tableIdentifier) {
        return getCatalogBaseTable(tableIdentifier, false);
    }

    private CatalogBaseTable getCatalogBaseTable(
            ObjectIdentifier tableIdentifier, boolean ifExists) {
        Optional<CatalogManager.TableLookupResult> optionalCatalogTable =
                catalogManager.getTable(tableIdentifier);
        if (!optionalCatalogTable.isPresent()) {
            if (ifExists) {
                return null;
            } else {
                throw new ValidationException(
                        String.format(
                                "Table or View %s doesn't exist.", tableIdentifier.toString()));
            }
        }
        if (optionalCatalogTable.get().isTemporary()) {
            throw new ValidationException(
                    String.format("Table or View %s is temporary.", tableIdentifier.toString()));
        }
        return optionalCatalogTable.get().getTable();
    }

    private CatalogPartition getPartition(
            ObjectIdentifier tableIdentifier, CatalogPartitionSpec partitionSpec) {
        return catalogManager
                .getPartition(tableIdentifier, partitionSpec)
                .orElseThrow(
                        () ->
                                new ValidationException(
                                        String.format(
                                                "Partition %s of table %s doesn't exist",
                                                partitionSpec.getPartitionSpec(),
                                                tableIdentifier)));
    }

    private ObjectIdentifier parseObjectIdentifier(String compoundName) {
        UnresolvedIdentifier unresolvedIdentifier = parser.parseIdentifier(compoundName);
        return catalogManager.qualifyIdentifier(unresolvedIdentifier);
    }

    private void encodeRowFormat(
            HiveParserBaseSemanticAnalyzer.HiveParserRowFormatParams rowFormatParams,
            Map<String, String> props) {
        if (rowFormatParams.getFieldDelim() != null) {
            props.put(FIELD_DELIM, rowFormatParams.getFieldDelim());
        }
        if (rowFormatParams.getCollItemDelim() != null) {
            props.put(COLLECTION_DELIM, rowFormatParams.getCollItemDelim());
        }
        if (rowFormatParams.getMapKeyDelim() != null) {
            props.put(MAPKEY_DELIM, rowFormatParams.getMapKeyDelim());
        }
        if (rowFormatParams.getFieldEscape() != null) {
            props.put(ESCAPE_CHAR, rowFormatParams.getFieldEscape());
        }
        if (rowFormatParams.getLineDelim() != null) {
            props.put(LINE_DELIM, rowFormatParams.getLineDelim());
        }
        if (rowFormatParams.getNullFormat() != null) {
            props.put(SERIALIZATION_NULL_FORMAT, rowFormatParams.getNullFormat());
        }
    }

    private void encodeStorageFormat(
            HiveParserStorageFormat storageFormat, Map<String, String> props) {
        String serdeName = storageFormat.getSerde();
        if (serdeName != null) {
            props.put(SERDE_LIB_CLASS_NAME, serdeName);
        }
        Map<String, String> serdeProps = storageFormat.getSerdeProps();
        if (serdeProps != null) {
            for (String serdeKey : serdeProps.keySet()) {
                props.put(SERDE_INFO_PROP_PREFIX + serdeKey, serdeProps.get(serdeKey));
            }
        }
        if (storageFormat.getInputFormat() != null) {
            props.put(STORED_AS_INPUT_FORMAT, storageFormat.getInputFormat());
        }
        if (storageFormat.getOutputFormat() != null) {
            props.put(STORED_AS_OUTPUT_FORMAT, storageFormat.getOutputFormat());
        }
    }

    private static void setWatermarkAndPK(TableSchema.Builder builder, TableSchema schema) {
        for (WatermarkSpec watermarkSpec : schema.getWatermarkSpecs()) {
            builder.watermark(watermarkSpec);
        }
        schema.getPrimaryKey()
                .ifPresent(
                        pk -> {
                            builder.primaryKey(
                                    pk.getName(), pk.getColumns().toArray(new String[0]));
                        });
    }
}
