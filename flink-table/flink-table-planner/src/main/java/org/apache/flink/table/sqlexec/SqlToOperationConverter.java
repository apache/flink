/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.sqlexec;

import org.apache.flink.sql.parser.ddl.SqlAlterDatabase;
import org.apache.flink.sql.parser.ddl.SqlAlterFunction;
import org.apache.flink.sql.parser.ddl.SqlAlterTable;
import org.apache.flink.sql.parser.ddl.SqlAlterTableProperties;
import org.apache.flink.sql.parser.ddl.SqlAlterTableRename;
import org.apache.flink.sql.parser.ddl.SqlCreateDatabase;
import org.apache.flink.sql.parser.ddl.SqlCreateFunction;
import org.apache.flink.sql.parser.ddl.SqlCreateTable;
import org.apache.flink.sql.parser.ddl.SqlDropDatabase;
import org.apache.flink.sql.parser.ddl.SqlDropFunction;
import org.apache.flink.sql.parser.ddl.SqlDropTable;
import org.apache.flink.sql.parser.ddl.SqlTableColumn;
import org.apache.flink.sql.parser.ddl.SqlTableOption;
import org.apache.flink.sql.parser.ddl.SqlUseCatalog;
import org.apache.flink.sql.parser.ddl.SqlUseDatabase;
import org.apache.flink.sql.parser.dml.RichSqlInsert;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.calcite.FlinkPlannerImpl;
import org.apache.flink.table.calcite.FlinkTypeFactory;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.CatalogFunction;
import org.apache.flink.table.catalog.CatalogFunctionImpl;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.FunctionLanguage;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.UnresolvedIdentifier;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.operations.CatalogSinkModifyOperation;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.PlannerQueryOperation;
import org.apache.flink.table.operations.UseCatalogOperation;
import org.apache.flink.table.operations.UseDatabaseOperation;
import org.apache.flink.table.operations.ddl.AlterDatabaseOperation;
import org.apache.flink.table.operations.ddl.AlterFunctionOperation;
import org.apache.flink.table.operations.ddl.AlterTablePropertiesOperation;
import org.apache.flink.table.operations.ddl.AlterTableRenameOperation;
import org.apache.flink.table.operations.ddl.CreateDatabaseOperation;
import org.apache.flink.table.operations.ddl.CreateFunctionOperation;
import org.apache.flink.table.operations.ddl.CreateTableOperation;
import org.apache.flink.table.operations.ddl.DropDatabaseOperation;
import org.apache.flink.table.operations.ddl.DropFunctionOperation;
import org.apache.flink.table.operations.ddl.DropTableOperation;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.util.StringUtils;

import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Mix-in tool class for {@code SqlNode} that allows DDL commands to be
 * converted to {@link Operation}.
 *
 * <p>For every kind of {@link SqlNode}, there needs to have a corresponding
 * #convert(type) method, the 'type' argument should be the subclass
 * of the supported {@link SqlNode}.
 *
 * <p>Every #convert() should return a {@link Operation} which can be used in
 * {@link org.apache.flink.table.delegation.Planner}.
 */
public class SqlToOperationConverter {
	private FlinkPlannerImpl flinkPlanner;
	private CatalogManager catalogManager;

	//~ Constructors -----------------------------------------------------------

	private SqlToOperationConverter(
			FlinkPlannerImpl flinkPlanner,
			CatalogManager catalogManager) {
		this.flinkPlanner = flinkPlanner;
		this.catalogManager = catalogManager;
	}

	/**
	 * This is the main entrance for executing all kinds of DDL/DML {@code SqlNode}s, different
	 * SqlNode will have it's implementation in the #convert(type) method whose 'type' argument
	 * is subclass of {@code SqlNode}.
	 *
	 * @param flinkPlanner     FlinkPlannerImpl to convert sql node to rel node
	 * @param sqlNode          SqlNode to execute on
	 */
	public static Optional<Operation> convert(
			FlinkPlannerImpl flinkPlanner,
			CatalogManager catalogManager,
			SqlNode sqlNode) {
		// validate the query
		final SqlNode validated = flinkPlanner.validate(sqlNode);
		SqlToOperationConverter converter = new SqlToOperationConverter(flinkPlanner, catalogManager);
		if (validated instanceof SqlCreateTable) {
			return Optional.of(converter.convertCreateTable((SqlCreateTable) validated));
		} else if (validated instanceof SqlDropTable) {
			return Optional.of(converter.convertDropTable((SqlDropTable) validated));
		} else if (validated instanceof SqlAlterTable) {
			return Optional.of(converter.convertAlterTable((SqlAlterTable) validated));
		} else if (validated instanceof SqlCreateFunction) {
			return Optional.of(converter.convertCreateFunction((SqlCreateFunction) validated));
		} else if (validated instanceof SqlAlterFunction) {
			return Optional.of(converter.convertAlterFunction((SqlAlterFunction) validated));
		} else if (validated instanceof SqlDropFunction) {
			return Optional.of(converter.convertDropFunction((SqlDropFunction) validated));
		} else if (validated instanceof RichSqlInsert) {
			SqlNodeList targetColumnList = ((RichSqlInsert) validated).getTargetColumnList();
			if (targetColumnList != null && targetColumnList.size() != 0) {
				throw new ValidationException("Partial inserts are not supported");
			}
			return Optional.of(converter.convertSqlInsert((RichSqlInsert) validated));
		} else if (validated instanceof SqlUseCatalog) {
			return Optional.of(converter.convertUseCatalog((SqlUseCatalog) validated));
		} else if (validated instanceof SqlUseDatabase) {
			return Optional.of(converter.convertUseDatabase((SqlUseDatabase) validated));
		} else if (validated instanceof SqlCreateDatabase) {
			return Optional.of(converter.convertCreateDatabase((SqlCreateDatabase) validated));
		} else if (validated instanceof SqlDropDatabase) {
			return Optional.of(converter.convertDropDatabase((SqlDropDatabase) validated));
		} else if (validated instanceof SqlAlterDatabase) {
			return Optional.of(converter.convertAlterDatabase((SqlAlterDatabase) validated));
		} else if (validated.getKind().belongsTo(SqlKind.QUERY)) {
			return Optional.of(converter.convertSqlQuery(validated));
		} else {
			return Optional.empty();
		}
	}

	//~ Tools ------------------------------------------------------------------

	/**
	 * Convert the {@link SqlCreateTable} node.
	 */
	private Operation convertCreateTable(SqlCreateTable sqlCreateTable) {
		// primary key and unique keys are not supported
		if ((sqlCreateTable.getPrimaryKeyList().size() > 0)
			|| (sqlCreateTable.getUniqueKeysList().size() > 0)) {
			throw new SqlConversionException("Primary key and unique key are not supported yet.");
		}

		if (sqlCreateTable.getWatermark().isPresent()) {
			throw new SqlConversionException(
				"Watermark statement is not supported in Old Planner, please use Blink Planner instead.");
		}

		// set with properties
		Map<String, String> properties = new HashMap<>();
		sqlCreateTable.getPropertyList().getList().forEach(p ->
			properties.put(((SqlTableOption) p).getKeyString().toLowerCase(),
				((SqlTableOption) p).getValueString()));

		TableSchema tableSchema = createTableSchema(sqlCreateTable);
		String tableComment = sqlCreateTable.getComment().map(comment ->
			comment.getNlsString().getValue()).orElse(null);
		// set partition key
		List<String> partitionKeys = sqlCreateTable.getPartitionKeyList()
			.getList()
			.stream()
			.map(p -> ((SqlIdentifier) p).getSimple())
			.collect(Collectors.toList());

		CatalogTable catalogTable = new CatalogTableImpl(tableSchema,
			partitionKeys,
			properties,
			tableComment);

		UnresolvedIdentifier unresolvedIdentifier = UnresolvedIdentifier.of(sqlCreateTable.fullTableName());
		ObjectIdentifier identifier = catalogManager.qualifyIdentifier(unresolvedIdentifier);

		return new CreateTableOperation(
			identifier,
			catalogTable,
			sqlCreateTable.isIfNotExists());
	}

	/** Convert DROP TABLE statement. */
	private Operation convertDropTable(SqlDropTable sqlDropTable) {
		UnresolvedIdentifier unresolvedIdentifier = UnresolvedIdentifier.of(sqlDropTable.fullTableName());
		ObjectIdentifier identifier = catalogManager.qualifyIdentifier(unresolvedIdentifier);

		return new DropTableOperation(identifier, sqlDropTable.getIfExists());
	}

	/** convert ALTER TABLE statement. */
	private Operation convertAlterTable(SqlAlterTable sqlAlterTable) {
		UnresolvedIdentifier unresolvedIdentifier = UnresolvedIdentifier.of(sqlAlterTable.fullTableName());
		ObjectIdentifier tableIdentifier = catalogManager.qualifyIdentifier(unresolvedIdentifier);
		if (sqlAlterTable instanceof SqlAlterTableRename) {
			UnresolvedIdentifier newUnresolvedIdentifier =
				UnresolvedIdentifier.of(((SqlAlterTableRename) sqlAlterTable).fullNewTableName());
			ObjectIdentifier newTableIdentifier = catalogManager.qualifyIdentifier(newUnresolvedIdentifier);
			return new AlterTableRenameOperation(tableIdentifier, newTableIdentifier);
		} else if (sqlAlterTable instanceof SqlAlterTableProperties) {
			Optional<CatalogManager.TableLookupResult> optionalCatalogTable = catalogManager.getTable(tableIdentifier);
			if (optionalCatalogTable.isPresent() && !optionalCatalogTable.get().isTemporary()) {
				CatalogTable originalCatalogTable = (CatalogTable) optionalCatalogTable.get().getTable();
				Map<String, String> properties = new HashMap<>();
				properties.putAll(originalCatalogTable.getProperties());
				((SqlAlterTableProperties) sqlAlterTable).getPropertyList().getList().forEach(p ->
					properties.put(((SqlTableOption) p).getKeyString().toLowerCase(),
						((SqlTableOption) p).getValueString()));
				CatalogTable catalogTable = new CatalogTableImpl(
					originalCatalogTable.getSchema(),
					originalCatalogTable.getPartitionKeys(),
					properties,
					originalCatalogTable.getComment());
				return new AlterTablePropertiesOperation(tableIdentifier, catalogTable);
			} else {
				throw new ValidationException(String.format("Table %s doesn't exist or is a temporary table.",
					tableIdentifier.toString()));
			}
		}
		return null;
	}

	/** Convert CREATE FUNCTION statement. */
	private Operation convertCreateFunction(SqlCreateFunction sqlCreateFunction) {
		FunctionLanguage language = parseLanguage(sqlCreateFunction.getFunctionLanguage());
		CatalogFunction catalogFunction = new CatalogFunctionImpl(
			sqlCreateFunction.getFunctionClassName().getValueAs(String.class), language);

		UnresolvedIdentifier unresolvedIdentifier = UnresolvedIdentifier.of(sqlCreateFunction.getFunctionIdentifier());
		ObjectIdentifier identifier = catalogManager.qualifyIdentifier(unresolvedIdentifier);

		return new CreateFunctionOperation(
			identifier,
			catalogFunction,
			sqlCreateFunction.isIfNotExists()
		);
	}

	/** Convert ALTER FUNCTION statement. */
	private Operation convertAlterFunction(SqlAlterFunction sqlAlterFunction) {
		FunctionLanguage language = parseLanguage(sqlAlterFunction.getFunctionLanguage());
		CatalogFunction catalogFunction = new CatalogFunctionImpl(
			sqlAlterFunction.getFunctionClassName().getValueAs(String.class), language);

		UnresolvedIdentifier unresolvedIdentifier = UnresolvedIdentifier.of(sqlAlterFunction.getFunctionIdentifier());
		ObjectIdentifier identifier = catalogManager.qualifyIdentifier(unresolvedIdentifier);
		return new AlterFunctionOperation(
			identifier,
			catalogFunction,
			sqlAlterFunction.isIfExists()
		);
	}

	/** Convert DROP FUNCTION statement. */
	private Operation convertDropFunction(SqlDropFunction sqlDropFunction) {
		UnresolvedIdentifier unresolvedIdentifier = UnresolvedIdentifier.of(sqlDropFunction.getFunctionIdentifier());
		ObjectIdentifier identifier = catalogManager.qualifyIdentifier(unresolvedIdentifier);

		return new DropFunctionOperation(
			identifier,
			sqlDropFunction.getIfExists());
	}

	/** Fallback method for sql query. */
	private Operation convertSqlQuery(SqlNode node) {
		return toQueryOperation(flinkPlanner, node);
	}

	/** Convert insert into statement. */
	private Operation convertSqlInsert(RichSqlInsert insert) {
		// get name of sink table
		List<String> targetTablePath = ((SqlIdentifier) insert.getTargetTable()).names;

		UnresolvedIdentifier unresolvedIdentifier = UnresolvedIdentifier.of(targetTablePath.toArray(new String[0]));
		ObjectIdentifier identifier = catalogManager.qualifyIdentifier(unresolvedIdentifier);

		PlannerQueryOperation query = (PlannerQueryOperation) SqlToOperationConverter.convert(
			flinkPlanner,
			catalogManager,
			insert.getSource())
			.orElseThrow(() -> new TableException(
				"Unsupported node type " + insert.getSource().getClass().getSimpleName()));

		return new CatalogSinkModifyOperation(
			identifier,
			query,
			insert.getStaticPartitionKVs(),
			insert.isOverwrite());
	}

	/** Convert use catalog statement. */
	private Operation convertUseCatalog(SqlUseCatalog useCatalog) {
		return new UseCatalogOperation(useCatalog.getCatalogName());
	}

	/** Convert use database statement. */
	private Operation convertUseDatabase(SqlUseDatabase useDatabase) {
		String[] fullDatabaseName = useDatabase.fullDatabaseName();
		if (fullDatabaseName.length > 2) {
			throw new SqlConversionException("use database identifier format error");
		}
		String catalogName = fullDatabaseName.length == 2 ? fullDatabaseName[0] : catalogManager.getCurrentCatalog();
		String databaseName = fullDatabaseName.length == 2 ? fullDatabaseName[1] : fullDatabaseName[0];
		return new UseDatabaseOperation(catalogName, databaseName);
	}

	/** Convert CREATE DATABASE statement. */
	private Operation convertCreateDatabase(SqlCreateDatabase sqlCreateDatabase) {
		String[] fullDatabaseName = sqlCreateDatabase.fullDatabaseName();
		if (fullDatabaseName.length > 2) {
			throw new SqlConversionException("create database identifier format error");
		}
		String catalogName = (fullDatabaseName.length == 1) ? catalogManager.getCurrentCatalog() : fullDatabaseName[0];
		String databaseName = (fullDatabaseName.length == 1) ? fullDatabaseName[0] : fullDatabaseName[1];
		boolean ignoreIfExists = sqlCreateDatabase.isIfNotExists();
		String databaseComment = sqlCreateDatabase.getComment()
			.map(comment -> comment.getNlsString().getValue()).orElse(null);
		// set with properties
		Map<String, String> properties = new HashMap<>();
		sqlCreateDatabase.getPropertyList().getList().forEach(p ->
			properties.put(((SqlTableOption) p).getKeyString().toLowerCase(),
				((SqlTableOption) p).getValueString()));
		CatalogDatabase catalogDatabase = new CatalogDatabaseImpl(properties, databaseComment);
		return new CreateDatabaseOperation(catalogName, databaseName, catalogDatabase, ignoreIfExists);
	}

	/** Convert DROP DATABASE statement. */
	private Operation convertDropDatabase(SqlDropDatabase sqlDropDatabase) {
		String[] fullDatabaseName = sqlDropDatabase.fullDatabaseName();
		if (fullDatabaseName.length > 2) {
			throw new SqlConversionException("drop database identifier format error");
		}
		String catalogName = (fullDatabaseName.length == 1) ? catalogManager.getCurrentCatalog() : fullDatabaseName[0];
		String databaseName = (fullDatabaseName.length == 1) ? fullDatabaseName[0] : fullDatabaseName[1];
		return new DropDatabaseOperation(
			catalogName,
			databaseName,
			sqlDropDatabase.getIfExists(),
			sqlDropDatabase.isCascade());
	}

	/** Convert ALTER DATABASE statement. */
	private Operation convertAlterDatabase(SqlAlterDatabase sqlAlterDatabase) {
		String[] fullDatabaseName = sqlAlterDatabase.fullDatabaseName();
		if (fullDatabaseName.length > 2) {
			throw new SqlConversionException("alter database identifier format error");
		}
		String catalogName = (fullDatabaseName.length == 1) ? catalogManager.getCurrentCatalog() : fullDatabaseName[0];
		String databaseName = (fullDatabaseName.length == 1) ? fullDatabaseName[0] : fullDatabaseName[1];
		Map<String, String> properties = new HashMap<>();
		CatalogDatabase originCatalogDatabase;
		Optional<Catalog> catalog = catalogManager.getCatalog(catalogName);
		if (catalog.isPresent()) {
			try {
				originCatalogDatabase = catalog.get().getDatabase(databaseName);
				properties.putAll(originCatalogDatabase.getProperties());
			} catch (DatabaseNotExistException e) {
				throw new SqlConversionException(String.format("Database %s not exists", databaseName), e);
			}
		} else {
			throw new SqlConversionException(String.format("Catalog %s not exists", catalogName));
		}
		// set with properties
		sqlAlterDatabase.getPropertyList().getList().forEach(p ->
			properties.put(((SqlTableOption) p).getKeyString().toLowerCase(),
				((SqlTableOption) p).getValueString()));
		CatalogDatabase catalogDatabase = new CatalogDatabaseImpl(properties, originCatalogDatabase.getComment());
		return new AlterDatabaseOperation(catalogName, databaseName, catalogDatabase);
	}

	/**
	 * Create a table schema from {@link SqlCreateTable}. This schema contains computed column
	 * fields, say, we have a create table DDL statement:
	 * <blockquote><pre>
	 *   create table t(
	 *     a int,
	 *     b varchar,
	 *     c as to_timestamp(b))
	 *   with (
	 *     'connector' = 'csv',
	 *     'k1' = 'v1')
	 * </pre></blockquote>
	 *
	 * <p>The returned table schema contains columns (a:int, b:varchar, c:timestamp).
	 *
	 * @param sqlCreateTable sql create table node.
	 * @return TableSchema
	 */
	private TableSchema createTableSchema(SqlCreateTable sqlCreateTable) {
		// setup table columns
		SqlNodeList columnList = sqlCreateTable.getColumnList();
		TableSchema physicalSchema = null;
		TableSchema.Builder builder = new TableSchema.Builder();
		// collect the physical table schema first.
		final List<SqlNode> physicalColumns = columnList.getList().stream()
			.filter(n -> n instanceof SqlTableColumn).collect(Collectors.toList());
		for (SqlNode node : physicalColumns) {
			SqlTableColumn column = (SqlTableColumn) node;
			final RelDataType relType = column.getType()
				.deriveType(
					flinkPlanner.getOrCreateSqlValidator(),
					column.getType().getNullable());
			builder.field(column.getName().getSimple(),
				TypeConversions.fromLegacyInfoToDataType(FlinkTypeFactory.toTypeInfo(relType)));
			physicalSchema = builder.build();
		}
		assert physicalSchema != null;
		if (sqlCreateTable.containsComputedColumn()) {
			throw new SqlConversionException("Computed columns for DDL is not supported yet!");
		}
		return physicalSchema;
	}

	/**
	 * Converts language string to the FunctionLanguage.
	 *
	 * @param languageString  the language string from SQL parser
	 * @return supported FunctionLanguage otherwise raise UnsupportedOperationException.
	 * @throws UnsupportedOperationException if the languageString is not parsable or language is not supported
	 */
	private FunctionLanguage parseLanguage(String languageString) {
		if (StringUtils.isNullOrWhitespaceOnly(languageString)) {
			return FunctionLanguage.JAVA;
		}

		FunctionLanguage language;
		try {
			language = FunctionLanguage.valueOf(languageString);
		} catch (IllegalArgumentException e) {
			throw new UnsupportedOperationException(
				String.format("Unrecognized function language string %s", languageString), e);
		}

		if (language.equals(FunctionLanguage.PYTHON)) {
			throw new UnsupportedOperationException("Only function language JAVA and SCALA are supported for now.");
		}

		return language;
	}

	private PlannerQueryOperation toQueryOperation(FlinkPlannerImpl planner, SqlNode validated) {
		// transform to a relational tree
		RelRoot relational = planner.rel(validated);
		return new PlannerQueryOperation(relational.rel);
	}
}
