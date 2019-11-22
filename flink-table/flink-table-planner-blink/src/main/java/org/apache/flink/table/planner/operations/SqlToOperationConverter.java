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

package org.apache.flink.table.planner.operations;

import org.apache.flink.sql.parser.ddl.SqlCreateTable;
import org.apache.flink.sql.parser.ddl.SqlDropTable;
import org.apache.flink.sql.parser.ddl.SqlTableColumn;
import org.apache.flink.sql.parser.ddl.SqlTableOption;
import org.apache.flink.sql.parser.ddl.SqlUseCatalog;
import org.apache.flink.sql.parser.dml.RichSqlInsert;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.UnresolvedIdentifier;
import org.apache.flink.table.operations.CatalogSinkModifyOperation;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.UseCatalogOperation;
import org.apache.flink.table.operations.ddl.CreateTableOperation;
import org.apache.flink.table.operations.ddl.DropTableOperation;
import org.apache.flink.table.planner.calcite.FlinkPlannerImpl;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.utils.TypeConversions;

import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.validate.SqlValidator;

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
	 * @param flinkPlanner FlinkPlannerImpl to convertCreateTable sql node to rel node
	 * @param catalogManager CatalogManager to resolve full path for operations
	 * @param sqlNode SqlNode to execute on
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
		} else if (validated instanceof RichSqlInsert) {
			return Optional.of(converter.convertSqlInsert((RichSqlInsert) validated));
		} else if (validated instanceof SqlUseCatalog) {
			return Optional.of(converter.convertUseCatalog((SqlUseCatalog) validated));
		} else if (validated.getKind().belongsTo(SqlKind.QUERY)) {
			return Optional.of(converter.convertSqlQuery(validated));
		} else {
			return Optional.empty();
		}
	}

	/**
	 * Convert the {@link SqlCreateTable} node.
	 */
	private Operation convertCreateTable(SqlCreateTable sqlCreateTable) {
		// primary key and unique keys are not supported
		if ((sqlCreateTable.getPrimaryKeyList().size() > 0)
			|| (sqlCreateTable.getUniqueKeysList().size() > 0)) {
			throw new SqlConversionException("Primary key and unique key are not supported yet.");
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

	/** Fallback method for sql query. */
	private Operation convertSqlQuery(SqlNode node) {
		return toQueryOperation(flinkPlanner, node);
	}

	//~ Tools ------------------------------------------------------------------

	/**
	 * Create a table schema from {@link SqlCreateTable}. This schema may contains computed column
	 * fields and watermark information, say, we have a create table DDL statement:
	 * <blockquote><pre>
	 *   CREATE TABLE myTable (
	 *     a INT,
	 *     b STRING,
	 *     c AS TO_TIMESTAMP(b),
	 *     WATERMARK FOR c AS c - INTERVAL '1' SECOND
	 *   ) WITH (
	 *     'connector.type' = 'csv',
	 *     ...
	 *   )
	 * </pre></blockquote>
	 *
	 * <p>The returned table schema contains columns (a:int, b:varchar, c:timestamp).
	 *
	 * @param sqlCreateTable sql create table node
	 * @return TableSchema
	 */
	private TableSchema createTableSchema(SqlCreateTable sqlCreateTable) {
		// Setup table columns.
		SqlNodeList columnList = sqlCreateTable.getColumnList();
		// Collect the physical fields info first.
		Map<String, RelDataType> nameToType = new HashMap<>();
		final SqlValidator validator = flinkPlanner.getOrCreateSqlValidator();
		for (SqlNode node : columnList.getList()) {
			if (node instanceof SqlTableColumn) {
				SqlTableColumn column = (SqlTableColumn) node;
				RelDataType relType = column.getType()
					.deriveType(validator, column.getType().getNullable());
				String name = column.getName().getSimple();
				nameToType.put(name, relType);
			}
		}
		final TableSchema.Builder builder = new TableSchema.Builder();
		// Build the table schema.
		for (SqlNode node : columnList) {
			if (node instanceof SqlTableColumn) {
				SqlTableColumn column = (SqlTableColumn) node;
				final String fieldName = column.getName().getSimple();
				assert nameToType.containsKey(fieldName);
				builder.field(fieldName,
					TypeConversions.fromLogicalToDataType(
						FlinkTypeFactory.toLogicalType(nameToType.get(fieldName))));
			} else if (node instanceof SqlBasicCall) {
				SqlBasicCall call = (SqlBasicCall) node;
				SqlNode validatedExpr = validator
					.validateParameterizedExpression(call.operand(0), nameToType);
				final RelDataType validatedType = validator.getValidatedNodeType(validatedExpr);
				builder.field(call.operand(1).toString(),
					TypeConversions.fromLogicalToDataType(
						FlinkTypeFactory.toLogicalType(validatedType)),
					validatedExpr.toString());
			} else {
				throw new TableException("Unexpected table column type!");
			}
		}

		// put watermark information into TableSchema
		sqlCreateTable.getWatermark().ifPresent(watermark -> {
			String rowtimeAttribute = watermark.getEventTimeColumnName().toString();
			SqlNode expression = watermark.getWatermarkStrategy();
			// this will validate and expand function identifiers.
			SqlNode validated = validator.validateParameterizedExpression(expression, nameToType);
			RelDataType validatedType = validator.getValidatedNodeType(validated);
			DataType exprDataType = TypeConversions.fromLogicalToDataType(
				FlinkTypeFactory.toLogicalType(validatedType));
			// use the qualified SQL expression string
			builder.watermark(rowtimeAttribute, validated.toString(), exprDataType);
		});

		return builder.build();
	}

	private PlannerQueryOperation toQueryOperation(FlinkPlannerImpl planner, SqlNode validated) {
		// transform to a relational tree
		RelRoot relational = planner.rel(validated);
		return new PlannerQueryOperation(relational.project());
	}
}
