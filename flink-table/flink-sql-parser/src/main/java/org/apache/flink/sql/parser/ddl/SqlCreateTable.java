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

package org.apache.flink.sql.parser.ddl;

import org.apache.flink.sql.parser.ExtendedSqlNode;
import org.apache.flink.sql.parser.ddl.constraint.SqlTableConstraint;
import org.apache.flink.sql.parser.error.SqlValidateException;
import org.apache.flink.sql.parser.type.ExtendedSqlRowTypeNameSpec;

import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlCreate;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlTypeNameSpec;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.dialect.AnsiSqlDialect;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.calcite.util.ImmutableNullableList;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

/**
 * CREATE TABLE DDL sql call.
 */
public class SqlCreateTable extends SqlCreate implements ExtendedSqlNode {

	public static final SqlSpecialOperator OPERATOR = new SqlSpecialOperator("CREATE TABLE", SqlKind.CREATE_TABLE);

	private final SqlIdentifier tableName;

	private final SqlNodeList columnList;

	private final SqlNodeList propertyList;

	private final List<SqlTableConstraint> tableConstraints;

	private final SqlNodeList partitionKeyList;

	private final SqlWatermark watermark;

	private final SqlCharStringLiteral comment;

	private final SqlTableLike tableLike;

	private final boolean isTemporary;

	private final Set<String> tableConstraintPrimaryKeys;

	public SqlCreateTable(
			SqlParserPos pos,
			SqlIdentifier tableName,
			SqlNodeList columnList,
			List<SqlTableConstraint> tableConstraints,
			SqlNodeList propertyList,
			SqlNodeList partitionKeyList,
			@Nullable SqlWatermark watermark,
			@Nullable SqlCharStringLiteral comment,
			@Nullable SqlTableLike tableLike,
			boolean isTemporary) {
		super(OPERATOR, pos, false, false);
		this.tableName = requireNonNull(tableName, "tableName should not be null");
		this.columnList = requireNonNull(columnList, "columnList should not be null");
		this.tableConstraints = requireNonNull(tableConstraints, "table constraints should not be null");
		this.propertyList = requireNonNull(propertyList, "propertyList should not be null");
		this.partitionKeyList = requireNonNull(partitionKeyList, "partitionKeyList should not be null");
		this.watermark = watermark;
		this.comment = comment;
		this.tableLike = tableLike;
		this.isTemporary = isTemporary;
		this.tableConstraintPrimaryKeys = getTableConstraintPrimaryKeys(tableConstraints);
	}

	@Override
	public SqlOperator getOperator() {
		return OPERATOR;
	}

	@Override
	public List<SqlNode> getOperandList() {
		return ImmutableNullableList.of(tableName,
				columnList,
				new SqlNodeList(tableConstraints, SqlParserPos.ZERO),
				propertyList,
				partitionKeyList,
				watermark,
				comment,
				tableLike);
	}

	public SqlIdentifier getTableName() {
		return tableName;
	}

	public SqlNodeList getColumnList() {
		return columnList;
	}

	public SqlNodeList getPropertyList() {
		return propertyList;
	}

	public SqlNodeList getPartitionKeyList() {
		return partitionKeyList;
	}

	public List<SqlTableConstraint> getTableConstraints() {
		return tableConstraints;
	}

	public Optional<SqlWatermark> getWatermark() {
		return Optional.ofNullable(watermark);
	}

	public Optional<SqlCharStringLiteral> getComment() {
		return Optional.ofNullable(comment);
	}

	public Optional<SqlTableLike> getTableLike() {
		return Optional.ofNullable(tableLike);
	}

	public boolean isIfNotExists() {
		return ifNotExists;
	}

	public boolean isTemporary() {
		return isTemporary;
	}

	@Override
	public void validate() throws SqlValidateException {
		ColumnValidator validator = new ColumnValidator();
		for (SqlNode column : columnList) {
			validator.addColumn(column);
		}

		// Validate table constraints.
		boolean pkDefined = false;
		Set<String> constraintNames = new HashSet<>();
		for (SqlTableConstraint constraint : getFullConstraints()) {
			Optional<String> constraintName = constraint.getConstraintName();
			// Validate constraint name should be unique.
			if (constraintName.isPresent() && !constraintNames.add(constraintName.get())) {
				throw new SqlValidateException(constraint.getParserPosition(),
						String.format("Duplicate definition for constraint [%s]",
								constraintName.get()));
			}
			// Validate primary key definition should be unique.
			if (constraint.isPrimaryKey()) {
				if (pkDefined) {
					throw new SqlValidateException(constraint.getParserPosition(),
							"Duplicate primary key definition");
				} else {
					pkDefined = true;
				}
			}
			// Validate the key field exists.
			if (constraint.isTableConstraint()) {
				for (SqlNode column : constraint.getColumns()) {
					String columnName = ((SqlIdentifier) column).getSimple();
					if (!validator.contains(columnName)) {
						String prefix = constraint.isPrimaryKey() ? "Primary" : "Unique";
						throw new SqlValidateException(
								constraint.getParserPosition(),
								String.format("%s key column [%s] not defined", prefix, columnName));
					}
				}
			}
		}

		for (SqlNode partitionKeyNode : this.partitionKeyList.getList()) {
			String partitionKey = ((SqlIdentifier) partitionKeyNode).getSimple();
			if (!validator.contains(partitionKey)) {
				throw new SqlValidateException(
					partitionKeyNode.getParserPosition(),
					"Partition column [" + partitionKey + "] not defined in columns, at "
						+ partitionKeyNode.getParserPosition());
			}
		}

		if (this.watermark != null) {
			// SqlIdentifier.toString() returns a qualified identifier string using "." separator
			String rowtimeField = watermark.getEventTimeColumnName().toString();
			if (!validator.contains(rowtimeField)) {
				throw new SqlValidateException(
					watermark.getEventTimeColumnName().getParserPosition(),
					"The rowtime attribute field \"" + rowtimeField + "\" is not defined in columns, at " +
						watermark.getEventTimeColumnName().getParserPosition());
			}
		}

		if (tableLike != null) {
			tableLike.validate();
		}
	}

	public boolean containsComputedColumn() {
		for (SqlNode column : columnList) {
			if (column instanceof SqlBasicCall) {
				return true;
			}
		}
		return false;
	}

	/** Returns the column constraints plus the table constraints. */
	public List<SqlTableConstraint> getFullConstraints() {
		List<SqlTableConstraint> ret = new ArrayList<>();
		this.columnList.forEach(column -> {
			if (column instanceof SqlTableColumn) {
				((SqlTableColumn) column).getConstraint()
						.map(ret::add);
			}
		});
		ret.addAll(this.tableConstraints);
		return ret;
	}

	/**
	 * Decides if the given column is nullable.
	 *
	 * <p>Collect primary key fields to fix nullability: primary key implies
	 * an implicit not null constraint.
	 */
	public boolean isColumnNullable(SqlTableColumn column) {
		boolean isPrimaryKey = column.getConstraint()
				.map(SqlTableConstraint::isPrimaryKey)
				.orElse(false);
		if (isPrimaryKey
				|| tableConstraintPrimaryKeys.contains(
						column.getName().getSimple().toUpperCase())) {
			return false;
		}
		return column.getType().getNullable();
	}

	/** Returns the primary key constraint columns of table constraint. */
	private Set<String> getTableConstraintPrimaryKeys(List<SqlTableConstraint> constraints) {
		return constraints.stream().filter(SqlTableConstraint::isPrimaryKey)
				.findFirst()
				.map(c -> c.getColumns()
						.getList()
						.stream()
						.map(col -> ((SqlIdentifier) col).getSimple().toUpperCase())
						.collect(Collectors.toSet()))
				.orElse(Collections.emptySet());
	}

	/**
	 * Returns the projection format of the DDL columns(including computed columns).
	 * i.e. the following DDL:
	 * <pre>
	 *   create table tbl1(
	 *     col1 int,
	 *     col2 varchar,
	 *     col3 as to_timestamp(col2)
	 *   ) with (
	 *     'connector' = 'csv'
	 *   )
	 * </pre>
	 * is equivalent with query:
	 *
	 * <p>"col1, col2, to_timestamp(col2) as col3", caution that the "computed column" operands
	 * have been reversed.
	 */
	public String getColumnSqlString() {
		SqlPrettyWriter writer = new SqlPrettyWriter(
			SqlPrettyWriter.config()
				.withDialect(AnsiSqlDialect.DEFAULT)
				.withAlwaysUseParentheses(true)
				.withSelectListItemsOnSeparateLines(false)
				.withIndentation(0));
		writer.startList("", "");
		for (SqlNode column : columnList) {
			writer.sep(",");
			if (column instanceof SqlTableColumn) {
				SqlTableColumn tableColumn = (SqlTableColumn) column;
				tableColumn.getName().unparse(writer, 0, 0);
			} else {
				column.unparse(writer, 0, 0);
			}
		}

		return writer.toString();
	}

	@Override
	public void unparse(
			SqlWriter writer,
			int leftPrec,
			int rightPrec) {
		writer.keyword("CREATE");
		if (isTemporary()) {
			writer.keyword("TEMPORARY");
		}
		writer.keyword("TABLE");
		tableName.unparse(writer, leftPrec, rightPrec);
		SqlWriter.Frame frame = writer.startList(SqlWriter.FrameTypeEnum.create("sds"), "(", ")");
		for (SqlNode column : columnList) {
			printIndent(writer);
			if (column instanceof SqlBasicCall) {
				SqlCall call = (SqlCall) column;
				SqlCall newCall = call.getOperator().createCall(
					SqlParserPos.ZERO,
					call.operand(1),
					call.operand(0));
				newCall.unparse(writer, leftPrec, rightPrec);
			} else {
				column.unparse(writer, leftPrec, rightPrec);
			}
		}
		if (tableConstraints.size() > 0) {
			for (SqlTableConstraint constraint : tableConstraints) {
				printIndent(writer);
				constraint.unparse(writer, leftPrec, rightPrec);
			}
		}
		if (watermark != null) {
			printIndent(writer);
			watermark.unparse(writer, leftPrec, rightPrec);
		}

		writer.newlineAndIndent();
		writer.endList(frame);

		if (comment != null) {
			writer.newlineAndIndent();
			writer.keyword("COMMENT");
			comment.unparse(writer, leftPrec, rightPrec);
		}

		if (this.partitionKeyList.size() > 0) {
			writer.newlineAndIndent();
			writer.keyword("PARTITIONED BY");
			SqlWriter.Frame partitionedByFrame = writer.startList("(", ")");
			this.partitionKeyList.unparse(writer, leftPrec, rightPrec);
			writer.endList(partitionedByFrame);
			writer.newlineAndIndent();
		}

		if (this.propertyList.size() > 0) {
			writer.keyword("WITH");
			SqlWriter.Frame withFrame = writer.startList("(", ")");
			for (SqlNode property : propertyList) {
				printIndent(writer);
				property.unparse(writer, leftPrec, rightPrec);
			}
			writer.newlineAndIndent();
			writer.endList(withFrame);
		}

		if (this.tableLike != null) {
			writer.newlineAndIndent();
			this.tableLike.unparse(writer, leftPrec, rightPrec);
		}
	}

	private void printIndent(SqlWriter writer) {
		writer.sep(",", false);
		writer.newlineAndIndent();
		writer.print("  ");
	}

	/**
	 * Table creation context.
	 */
	public static class TableCreationContext {
		public List<SqlNode> columnList = new ArrayList<>();
		public List<SqlTableConstraint> constraints = new ArrayList<>();
		@Nullable public SqlWatermark watermark;
	}

	public String[] fullTableName() {
		return tableName.names.toArray(new String[0]);
	}

	// -------------------------------------------------------------------------------------

	private static final class ColumnValidator {

		private final Set<String> allColumnNames = new HashSet<>();

		/**
		 * Adds column name to the registered column set. This will add nested column names recursive.
		 * Nested column names are qualified using "." separator.
		 */
		public void addColumn(SqlNode column) throws SqlValidateException {
			String columnName;
			if (column instanceof SqlTableColumn) {
				SqlTableColumn tableColumn = (SqlTableColumn) column;
				columnName = tableColumn.getName().getSimple();
				addNestedColumn(columnName, tableColumn.getType());
			} else if (column instanceof SqlBasicCall) {
				SqlBasicCall tableColumn = (SqlBasicCall) column;
				columnName = tableColumn.getOperands()[1].toString();
			} else {
				throw new UnsupportedOperationException("Unsupported column:" + column);
			}

			addColumnName(columnName, column.getParserPosition());
		}

		/**
		 * Returns true if the column name is existed in the registered column set.
		 * This supports qualified column name using "." separator.
		 */
		public boolean contains(String columnName) {
			return allColumnNames.contains(columnName);
		}

		private void addNestedColumn(String columnName, SqlDataTypeSpec columnType) throws SqlValidateException {
			SqlTypeNameSpec typeName = columnType.getTypeNameSpec();
			// validate composite type
			if (typeName instanceof ExtendedSqlRowTypeNameSpec) {
				ExtendedSqlRowTypeNameSpec rowType = (ExtendedSqlRowTypeNameSpec) typeName;
				for (int i = 0; i < rowType.getFieldNames().size(); i++) {
					SqlIdentifier fieldName = rowType.getFieldNames().get(i);
					String fullName = columnName + "." + fieldName;
					addColumnName(fullName, fieldName.getParserPosition());
					SqlDataTypeSpec fieldType = rowType.getFieldTypes().get(i);
					addNestedColumn(fullName, fieldType);
				}
			}
		}

		private void addColumnName(String columnName, SqlParserPos pos) throws SqlValidateException {
			if (!allColumnNames.add(columnName)) {
				throw new SqlValidateException(pos,
					"Duplicate column name [" + columnName + "], at " + pos);
			}
		}
	}
}
