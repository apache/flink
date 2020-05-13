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

package org.apache.flink.sql.parser.ddl.constraint;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Optional;

/**
 * Table constraint of a table definition.
 *
 * <p>Syntax from
 * SQL-2011 IWD 9075-2:201?(E) 11.3 &lt;table definition&gt;:
 *
 * <pre>
 * &lt;table constraint definition&gt; ::=
 *   [ &lt;constraint name definition&gt; ] &lt;table constraint&gt;
 *       [ &lt;constraint characteristics&gt; ]
 *
 * &lt;table constraint&gt; ::=
 *     &lt;unique constraint definition&gt;
 *
 * &lt;unique constraint definition&gt; ::=
 *     &lt;unique specification&gt; &lt;left paren&gt; &lt;unique column list&gt; &lt;right paren&gt;
 *
 * &lt;unique specification&gt; ::=
 *     UNIQUE
 *   | PRIMARY KEY
 * </pre>
 */
public class SqlTableConstraint extends SqlCall {
	/** Use this operator only if you don't have a better one. */
	private static final SqlOperator OPERATOR =
			new SqlSpecialOperator("SqlTableConstraint", SqlKind.OTHER);

	private final SqlIdentifier constraintName;
	private final SqlLiteral uniqueSpec;
	private final SqlNodeList columns;
	private final SqlLiteral enforcement;
	// Whether this is a table constraint, currently it is only used for SQL unparse.
	private final boolean isTableConstraint;

	/**
	 * Creates a table constraint node.
	 *
	 * @param constraintName Constraint name
	 * @param uniqueSpec     Unique specification
	 * @param columns        Column list on which the constraint enforces
	 *                       or null if this is a column constraint
	 * @param enforcement    Whether the constraint is enforced
	 * @param isTableConstraint Whether this is a table constraint
	 * @param pos            Parser position
	 */
	public SqlTableConstraint(
			@Nullable SqlIdentifier constraintName,
			SqlLiteral uniqueSpec,
			SqlNodeList columns,
			@Nullable SqlLiteral enforcement,
			boolean isTableConstraint,
			SqlParserPos pos) {
		super(pos);
		this.constraintName = constraintName;
		this.uniqueSpec = uniqueSpec;
		this.columns = columns;
		this.enforcement = enforcement;
		this.isTableConstraint = isTableConstraint;
	}

	@Override
	public SqlOperator getOperator() {
		return OPERATOR;
	}

	/** Returns whether the constraint is UNIQUE. */
	public boolean isUnique() {
		return this.uniqueSpec.getValueAs(SqlUniqueSpec.class) == SqlUniqueSpec.UNIQUE;
	}

	/** Returns whether the constraint is PRIMARY KEY. */
	public boolean isPrimaryKey() {
		return this.uniqueSpec.getValueAs(SqlUniqueSpec.class) == SqlUniqueSpec.PRIMARY_KEY;
	}

	/** Returns whether the constraint is enforced. */
	public boolean isEnforced() {
		// Default is enforced.
		return this.enforcement == null
				|| this.enforcement.getValueAs(SqlConstraintEnforcement.class)
					== SqlConstraintEnforcement.ENFORCED;
	}

	public Optional<String> getConstraintName() {
		String ret = constraintName != null ? constraintName.getSimple() : null;
		return Optional.ofNullable(ret);
	}

	public Optional<SqlIdentifier> getConstraintNameIdentifier() {
		return Optional.ofNullable(constraintName);
	}

	public SqlNodeList getColumns() {
		return columns;
	}

	public boolean isTableConstraint() {
		return isTableConstraint;
	}

	/**
	 * Returns the columns as a string array.
	 */
	public String[] getColumnNames() {
		return columns.getList()
				.stream()
				.map(col -> ((SqlIdentifier) col)
						.getSimple())
				.toArray(String[]::new);
	}

	@Override
	public List<SqlNode> getOperandList() {
		return ImmutableNullableList.of(constraintName, uniqueSpec, columns, enforcement);
	}

	@Override
	public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
		if (this.constraintName != null) {
			writer.keyword("CONSTRAINT");
			this.constraintName.unparse(writer, leftPrec, rightPrec);
		}
		this.uniqueSpec.unparse(writer, leftPrec, rightPrec);
		if (isTableConstraint) {
			SqlWriter.Frame frame = writer.startList("(", ")");
			for (SqlNode column : this.columns) {
				writer.sep(",", false);
				column.unparse(writer, leftPrec, rightPrec);
			}
			writer.endList(frame);
		}
		if (this.enforcement != null) {
			this.enforcement.unparse(writer, leftPrec, rightPrec);
		}
	}
}
