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

package org.apache.flink.sql.parser.type;

import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Pair;

import java.util.List;

/**
 * Parse column of Row type.
 */
public class SqlRowType extends SqlIdentifier implements ExtendedSqlType {

	private final List<SqlIdentifier> fieldNames;
	private final List<SqlDataTypeSpec> fieldTypes;
	private final List<SqlCharStringLiteral> comments;

	public SqlRowType(SqlParserPos pos,
			List<SqlIdentifier> fieldNames,
			List<SqlDataTypeSpec> fieldTypes,
			List<SqlCharStringLiteral> comments) {
		super(SqlTypeName.ROW.getName(), pos);
		this.fieldNames = fieldNames;
		this.fieldTypes = fieldTypes;
		this.comments = comments;
	}

	public List<SqlIdentifier> getFieldNames() {
		return fieldNames;
	}

	public List<SqlDataTypeSpec> getFieldTypes() {
		return fieldTypes;
	}

	public List<SqlCharStringLiteral> getComments() {
		return comments;
	}

	public int getArity() {
		return fieldNames.size();
	}

	public SqlIdentifier getFieldName(int i) {
		return fieldNames.get(i);
	}

	public SqlDataTypeSpec getFieldType(int i) {
		return fieldTypes.get(i);
	}

	@Override
	public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
		writer.print("ROW");
		if (getFieldNames().size() == 0) {
			writer.print("<>");
		} else {
			SqlWriter.Frame frame = writer.startList(SqlWriter.FrameTypeEnum.FUN_CALL, "<", ">");
			int i = 0;
			for (Pair<SqlIdentifier, SqlDataTypeSpec> p : Pair.zip(this.fieldNames, this.fieldTypes)) {
				writer.sep(",", false);
				p.left.unparse(writer, 0, 0);
				ExtendedSqlType.unparseType(p.right, writer, leftPrec, rightPrec);
				if (comments.get(i) != null) {
					comments.get(i).unparse(writer, leftPrec, rightPrec);
				}
				i += 1;
			}
			writer.endList(frame);
		}
	}
}
