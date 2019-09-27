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

import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * Parse column of Map type.
 */
public class SqlMapType extends SqlIdentifier implements ExtendedSqlType {

	private final SqlDataTypeSpec keyType;
	private final SqlDataTypeSpec valType;

	public SqlMapType(SqlParserPos pos, SqlDataTypeSpec keyType, SqlDataTypeSpec valType) {
		super(SqlTypeName.MAP.getName(), pos);
		this.keyType = keyType;
		this.valType = valType;
	}

	public SqlDataTypeSpec getKeyType() {
		return keyType;
	}

	public SqlDataTypeSpec getValType() {
		return valType;
	}

	@Override
	public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
		writer.keyword("MAP");
		SqlWriter.Frame frame = writer.startList(SqlWriter.FrameTypeEnum.FUN_CALL, "<", ">");
		writer.sep(",");
		ExtendedSqlType.unparseType(keyType, writer, leftPrec, rightPrec);
		writer.sep(",");
		ExtendedSqlType.unparseType(valType, writer, leftPrec, rightPrec);
		writer.endList(frame);
	}
}
