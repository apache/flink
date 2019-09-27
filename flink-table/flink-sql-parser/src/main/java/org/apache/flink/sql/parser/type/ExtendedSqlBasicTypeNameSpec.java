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

package org.apache.flink.sql.parser.type;

import org.apache.calcite.sql.SqlBasicTypeNameSpec;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * A sql type name specification of extended basic data type, it has a counterpart basic
 * sql type name but always represents as a special alias in Flink.
 *
 * <p>For example, STRING is synonym of VARCHAR(INT_MAX)
 * and BYTES is synonym of VARBINARY(INT_MAX).
 */
public class ExtendedSqlBasicTypeNameSpec extends SqlBasicTypeNameSpec {
	// Type alias used for unparsing.
	private final String typeAlias;

	/**
	 * Creates a {@code ExtendedSqlBuiltinTypeNameSpec} instance.
	 *
	 * @param typeName  type name
	 * @param precision type precision
	 * @param pos       parser position
	 */
	public ExtendedSqlBasicTypeNameSpec(
			String typeAlias,
			SqlTypeName typeName,
			int precision,
			SqlParserPos pos) {
		super(typeName, precision, pos);
		this.typeAlias = typeAlias;
	}

	@Override
	public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
		writer.keyword(typeAlias);
	}
}
