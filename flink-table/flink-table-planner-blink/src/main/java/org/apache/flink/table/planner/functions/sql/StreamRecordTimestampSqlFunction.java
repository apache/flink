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

package org.apache.flink.table.planner.functions.sql;

import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * Function to access the timestamp of a StreamRecord.
 */
public class StreamRecordTimestampSqlFunction extends SqlFunction {

	public StreamRecordTimestampSqlFunction() {
		super(
			"STREAMRECORD_TIMESTAMP",
			SqlKind.OTHER_FUNCTION,
			ReturnTypes.explicit(SqlTypeName.BIGINT),
			InferTypes.RETURN_TYPE,
			OperandTypes.family(SqlTypeFamily.NUMERIC),
			SqlFunctionCategory.SYSTEM);
	}

	@Override
	public SqlSyntax getSyntax() {
		return SqlSyntax.FUNCTION;
	}

	@Override
	public boolean isDeterministic() {
		return true;
	}
}
