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
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeTransforms;
import org.apache.calcite.sql.validate.SqlMonotonicity;

/**
 * Function that materializes a processing time attribute.
 * After materialization the result can be used in regular arithmetical calculations.
 */
public class ProctimeMaterializeSqlFunction extends SqlFunction {

	public ProctimeMaterializeSqlFunction() {
		super(
			"PROCTIME_MATERIALIZE",
			SqlKind.OTHER_FUNCTION,
			ReturnTypes.cascade(
					ReturnTypes.explicit(SqlTypeName.TIMESTAMP, 3),
					SqlTypeTransforms.TO_NULLABLE),
			InferTypes.RETURN_TYPE,
			OperandTypes.family(SqlTypeFamily.TIMESTAMP),
			SqlFunctionCategory.SYSTEM);
	}

	@Override
	public SqlSyntax getSyntax() {
		return SqlSyntax.FUNCTION;
	}

	@Override
	public SqlMonotonicity getMonotonicity(SqlOperatorBinding call) {
		return SqlMonotonicity.INCREASING;
	}

	@Override
	public boolean isDeterministic() {
		return false;
	}
}
