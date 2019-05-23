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

package org.apache.flink.table.functions.sql;

import org.apache.flink.table.calcite.FlinkTypeFactory;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;

/**
 * Function used to access a proctime attribute.
 */
public class ProctimeSqlFunction extends SqlFunction {
	public ProctimeSqlFunction() {
		super(
				"PROCTIME",
				SqlKind.OTHER_FUNCTION,
				ReturnTypes.explicit(new ProctimeRelProtoDataType()),
				null,
				OperandTypes.NILADIC,
				SqlFunctionCategory.TIMEDATE);
	}

	private static class ProctimeRelProtoDataType implements RelProtoDataType {
		@Override
		public RelDataType apply(RelDataTypeFactory factory) {
			return ((FlinkTypeFactory) factory).createProctimeIndicatorType();
		}
	}
}
