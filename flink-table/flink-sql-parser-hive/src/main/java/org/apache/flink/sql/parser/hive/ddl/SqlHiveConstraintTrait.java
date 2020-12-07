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

package org.apache.flink.sql.parser.hive.ddl;

import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlWriter;

/**
 * To describe a Hive constraint, i.e. ENABLE/DISABLE, VALIDATE/NOVALIDATE, RELY/NORELY.
 */
public class SqlHiveConstraintTrait {

	private final SqlLiteral enable;
	private final SqlLiteral validate;
	private final SqlLiteral rely;

	public SqlHiveConstraintTrait(
			SqlLiteral enable,
			SqlLiteral validate,
			SqlLiteral rely) {
		this.enable = enable;
		this.validate = validate;
		this.rely = rely;
	}

	public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
		enable.unparse(writer, leftPrec, rightPrec);
		validate.unparse(writer, leftPrec, rightPrec);
		rely.unparse(writer, leftPrec, rightPrec);
	}

	public boolean isEnable() {
		return enable.getValueAs(SqlHiveConstraintEnable.class) == SqlHiveConstraintEnable.ENABLE;
	}

	public boolean isValidate() {
		return validate.getValueAs(SqlHiveConstraintValidate.class) == SqlHiveConstraintValidate.VALIDATE;
	}

	public boolean isRely() {
		return rely.getValueAs(SqlHiveConstraintRely.class) == SqlHiveConstraintRely.RELY;
	}
}
