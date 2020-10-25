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

import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.parser.SqlParserPos;

/** Enumeration of SQL constraint enforcement. */
public enum SqlConstraintEnforcement {
	ENFORCED("ENFORCED"),
	NOT_ENFORCED("NOT ENFORCED");

	private final String digest;

	SqlConstraintEnforcement(String digest) {
		this.digest = digest;
	}

	@Override
	public String toString() {
		return digest;
	}

	/**
	 * Creates a parse-tree node representing an occurrence of this keyword
	 * at a particular position in the parsed text.
	 */
	public SqlLiteral symbol(SqlParserPos pos) {
		return SqlLiteral.createSymbol(this, pos);
	}
}
