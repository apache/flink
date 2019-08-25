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

package org.apache.flink.sql.parser.error;

import org.apache.calcite.sql.parser.SqlParserPos;

/**
 * SQL parse Exception. This is a simpler version
 * of Calcite {@link org.apache.calcite.sql.parser.SqlParseException}
 * which is used for SqlNode validation.
 */
public class SqlParseException extends Exception {

	private SqlParserPos errorPosition;

	private String message;

	public SqlParseException(SqlParserPos errorPosition, String message) {
		this.errorPosition = errorPosition;
		this.message = message;
	}

	public SqlParseException(SqlParserPos errorPosition, String message, Exception e) {
		super(e);
		this.errorPosition = errorPosition;
		this.message = message;
	}

	public String getMessage() {
		return message;
	}

	public void setMessage(String message) {
		this.message = message;
	}

	public SqlParserPos getErrorPosition() {
		return errorPosition;
	}

	public void setErrorPosition(SqlParserPos errorPosition) {
		this.errorPosition = errorPosition;
	}
}
