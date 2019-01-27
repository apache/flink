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

package org.apache.flink.sql.parser.util;

/**
 * A wrapper of a sql statement and its line info.
 */
public class SqlInfo {

	private String sqlContent;

	private int line;

	private int firstLineIndex;

	public String getSqlContent() {
		return sqlContent;
	}

	public void setSqlContent(String sqlContent) {
		this.sqlContent = sqlContent;
	}

	public int getLine() {
		return line;
	}

	public void setLine(int line) {
		this.line = line;
	}

	public int getFirstLineIndex() {
		return firstLineIndex;
	}

	public void setFirstLineIndex(int firstLineIndex) {
		this.firstLineIndex = firstLineIndex;
	}

	@Override
	public String toString() {
		return "Sqlcontent => " + sqlContent + "\nSql start line num => "
			+ line + "\n First line index =>" + firstLineIndex;
	}
}
