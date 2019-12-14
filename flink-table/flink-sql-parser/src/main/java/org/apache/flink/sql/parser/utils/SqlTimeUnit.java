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

package org.apache.flink.sql.parser.utils;

import org.apache.calcite.sql.SqlWriter;

/** SqlTimeUnit used for Flink DDL sql. **/
public enum SqlTimeUnit {
	DAY("DAY", 24 * 3600 * 1000),
	HOUR("HOUR", 3600 * 1000),
	MINUTE("MINUTE", 60 * 1000),
	SECOND("SECOND", 1000),
	MILLISECOND("MILLISECOND", 1);

	/** Unparsing keyword. */
	private String keyword;
	/** Times used to transform this time unit to millisecond. **/
	private long timeToMillisecond;

	SqlTimeUnit(String keyword, long timeToMillisecond) {
		this.keyword = keyword;
		this.timeToMillisecond = timeToMillisecond;
	}

	public long populateAsMillisecond(int timeInterval) {
		return timeToMillisecond * timeInterval;
	}

	public void unparse(SqlWriter writer) {
		writer.keyword(keyword);
	}

}
