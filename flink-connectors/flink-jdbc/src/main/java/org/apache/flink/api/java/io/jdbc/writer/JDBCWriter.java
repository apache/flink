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

package org.apache.flink.api.java.io.jdbc.writer;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.Row;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * JDBCWriter used to execute statements (e.g. INSERT, UPSERT, DELETE).
 */
public interface JDBCWriter extends Serializable {

	/**
	 * Open the writer by JDBC Connection. It can create Statement from Connection.
	 */
	void open(Connection connection) throws SQLException;

	/**
	 * Add record to writer, the writer may cache the data.
	 */
	void addRecord(Tuple2<Boolean, Row> record) throws SQLException;

	/**
	 * Submits a batch of commands to the database for execution.
	 */
	void executeBatch() throws SQLException;

	/**
	 * Close JDBC related statements and other classes.
	 */
	void close() throws SQLException;
}
