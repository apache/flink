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

package org.apache.flink.connector.jdbc.internal.converter;

import org.apache.flink.table.data.RowData;

import java.io.Serializable;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Converter that is responsible to convert between JDBC object and Flink SQL internal data structure {@link RowData}.
 */
public interface JdbcRowConverter extends Serializable {

	/**
	 * Convert data retrieved from {@link ResultSet} to internal {@link RowData}.
	 *
	 * @param resultSet ResultSet from JDBC
	 */
	RowData toInternal(ResultSet resultSet) throws SQLException;

	/**
	 * Convert data retrieved from Flink internal RowData to JDBC Object.
	 *
	 * @param rowData The given internal {@link RowData}.
	 * @param statement The statement to be filled.
	 * @return The filled statement.
	 */
	PreparedStatement toExternal(RowData rowData, PreparedStatement statement) throws SQLException;
}
