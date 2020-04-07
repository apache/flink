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

package org.apache.flink.api.java.io.jdbc.source.row.converter;

import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;

import java.sql.ResultSet;
import java.sql.SQLException;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Base for all row converters.
 */
public abstract class AbstractJDBCRowConverter implements JDBCRowConverter {

	protected final RowType rowType;

	public AbstractJDBCRowConverter(RowType rowType) {
		this.rowType = checkNotNull(rowType);
	}

	@Override
	public Row convert(ResultSet resultSet, Row reuse) throws SQLException {
		for (int pos = 0; pos < rowType.getFieldCount(); pos++) {
			Object v = resultSet.getObject(pos + 1);
			reuse.setField(pos, v);
		}

		return reuse;
	}
}
