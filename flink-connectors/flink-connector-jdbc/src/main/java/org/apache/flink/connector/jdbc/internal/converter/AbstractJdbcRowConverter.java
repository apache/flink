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

import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;

import java.sql.ResultSet;
import java.sql.SQLException;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Base for all row converters.
 */
public abstract class AbstractJdbcRowConverter implements JdbcRowConverter {

	protected final RowType rowType;
	protected final JdbcFieldConverter[] converters;

	public AbstractJdbcRowConverter(RowType rowType) {
		this.rowType = checkNotNull(rowType);
		converters = new JdbcFieldConverter[rowType.getFieldCount()];

		for (int i = 0; i < converters.length; i++) {
			converters[i] = createConverter(rowType.getTypeAt(i));
		}
	}

	@Override
	public Row convert(ResultSet resultSet, Row reuse) throws SQLException {
		for (int pos = 0; pos < rowType.getFieldCount(); pos++) {
			reuse.setField(pos, converters[pos].convert(resultSet.getObject(pos + 1)));
		}

		return reuse;
	}

	/**
	 * Create a runtime JDBC field converter from given {@link LogicalType}.
	 */
	public JdbcFieldConverter createConverter(LogicalType type) {
		LogicalTypeRoot root = type.getTypeRoot();

		if (root == LogicalTypeRoot.SMALLINT) {
			// Converter for small type that casts value to int and then return short value, since
	        // JDBC 1.0 use int type for small values.
			return v -> ((Integer) v).shortValue();
		} else {
			return v -> v;
		}
	}
}
