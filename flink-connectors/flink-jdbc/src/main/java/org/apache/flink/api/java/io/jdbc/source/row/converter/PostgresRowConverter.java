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

import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;

import org.postgresql.jdbc.PgArray;
import org.postgresql.util.PGobject;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Row converter for Postgres.
 */
public class PostgresRowConverter extends AbstractJDBCRowConverter {

	public PostgresRowConverter(RowType rowType) {
		super(rowType);
	}

	@Override
	public Row setRow(ResultSet resultSet, Row reuse) throws SQLException {
		for (int pos = 0; pos < rowType.getFieldCount(); pos++) {
			LogicalType logicalType = rowType.getTypeAt(pos);
			LogicalTypeRoot root = logicalType.getTypeRoot();
			Object v = resultSet.getObject(pos + 1);

			if (root == LogicalTypeRoot.SMALLINT) {
				reuse.setField(pos, ((Integer) v).shortValue());
			} else if (root == LogicalTypeRoot.ARRAY) {

				ArrayType arrayType = (ArrayType) logicalType;
				LogicalTypeRoot elemType = arrayType.getElementType().getTypeRoot();

				PgArray pgArray = (PgArray) v;

				if (elemType == LogicalTypeRoot.VARBINARY) {
					Object[] in = (Object[]) pgArray.getArray();

					Object[] out = new Object[in.length];
					for (int i = 0; i < in.length; i++) {
						out[i] = ((PGobject) in[i]).getValue().getBytes();
					}

					reuse.setField(pos, out);
				} else {
					reuse.setField(pos, pgArray.getArray());
				}
			} else {
				reuse.setField(pos, v);
			}
		}

		return reuse;
	}
}
