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

import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;

import org.postgresql.jdbc.PgArray;
import org.postgresql.util.PGobject;

/**
 * Row converter for Postgres.
 */
public class PostgresRowConverter extends AbstractJdbcRowConverter {

	public PostgresRowConverter(RowType rowType) {
		super(rowType);
	}

	@Override
	public JdbcFieldConverter createConverter(LogicalType type) {
		LogicalTypeRoot root = type.getTypeRoot();

		if (root == LogicalTypeRoot.ARRAY) {
			ArrayType arrayType = (ArrayType) type;

			// PG's bytea[] is wrapped in PGobject, rather than primitive byte arrays
			if (LogicalTypeChecks.hasFamily(arrayType.getElementType(), LogicalTypeFamily.BINARY_STRING)) {

				return v -> {
					PgArray pgArray = (PgArray) v;
					Object[] in = (Object[]) pgArray.getArray();

					Object[] out = new Object[in.length];
					for (int i = 0; i < in.length; i++) {
						out[i] = ((PGobject) in[i]).getValue().getBytes();
					}

					return out;
				};
			} else {
				return v -> ((PgArray) v).getArray();
			}
		} else {
			return createPrimitiveConverter(type);
		}
	}

	// Have its own method so that Postgres can support primitives that super class doesn't support in the future
	private JdbcFieldConverter createPrimitiveConverter(LogicalType type) {
		return super.createConverter(type);
	}

}
