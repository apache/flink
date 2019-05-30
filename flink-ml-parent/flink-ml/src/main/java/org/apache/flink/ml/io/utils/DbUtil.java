/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.ml.io.utils;

import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.ml.io.BaseDB;
import org.apache.flink.ml.params.Params;
import org.apache.flink.ml.params.io.BaseSinkBatchParams;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;

/**
 * DataBase Util.
 */
public class DbUtil {

	public static void prepareSink(BaseDB db, String tableName, Table in, Params params) {

		boolean isOverwriteSink = params.get(BaseSinkBatchParams.OVERWRITE_SINK);

		//Create Table
		try {
			if (isOverwriteSink) {
				if (db.hasTable(tableName)) {
					db.dropTable(tableName);
				}
			}
			db.createTable(tableName, in.getSchema(), params);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Rearrange columns order of dataset to conform to sinking table in DB.
	 *
	 * @param input
	 * @return
	 */
	public static Table rearrangeColumns(Table input, TableSchema tableSchemaInDB) {
		if (input.getSchema().getFieldNames().length != tableSchemaInDB.getFieldNames().length) {
			throw new RuntimeException("mismatched column size.");
		}
		String[] colNamesInDataset = input.getSchema().getFieldNames();
		String[] colNamesInDB = tableSchemaInDB.getFieldNames();
		System.out.println(Tuple1.of(colNamesInDataset));
		System.out.println(Tuple1.of(colNamesInDB));

		boolean isSame = true;
		for (int i = 0; i < colNamesInDataset.length; i++) {
			if (colNamesInDataset[i].compareToIgnoreCase(colNamesInDB[i]) != 0) {
				isSame = false;
				break;
			}
		}
		if (isSame) {
			return input;
		}

		StringBuilder sbd = new StringBuilder();
		for (int i = 0; i < colNamesInDB.length; i++) {
			if (i > 0) {
				sbd.append(",");
			}
			sbd.append(colNamesInDB[i]);
		}
		return input.select(sbd.toString());
	}
}
