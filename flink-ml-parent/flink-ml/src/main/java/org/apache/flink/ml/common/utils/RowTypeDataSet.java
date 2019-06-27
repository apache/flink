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

package org.apache.flink.ml.common.utils;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.ml.common.MLSession;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

/**
 * Transforms for DataSet< Row > and Table.
 */
public class RowTypeDataSet {

	public static DataSet <Row> fromTable(Table table) {
		return MLSession.getBatchTableEnvironment().toDataSet(table, Row.class);
	}

	public static Table toTable(DataSet <Row> data, TableSchema schema) {
		return toTable(data, schema.getFieldNames(), schema.getFieldTypes());
	}

	public static Table toTable(DataSet <Row> data) {
		return toTable(data, new String[] {});
	}

	public static Table toTable(DataSet <Row> data, String[] colNames) {
		if (null == colNames || colNames.length == 0) {
			return MLSession.getBatchTableEnvironment().fromDataSet(data);
		} else {
			StringBuilder sbd = new StringBuilder();
			sbd.append(colNames[0]);
			for (int i = 1; i < colNames.length; i++) {
				sbd.append(",").append(colNames[i]);
			}
			return MLSession.getBatchTableEnvironment().fromDataSet(data, sbd.toString());
		}
	}

	public static Table toTable(DataSet <Row> data, String[] colNames, TypeInformation <?>[] colTypes) {
		try {
			return toTable(data, colNames);
		} catch (Exception ex) {
			if (null == colTypes) {
				throw ex;
			} else {
				DataSet <Row> t = getDataSetWithExplicitTypeDefine(data, colNames, colTypes);
				return toTable(t, colNames);
			}
		}
	}

	private static DataSet <Row> getDataSetWithExplicitTypeDefine(
		DataSet <Row> data, String[] colNames, TypeInformation <?>[] colTypes) {
		DataSet <Row> r = data
			.map(
				new MapFunction <Row, Row>() {
					@Override
					public Row map(Row t) throws Exception {
						return t;
					}
				}
			)
			.returns(new RowTypeInfo(colTypes, colNames));

		return r;
	}

}
