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

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.ml.common.MLSession;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

/**
 * Transforms for DataStream< Row > and Table.
 */
public class RowTypeDataStream {

	public static DataStream <Row> fromTable(Table table) {
		return MLSession.getStreamTableEnvironment().toAppendStream(table, Row.class);
	}

	public static DataStream <Row> toRetractStream(Table table) {
		return MLSession.getStreamTableEnvironment().toRetractStream(table, Row.class)
			.flatMap(new FlatMapFunction <Tuple2 <Boolean, Row>, Row>() {
				@Override
				public void flatMap(Tuple2 <Boolean, Row> tuple2, Collector <Row> collector) throws Exception {
					if (tuple2.f0) {
						collector.collect(tuple2.f1);
					}
				}
			});
	}

	public static Boolean isAppendStream(Table table) {
		try {
			MLSession.getStreamTableEnvironment().toAppendStream(table, Row.class);
			return true;
		} catch (org.apache.flink.table.api.TableException ex) {
			return false;
		}
	}

	public static Table toTable(DataStream <Row> data, TableSchema schema) {
		return toTable(data, schema.getFieldNames(), schema.getFieldTypes());
	}

	public static Table toTable(DataStream <Row> data) {
		return toTable(data, new String[] {});
	}

	public static Table toTable(DataStream <Row> data, String[] colNames) {
		if (null == colNames || colNames.length == 0) {
			return MLSession.getStreamTableEnvironment().fromDataStream(data);
		} else {
			StringBuilder sbd = new StringBuilder();
			sbd.append(colNames[0]);
			for (int i = 1; i < colNames.length; i++) {
				sbd.append(",").append(colNames[i]);
			}
			return MLSession.getStreamTableEnvironment().fromDataStream(data, sbd.toString());
		}
	}

	public static Table toTable(DataStream <Row> data, String[] colNames, TypeInformation <?>[] colTypes) {
		try {
			return toTable(data, colNames);
		} catch (Exception ex) {
			if (null == colTypes) {
				throw ex;
			} else {
				DataStream <Row> t = getDataSetWithExplicitTypeDefine(data, colNames, colTypes);
				return toTable(t, colNames);
			}
		}
	}

	public static Table toTableUsingStreamEnv(
		DataStream <Row> data, String[] colNames, TypeInformation <?>[] colTypes) {
		if (null == colTypes) {
			throw new RuntimeException("col type is null");
		}

		DataStream <Row> t = getDataSetWithExplicitTypeDefine(data, colNames, colTypes);

		return toTable(t, colNames);
	}

	private static DataStream <Row> getDataSetWithExplicitTypeDefine(
		DataStream <Row> data, String[] colNames, TypeInformation <?>[] colTypes) {
		DataStream <Row> r = data
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
