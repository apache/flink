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
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.ml.common.MLEnvironment;
import org.apache.flink.ml.common.MLEnvironmentFactory;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.types.Row;

/**
 * Provide functions of conversions between DataStream and Table.
 */
public class DataStreamConversionUtil {
	/**
	 * Convert the given Table to {@link DataStream}<{@link Row}>.
	 *
	 * @param sessionId the sessionId of {@link MLEnvironmentFactory}
	 * @param table the Table to convert.
	 * @return the converted DataStream.
	 */
	public static DataStream <Row> fromTable(Long sessionId, Table table) {
		return MLEnvironmentFactory
			.get(sessionId)
			.getStreamTableEnvironment()
			.toAppendStream(table, Row.class);
	}

	/**
	 * Convert the given DataStream to Table with specified TableSchema.
	 *
	 * @param sessionId the sessionId of {@link MLEnvironmentFactory}
	 * @param data   the DataStream to convert.
	 * @param schema the specified TableSchema.
	 * @return the converted Table.
	 */
	public static Table toTable(Long sessionId, DataStream <Row> data, TableSchema schema) {
		// TableSchema.getFieldTypes() is deprecated, this should be improved once FLIP-65 is fully merged.
		return toTable(sessionId, data, schema.getFieldNames(), schema.getFieldTypes());
	}

	/**
	 * Convert the given DataStream to Table with specified colNames.
	 *
	 * @param sessionId sessionId the sessionId of {@link MLEnvironmentFactory}.
	 * @param data     the DataStream to convert.
	 * @param colNames the specified colNames.
	 * @return the converted Table.
	 */
	public static Table toTable(Long sessionId, DataStream <Row> data, String[] colNames) {
		return toTable(MLEnvironmentFactory.get(sessionId), data, colNames);
	}

	/**
	 * Convert the given DataStream to Table with specified colNames and colTypes.
	 *
	 * @param sessionId sessionId the sessionId of {@link MLEnvironmentFactory}.
	 * @param data     the DataStream to convert.
	 * @param colNames the specified colNames.
	 * @param colTypes the specified colTypes. This variable is used only when the
	 *                 DataStream is produced by a function and Flink cannot determine
	 *                 automatically what the produced type is.
	 * @return the converted Table.
	 */
	public static Table toTable(Long sessionId, DataStream <Row> data, String[] colNames, TypeInformation <?>[] colTypes) {
		return toTable(MLEnvironmentFactory.get(sessionId), data, colNames, colTypes);
	}

	/**
	 * Convert the given DataStream to Table with specified colNames.
	 *
	 * @param session the MLEnvironment using to convert DataStream to Table.
	 * @param data     the DataStream to convert.
	 * @param colNames the specified colNames.
	 * @return the converted Table.
	 */
	public static Table toTable(MLEnvironment session, DataStream <Row> data, String[] colNames) {
		if (null == colNames || colNames.length == 0) {
			return session.getStreamTableEnvironment().fromDataStream(data);
		} else {
			StringBuilder sbd = new StringBuilder();
			sbd.append(colNames[0]);
			for (int i = 1; i < colNames.length; i++) {
				sbd.append(",").append(colNames[i]);
			}
			return session.getStreamTableEnvironment().fromDataStream(data, sbd.toString());
		}
	}

	/**
	 * Convert the given DataStream to Table with specified colNames and colTypes.
	 *
	 * @param session the MLEnvironment using to convert DataStream to Table.
	 * @param data     the DataStream to convert.
	 * @param colNames the specified colNames.
	 * @param colTypes the specified colTypes. This variable is used only when the
	 *                 DataStream is produced by a function and Flink cannot determine
	 *                 automatically what the produced type is.
	 * @return the converted Table.
	 */
	public static Table toTable(MLEnvironment session, DataStream <Row> data, String[] colNames, TypeInformation <?>[] colTypes) {
		try {
			if (null != colTypes) {
				// Try to add row type information for the datastream to be converted.
				// In most case, this keeps us from the rolling back logic in the catch block,
				// which adds an unnecessary map function just in order to add row type information.
				if (data instanceof SingleOutputStreamOperator) {
					((SingleOutputStreamOperator) data).returns(new RowTypeInfo(colTypes, colNames));
				}
			}
			return toTable(session, data, colNames);
		} catch (ValidationException ex) {
			if (null == colTypes) {
				throw ex;
			} else {
				DataStream <Row> t = fallbackToExplicitTypeDefine(data, colNames, colTypes);
				return toTable(session, t, colNames);
			}
		}
	}

	/**
	 * Adds a type information hint about the colTypes with the Row to the DataStream.
	 *
	 * @param data     the DataStream to add type information.
	 * @param colNames the specified colNames
	 * @param colTypes the specified colTypes
	 * @return the DataStream with type information hint.
	 */
	private static DataStream <Row> fallbackToExplicitTypeDefine(
		DataStream <Row> data,
		String[] colNames,
		TypeInformation <?>[] colTypes) {
		return data
			.map((MapFunction<Row, Row>) t -> t)
			.returns(new RowTypeInfo(colTypes, colNames));
	}
}
