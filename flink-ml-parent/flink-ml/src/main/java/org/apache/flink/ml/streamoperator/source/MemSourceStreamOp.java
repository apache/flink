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

package org.apache.flink.ml.streamoperator.source;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.ml.common.MLSession;
import org.apache.flink.ml.common.utils.RowTypeDataStream;
import org.apache.flink.ml.streamoperator.StreamOperator;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Streaming source which using local data objects.
 */
public final class MemSourceStreamOp extends StreamOperator <MemSourceStreamOp> {

	public MemSourceStreamOp(Object[] vals, String colName) {
		super(null);
		List <Row> rows = new ArrayList <Row>();
		for (Object str : vals) {
			rows.add(Row.of(str));
		}
		init(rows, new String[] {colName});
	}

	public MemSourceStreamOp(Object[][] vals, String[] colNames) {
		super(null);
		List <Row> rows = new ArrayList <Row>();
		for (int i = 0; i < vals.length; i++) {
			rows.add(Row.of(vals[i]));
		}
		init(rows, colNames);
	}

	public MemSourceStreamOp(List <Row> rows, TableSchema schema) {
		super();
		init(rows, schema.getFieldNames(), schema.getFieldTypes());
	}

	public MemSourceStreamOp(Row[] rows, String[] colNames) {
		this(Arrays.asList(rows), colNames);
	}

	public MemSourceStreamOp(List <Row> rows, String[] colNames) {
		super(null);
		init(rows, colNames);
	}

	private void init(List <Row> rows, String[] colNames) {
		if (rows == null || rows.size() < 1) {
			throw new IllegalArgumentException("Values can not be empty.");
		}

		Row first = rows.iterator().next();

		int arity = first.getArity();

		TypeInformation <?>[] types = new TypeInformation[arity];

		for (int i = 0; i < arity; ++i) {
			types[i] = TypeExtractor.getForObject(first.getField(i));
		}

		init(rows, colNames, types);
	}

	private void init(List <Row> rows, String[] colNames, TypeInformation <?>[] colTypes) {
		if (null == colNames || colNames.length < 1) {
			throw new IllegalArgumentException("colNames can not be empty.");
		}

		DataStream <Row> dastr = MLSession.getStreamExecutionEnvironment().fromCollection(rows);

		StringBuilder sbd = new StringBuilder();
		sbd.append(colNames[0]);
		for (int i = 1; i < colNames.length; i++) {
			sbd.append(",").append(colNames[i]);
		}

		this.table = RowTypeDataStream.toTable(dastr, colNames, colTypes);
	}

	@Override
	public MemSourceStreamOp linkFrom(StreamOperator in) {
		throw new UnsupportedOperationException(
			"Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}

}
