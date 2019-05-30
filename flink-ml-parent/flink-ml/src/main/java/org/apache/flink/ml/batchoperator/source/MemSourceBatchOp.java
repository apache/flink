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

package org.apache.flink.ml.batchoperator.source;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.ml.batchoperator.BatchOperator;
import org.apache.flink.ml.common.MLSession;
import org.apache.flink.ml.common.utils.RowTypeDataSet;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Batch source which using local data objects.
 */
public final class MemSourceBatchOp extends BatchOperator <MemSourceBatchOp> {

	public MemSourceBatchOp(Object[] vals, String colName) {
		super(null);
		List <Row> rows = new ArrayList <Row>();
		for (Object val : vals) {
			rows.add(Row.of(val));
		}
		init(rows, new String[] {colName});
	}

	public MemSourceBatchOp(Object[][] vals, String[] colNames) {
		super(null);
		List <Row> rows = new ArrayList <Row>();
		for (int i = 0; i < vals.length; i++) {
			rows.add(Row.of(vals[i]));
		}
		init(rows, colNames);
	}

	public MemSourceBatchOp(List <Row> rows, TableSchema schema) {
		super(null);
		init(rows, schema.getFieldNames(), schema.getFieldTypes());
	}

	public MemSourceBatchOp(Row[] rows, String[] colNames) {
		this(Arrays.asList(rows), colNames);
	}

	public MemSourceBatchOp(List <Row> rows, String[] colNames) {
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

		DataSet <Row> dastr = MLSession.getExecutionEnvironment().fromCollection(rows);

		this.table = RowTypeDataSet.toTable(dastr, colNames, colTypes);
	}

	@Override
	public MemSourceBatchOp linkFrom(BatchOperator in) {
		throw new UnsupportedOperationException("Not supported yet.");
	}

}
