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

package org.apache.flink.ml.batchoperator.dataproc;

import org.apache.flink.api.common.functions.CrossFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.ml.batchoperator.BatchOperator;
import org.apache.flink.ml.common.utils.ArrayUtil;
import org.apache.flink.ml.common.utils.RowTypeDataSet;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;

import java.util.List;

/**
 * Cross operation for Batch data.
 */
public final class CrossBatchOp extends BatchOperator <CrossBatchOp> {

	private Type type = Type.Auto;

	public CrossBatchOp() {
		super(null);
	}

	public CrossBatchOp(CrossBatchOp.Type type) {
		super(null);
		this.type = type;
	}

	@Override
	public CrossBatchOp linkFrom(List <BatchOperator> ins) {
		if (null == ins || ins.size() != 2) {
			throw new RuntimeException("Need 2 inputs!");
		}
		Table table1 = ins.get(0).getTable();
		Table table2 = ins.get(1).getTable();
		DataSet <Row> r;
		if (Type.WithTiny == type) {
			r = RowTypeDataSet.fromTable(table1).crossWithTiny(RowTypeDataSet.fromTable(table2)).with(
				new MyCrossFunc());
		} else if (Type.WithHuge == type) {
			r = RowTypeDataSet.fromTable(table1).crossWithHuge(RowTypeDataSet.fromTable(table2)).with(
				new MyCrossFunc());
		} else {
			r = RowTypeDataSet.fromTable(table1).cross(RowTypeDataSet.fromTable(table2)).with(new MyCrossFunc());
		}
		this.table = RowTypeDataSet.toTable(r,
			ArrayUtil.arrayMerge(table1.getSchema().getFieldNames(), table2.getSchema().getFieldNames()),
			ArrayUtil.arrayMerge(table1.getSchema().getFieldTypes(), table2.getSchema().getFieldTypes()));
		return this;
	}

	@Override
	public CrossBatchOp linkFrom(BatchOperator in) {
		throw new RuntimeException("Need 2 inputs!");
	}

	/**
	 * Type of cross operation.
	 */
	public enum Type {

		Auto,
		WithTiny,
		WithHuge
	}

	private static class MyCrossFunc implements CrossFunction <Row, Row, Row> {

		@Override
		public Row cross(Row in1, Row in2) throws Exception {
			int n1 = in1.getArity();
			int n2 = in2.getArity();
			Row r = new Row(n1 + n2);
			for (int i = 0; i < n1; i++) {
				r.setField(i, in1.getField(i));
			}
			for (int i = 0; i < n2; i++) {
				r.setField(i + n1, in2.getField(i));
			}
			return r;
		}
	}

}
