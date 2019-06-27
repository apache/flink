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

package org.apache.flink.ml.batchoperator;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.ml.batchoperator.dataproc.CrossBatchOp;
import org.apache.flink.ml.batchoperator.dataproc.FirstN;
import org.apache.flink.ml.batchoperator.dataproc.SampleBatchOp;
import org.apache.flink.ml.batchoperator.dataproc.SampleWithSizeBatchOp;
import org.apache.flink.ml.batchoperator.source.TableSourceBatchOp;
import org.apache.flink.ml.batchoperator.utils.PrintBatchOp;
import org.apache.flink.ml.common.AlgoOperator;
import org.apache.flink.ml.common.MLSession;
import org.apache.flink.ml.common.utils.RowTypeDataSet;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.List;

/**
 * Base class of batch algorithm operators.
 */
public abstract class BatchOperator<T extends BatchOperator <T>> extends AlgoOperator <T> {

	protected String tableName = null;

	public BatchOperator() {
		super();
	}

	public BatchOperator(Params params) {
		super(params);
	}

	public static void execute() throws Exception {
		MLSession.getExecutionEnvironment().execute();
	}

	public static void setParallelism(int parallelism) {
		MLSession.getExecutionEnvironment().setParallelism(parallelism);
	}

	public static BatchOperator sourceFrom(Table table) {
		return new TableSourceBatchOp(table);
	}

	public long count() throws Exception {
		return RowTypeDataSet.fromTable(getTable()).count();
	}

	public DataSet <Row> getDataSet() {
		return RowTypeDataSet.fromTable(getTable());
	}

	protected void setTable(DataSet <Row> dataSet, TableSchema schema) {
		setTable(RowTypeDataSet.toTable(dataSet, schema));
	}

	protected void setTable(DataSet <Row> dataSet) {
		setTable(RowTypeDataSet.toTable(dataSet));
	}

	protected void setTable(DataSet <Row> dataSet, String[] colNames) {
		setTable(RowTypeDataSet.toTable(dataSet, colNames));
	}

	protected void setTable(DataSet <Row> dataSet, String[] colNames, TypeInformation <?>[] colTypes) {
		setTable(RowTypeDataSet.toTable(dataSet, colNames, colTypes));
	}

	public List <Row> collect() {
		try {
			return getDataSet().collect();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public String getTableName() {
		if (null == tableName) {
			tableName = getTable().toString();
		}
		return tableName;
	}

	public BatchOperator setTableName(String name) {
		MLSession.getBatchTableEnvironment().registerTable(name, getTable());
		this.tableName = name;
		return this;
	}

	public BatchOperator getSideOutput() {
		return getSideOutput(0);
	}

	public BatchOperator getSideOutput(int idx) {
		if (null == this.sideTables) {
			throw new RuntimeException("There is no side output.");
		} else if (idx < 0 && idx >= this.sideTables.length) {
			throw new RuntimeException("There is no  side output.");
		} else {
			return new TableSourceBatchOp(this.sideTables[idx]);
		}
	}

	public int getSideOutputCount() {
		return null == this.sideTables ? 0 : this.sideTables.length;
	}

	@Override
	public BatchOperator print() throws Exception {
		return linkTo(new PrintBatchOp());
	}

	public BatchOperator sample(double ratio) {
		return linkTo(new SampleBatchOp(ratio));
	}

	public BatchOperator sample(double ratio, boolean withReplacement) {
		return linkTo(new SampleBatchOp(ratio, withReplacement));
	}

	public BatchOperator sample(double ratio, boolean withReplacement, long seed) {
		return linkTo(new SampleBatchOp(ratio, withReplacement, seed));
	}

	public BatchOperator sampleWithSize(int numSamples) {
		return linkTo(new SampleWithSizeBatchOp(numSamples));
	}

	public BatchOperator sampleWithSize(int numSamples, boolean withReplacement) {
		return linkTo(new SampleWithSizeBatchOp(numSamples, withReplacement));
	}

	public BatchOperator sampleWithSize(int numSamples, boolean withReplacement, long seed) {
		return linkTo(new SampleWithSizeBatchOp(numSamples, withReplacement, seed));
	}

	@Override
	public String toString() {
		return getTable().toString();
	}

	public <B extends BatchOperator> B link(B next) {
		return linkTo(next);
	}

	public <B extends BatchOperator> B linkTo(B next) {
		next.linkFrom(this);
		return (B) next;
	}

	public abstract T linkFrom(BatchOperator in);

	public T linkFrom(BatchOperator in1, BatchOperator in2) {
		List <BatchOperator> ls = new ArrayList();
		ls.add(in1);
		ls.add(in2);
		return linkFrom(ls);
	}

	public T linkFrom(BatchOperator in1, BatchOperator in2, BatchOperator in3) {
		List <BatchOperator> ls = new ArrayList();
		ls.add(in1);
		ls.add(in2);
		ls.add(in3);
		return linkFrom(ls);
	}

	public T linkFrom(List <BatchOperator> ins) {
		if (null != ins && ins.size() == 1) {
			return linkFrom(ins.get(0));
		} else {
			throw new RuntimeException("Not support more than 1 inputs!");
		}
	}

	@Override
	public BatchOperator select(String param) {
		return new TableSourceBatchOp(this.table.select(param));
	}

	@Override
	public BatchOperator select(String[] colNames) {
		return select(String.join(",", colNames));
	}

	public FirstN firstN(int n) {
		return (FirstN) linkTo(new FirstN(n));
	}

	public BatchOperator cross(BatchOperator batchOp) {
		CrossBatchOp crs = new CrossBatchOp();
		return crs.linkFrom(this, batchOp);
	}

	public BatchOperator crossWithTiny(BatchOperator batchOp) {
		CrossBatchOp crs = new CrossBatchOp(CrossBatchOp.Type.WithTiny);
		return crs.linkFrom(this, batchOp);
	}

	public BatchOperator crossWithHuge(BatchOperator batchOp) {
		CrossBatchOp crs = new CrossBatchOp(CrossBatchOp.Type.WithHuge);
		return crs.linkFrom(this, batchOp);
	}
}
