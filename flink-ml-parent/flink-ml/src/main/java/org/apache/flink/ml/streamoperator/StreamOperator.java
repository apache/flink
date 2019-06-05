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

package org.apache.flink.ml.streamoperator;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.ml.common.AlgoOperator;
import org.apache.flink.ml.common.MLSession;
import org.apache.flink.ml.common.utils.RowTypeDataStream;
import org.apache.flink.ml.streamoperator.dataproc.SampleStreamOp;
import org.apache.flink.ml.streamoperator.source.TableSourceStreamOp;
import org.apache.flink.ml.streamoperator.utils.PrintStreamOp;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.List;

/**
 * Base class of streaming algorithm operators.
 */
public abstract class StreamOperator<T extends StreamOperator <T>> extends AlgoOperator <T> {

	protected String tableName = null;

	public StreamOperator() {
		super();
	}

	public StreamOperator(Params params) {
		super(params);
	}

	public static JobExecutionResult execute() throws Exception {
		return MLSession.getStreamExecutionEnvironment().execute();
	}

	public static JobExecutionResult execute(String string) throws Exception {
		return MLSession.getStreamExecutionEnvironment().execute(string);
	}

	public static void setParallelism(int parallelism) {
		MLSession.getStreamExecutionEnvironment().setParallelism(parallelism);
	}

	public static StreamOperator sourceFrom(Table table) {
		return new TableSourceStreamOp(table);
	}

	public DataStream <Row> getDataStream() {
		return RowTypeDataStream.fromTable(getTable());
	}

	protected void setTable(DataStream <Row> dataSet, TableSchema schema) {
		setTable(RowTypeDataStream.toTable(dataSet, schema));
	}

	protected void setTable(DataStream <Row> dataSet) {
		setTable(RowTypeDataStream.toTable(dataSet));
	}

	protected void setTable(DataStream <Row> dataSet, String[] colNames) {
		setTable(RowTypeDataStream.toTable(dataSet, colNames));
	}

	protected void setTable(DataStream <Row> dataSet, String[] colNames, TypeInformation <?>[] colTypes) {
		setTable(RowTypeDataStream.toTable(dataSet, colNames, colTypes));
	}

	public String getTableName() {
		if (null == tableName) {
			tableName = getTable().toString();
		}
		return tableName;
	}

	public StreamOperator setTableName(String name) {
		MLSession.getStreamTableEnvironment().registerTable(name, getTable());
		this.tableName = name;
		return this;
	}

	@Override
	public String toString() {
		return getTable().toString();
	}

	public StreamOperator sample(double ratio) {
		return linkTo(new SampleStreamOp(ratio));
	}

	public StreamOperator sample(double ratio, long maxSamples) {
		return linkTo(new SampleStreamOp(ratio, maxSamples));
	}

	public StreamOperator getSideOutput() {
		return getSideOutput(0);
	}

	public StreamOperator getSideOutput(int idx) {
		if (null == this.sideTables) {
			throw new RuntimeException("There is no side output.");
		} else if (idx < 0 && idx >= this.sideTables.length) {
			throw new RuntimeException("There is no  side output.");
		} else {
			return new TableSourceStreamOp(this.sideTables[idx]);
		}
	}

	public int getSideOutputCount() {
		return null == this.sideTables ? 0 : this.sideTables.length;
	}

	@Override
	public StreamOperator print() {
		return linkTo(new PrintStreamOp());
	}

	public <S extends StreamOperator> S link(S next) {
		return linkTo(next);
	}

	public <S extends StreamOperator> S linkTo(S next) {
		next.linkFrom(this);
		return (S) next;
	}

	public abstract T linkFrom(StreamOperator in);

	public T linkFrom(StreamOperator in1, StreamOperator in2) {
		List <StreamOperator> ls = new ArrayList();
		ls.add(in1);
		ls.add(in2);
		return linkFrom(ls);
	}

	public T linkFrom(StreamOperator in1, StreamOperator in2, StreamOperator in3) {
		List <StreamOperator> ls = new ArrayList();
		ls.add(in1);
		ls.add(in2);
		ls.add(in3);
		return linkFrom(ls);
	}

	public T linkFrom(List <StreamOperator> ins) {
		if (null != ins && ins.size() == 1) {
			return linkFrom(ins.get(0));
		} else {
			throw new RuntimeException("Not support more than 1 inputs!");
		}
	}

	@Override
	public StreamOperator select(String param) {
		return new TableSourceStreamOp(this.table.select(param));
	}

	@Override
	public StreamOperator select(String[] colNames) {
		StringBuilder sbd = new StringBuilder();
		for (int i = 0; i < colNames.length; i++) {
			if (i > 0) {
				sbd.append(",");
			}
			sbd.append("`").append(colNames[i]).append("`");
		}
		return select(sbd.toString());
	}

}
