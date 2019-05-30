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

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.ml.batchoperator.BatchOperator;
import org.apache.flink.ml.common.MLSession;
import org.apache.flink.ml.common.utils.RowTypeDataSet;
import org.apache.flink.ml.common.utils.Types;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

/**
 * Batch source for the number sequence.
 */
public final class NumSeqSourceBatchOp extends BatchOperator <NumSeqSourceBatchOp> {

	public NumSeqSourceBatchOp(long n) {
		this(1L, n);
	}

	public NumSeqSourceBatchOp(long from, long to) {
		this(from, to, "num");
	}

	public NumSeqSourceBatchOp(long from, long to, String colName) {
		super(null);
		final int parallelism = MLSession.getExecutionEnvironment().getParallelism();
		DataSet <Row> data = MLSession.getExecutionEnvironment()
			.fromElements(0)
			.flatMap(new FlatMapFunction <Integer, Tuple1 <Integer>>() {
				@Override
				public void flatMap(Integer value, Collector <Tuple1 <Integer>> out) throws Exception {
					for (int i = 0; i < parallelism; i++) {
						out.collect(Tuple1.of(i));
					}
				}
			})
			.partitionCustom(new Partitioner <Integer>() {
				@Override
				public int partition(Integer key, int numPartitions) {
					return key % numPartitions;
				}
			}, 0)
			.mapPartition(new RichMapPartitionFunction <Tuple1 <Integer>, Row>() {
				@Override
				public void mapPartition(Iterable <Tuple1 <Integer>> values, Collector <Row> out) throws Exception {
					int taskId = getRuntimeContext().getIndexOfThisSubtask();
					for (long i = from; i <= to; i++) {
						if (i % parallelism == taskId) {
							out.collect(Row.of(i));
						}
					}
				}
			});
		this.table = RowTypeDataSet.toTable(data, new String[] {colName}, new TypeInformation[] {Types.LONG});
	}

	@Override
	public NumSeqSourceBatchOp linkFrom(BatchOperator in) {
		throw new UnsupportedOperationException(
			"Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}

}
