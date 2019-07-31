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

package org.apache.flink.ml.pipeline;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.ml.batchoperator.BatchOperator;
import org.apache.flink.ml.batchoperator.source.TableSourceBatchOp;
import org.apache.flink.ml.common.MLSession;
import org.apache.flink.ml.streamoperator.StreamOperator;
import org.apache.flink.ml.streamoperator.source.TableSourceStreamOp;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.internal.TableImpl;
import org.apache.flink.table.api.java.StreamTableEnvironment;

/**
 * Abstract class for a transformer that transform one data into another.
 * A transformer is a {@link PipelineStage} that transforms an input {@link Table} to a result
 * {@link Table}.
 *
 * @param <T> The class type of the {@link Transformer} implementation itself, used by {@link
 *            org.apache.flink.ml.api.misc.param.WithParams}
 */
public abstract class Transformer<T extends Transformer <T>>
	extends PipelineStage <T> implements org.apache.flink.ml.api.core.Transformer <T> {

	public Transformer() {
		super();
	}

	public Transformer(Params params) {
		super(params);
	}

	/**
	 * Applies the transformer on the input table, and returns the result table.
	 *
	 * @param tEnv  the table environment to which the input table is bound.
	 * @param input the table to be transformed
	 * @return the transformed table
	 */
	@Override
	public Table transform(TableEnvironment tEnv, Table input) {
		MLSession.setTableEnvironment(tEnv, input);
		return transform(input);
	}

	/**
	 * Applies the transformer on the input table, and returns the result table.
	 *
	 * @param input the table to be transformed
	 * @return the transformed table
	 */
	public Table transform(Table input) {
		if (null == input) {
			throw new IllegalArgumentException("Input CAN NOT BE null!");
		}
		if (((TableImpl) input).getTableEnvironment() instanceof StreamTableEnvironment) {
			return transform(new TableSourceStreamOp(input)).getOutput();
		} else {
			return transform(new TableSourceBatchOp(input)).getOutput();
		}
	}

	/**
	 * Applies the transformer on the input batch data from BatchOperator, and returns the batch result data with
	 * BatchOperator.
	 *
	 * @param input the input batch data from BatchOperator
	 * @return the transformed batch result data
	 */
	public abstract BatchOperator transform(BatchOperator input);

	/**
	 * Applies the transformer on the input streaming data from StreamOperator, and returns the streaming result data
	 * with StreamOperator.
	 *
	 * @param input the input streaming data from StreamOperator
	 * @return the transformed streaming result data
	 */
	public abstract StreamOperator transform(StreamOperator input);

}
