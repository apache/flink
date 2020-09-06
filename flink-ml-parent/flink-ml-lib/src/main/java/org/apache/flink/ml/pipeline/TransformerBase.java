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

import org.apache.flink.ml.api.core.Transformer;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.ml.operator.batch.BatchOperator;
import org.apache.flink.ml.operator.batch.source.TableSourceBatchOp;
import org.apache.flink.ml.operator.stream.StreamOperator;
import org.apache.flink.ml.operator.stream.source.TableSourceStreamOp;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Preconditions;

/**
 * The base class for transformer implementations.
 *
 * @param <T> A subclass of {@link TransformerBase}, used by {@link
 *            org.apache.flink.ml.api.misc.param.WithParams}
 */
public abstract class TransformerBase<T extends TransformerBase<T>>
	extends PipelineStageBase<T> implements Transformer<T> {

	public TransformerBase() {
		super();
	}

	public TransformerBase(Params params) {
		super(params);
	}

	@Override
	public Table transform(TableEnvironment tEnv, Table input) {
		Preconditions.checkArgument(input != null, "Input CAN NOT BE null!");
		Preconditions.checkArgument(
				tableEnvOf(input) == tEnv,
				"The input table is not in the specified table environment.");
		return transform(input);
	}

	/**
	 * Applies the transformer on the input table, and returns the result table.
	 *
	 * @param input the table to be transformed
	 * @return the transformed table
	 */
	public Table transform(Table input) {
		Preconditions.checkArgument(input != null, "Input CAN NOT BE null!");
		if (tableEnvOf(input) instanceof StreamTableEnvironment) {
			TableSourceStreamOp source = new TableSourceStreamOp(input);
			if (this.params.contains(ML_ENVIRONMENT_ID)) {
				source.setMLEnvironmentId(this.params.get(ML_ENVIRONMENT_ID));
			}
			return transform(source).getOutput();
		} else {
			TableSourceBatchOp source = new TableSourceBatchOp(input);
			if (this.params.contains(ML_ENVIRONMENT_ID)) {
				source.setMLEnvironmentId(this.params.get(ML_ENVIRONMENT_ID));
			}
			return transform(source).getOutput();
		}
	}

	/**
	 * Applies the transformer on the input batch data from BatchOperator, and returns the batch result data with
	 * BatchOperator.
	 *
	 * @param input the input batch data from BatchOperator
	 * @return the transformed batch result data
	 */
	protected abstract BatchOperator transform(BatchOperator input);

	/**
	 * Applies the transformer on the input streaming data from StreamOperator, and returns the streaming result data
	 * with StreamOperator.
	 *
	 * @param input the input streaming data from StreamOperator
	 * @return the transformed streaming result data
	 */
	protected abstract StreamOperator transform(StreamOperator input);

}
