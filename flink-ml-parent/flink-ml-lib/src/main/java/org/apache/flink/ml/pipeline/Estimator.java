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
 * Abstract class for a estimator that fit a {@link Model}.
 *
 * @param <E> The class type of the {@link Estimator} implementation itself
 * @param <M> class type of the {@link Model} this Estimator produces.
 */
public abstract class Estimator<E extends Estimator <E, M>, M extends Model <M>>
	extends PipelineStage <E> implements org.apache.flink.ml.api.core.Estimator <E, M> {

	public Estimator() {
		super();
	}

	public Estimator(Params params) {
		super(params);
	}

	/**
	 * Train and produce a {@link Model} which fits the records in the given {@link Table}.
	 *
	 * @param tEnv  the table environment to which the input table is bound.
	 * @param input the table with records to train the Model.
	 * @return a model trained to fit on the given Table.
	 */
	@Override
	public M fit(TableEnvironment tEnv, Table input) {
		MLSession.setTableEnvironment(tEnv, input);
		return fit(input);
	}

	/**
	 * Train and produce a {@link Model} which fits the records in the given {@link Table}.
	 *
	 * @param input the table with records to train the Model.
	 * @return a model trained to fit on the given Table.
	 */
	public M fit(Table input) {
		if (null == input) {
			throw new IllegalArgumentException("Input CAN NOT BE null!");
		}
		if (((TableImpl) input).getTableEnvironment() instanceof StreamTableEnvironment) {
			return fit(new TableSourceStreamOp(input));
		} else {
			return fit(new TableSourceBatchOp(input));
		}
	}

	/**
	 * Train and produce a {@link Model} which fits the records from the given {@link BatchOperator}.
	 *
	 * @param input the table with records to train the Model.
	 * @return a model trained to fit on the given Table.
	 */
	public abstract M fit(BatchOperator input);

	/**
	 * Online learning and produce {@link Model} series which fit the streaming records from the given {@link
	 * StreamOperator}.
	 *
	 * @param input the StreamOperator with streaming records to online train the Model series.
	 * @return the model series trained to fit on the streaming data from given StreamOperator.
	 */
	public M fit(StreamOperator input) {
		throw new UnsupportedOperationException("NOT supported yet!");
	}

}
