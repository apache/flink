/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.ml.api.core;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;

/**
 * Estimators are {@link PipelineStage}s responsible for training and generating machine learning
 * models.
 *
 * <p>The implementations are expected to take an input table as training samples and generate a
 * {@link Model} which fits these samples.
 *
 * @param <E> class type of the Estimator implementation itself, used by {@link
 *            org.apache.flink.ml.api.misc.param.WithParams}.
 * @param <M> class type of the {@link Model} this Estimator produces.
 */
@PublicEvolving
public interface Estimator<E extends Estimator<E, M>, M extends Model<M>> extends PipelineStage<E> {

	/**
	 * Train and produce a {@link Model} which fits the records in the given {@link Table}.
	 *
	 * @param tEnv  the table environment to which the input table is bound.
	 * @param input the table with records to train the Model.
	 * @return a model trained to fit on the given Table.
	 */
	M fit(TableEnvironment tEnv, Table input);
}
