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

import org.apache.flink.ml.api.core.Estimator;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.ml.operator.batch.BatchOperator;
import org.apache.flink.ml.operator.batch.source.TableSourceBatchOp;
import org.apache.flink.ml.operator.stream.StreamOperator;
import org.apache.flink.ml.operator.stream.source.TableSourceStreamOp;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.internal.TableImpl;
import org.apache.flink.util.Preconditions;

/**
 * The base class for estimator implementations.
 *
 * @param <E> A subclass of the {@link EstimatorBase}, used by {@link
 *     org.apache.flink.ml.api.misc.param.WithParams}
 * @param <M> class type of the {@link ModelBase} this Estimator produces.
 */
public abstract class EstimatorBase<E extends EstimatorBase<E, M>, M extends ModelBase<M>>
        extends PipelineStageBase<E> implements Estimator<E, M> {

    public EstimatorBase() {
        super();
    }

    public EstimatorBase(Params params) {
        super(params);
    }

    @Override
    public M fit(TableEnvironment tEnv, Table input) {
        Preconditions.checkArgument(input != null, "Input CAN NOT BE null!");
        Preconditions.checkArgument(
                tableEnvOf(input) == tEnv,
                "The input table is not in the specified table environment.");
        return fit(input);
    }

    /**
     * Train and produce a {@link ModelBase} which fits the records in the given {@link Table}.
     *
     * @param input the table with records to train the Model.
     * @return a model trained to fit on the given Table.
     */
    public M fit(Table input) {
        Preconditions.checkArgument(input != null, "Input CAN NOT BE null!");
        if (((TableImpl) input).getTableEnvironment() instanceof StreamTableEnvironment) {
            TableSourceStreamOp source = new TableSourceStreamOp(input);
            if (this.params.contains(ML_ENVIRONMENT_ID)) {
                source.setMLEnvironmentId(this.params.get(ML_ENVIRONMENT_ID));
            }
            return fit(source);
        } else {
            TableSourceBatchOp source = new TableSourceBatchOp(input);
            if (this.params.contains(ML_ENVIRONMENT_ID)) {
                source.setMLEnvironmentId(this.params.get(ML_ENVIRONMENT_ID));
            }
            return fit(source);
        }
    }

    /**
     * Train and produce a {@link ModelBase} which fits the records from the given {@link
     * BatchOperator}.
     *
     * @param input the table with records to train the Model.
     * @return a model trained to fit on the given Table.
     */
    protected abstract M fit(BatchOperator input);

    /**
     * Online learning and produce {@link ModelBase} series which fit the streaming records from the
     * given {@link StreamOperator}.
     *
     * @param input the StreamOperator with streaming records to online train the Model series.
     * @return the model series trained to fit on the streaming data from given StreamOperator.
     */
    protected M fit(StreamOperator input) {
        throw new UnsupportedOperationException("NOT supported yet!");
    }
}
