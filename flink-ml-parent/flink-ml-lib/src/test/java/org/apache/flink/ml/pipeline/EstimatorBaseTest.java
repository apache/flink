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

import org.apache.flink.api.java.DataSet;
import org.apache.flink.ml.common.MLEnvironment;
import org.apache.flink.ml.common.MLEnvironmentFactory;
import org.apache.flink.ml.operator.batch.BatchOperator;
import org.apache.flink.ml.operator.stream.StreamOperator;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;

import org.junit.Assert;
import org.junit.Test;

/** Test for {@link EstimatorBase}. */
public class EstimatorBaseTest extends PipelineStageTestBase {

    /** This fake estimator simply record which fit method is invoked. */
    private static class FakeEstimator extends EstimatorBase {

        boolean batchFitted = false;
        boolean streamFitted = false;

        @Override
        public ModelBase fit(BatchOperator input) {
            batchFitted = true;
            return null;
        }

        @Override
        public ModelBase fit(StreamOperator input) {
            streamFitted = true;
            return null;
        }
    }

    @Override
    protected PipelineStageBase createPipelineStage() {
        return new FakeEstimator();
    }

    @Test
    public void testFitBatchTable() {
        Long id = MLEnvironmentFactory.getNewMLEnvironmentId();
        MLEnvironment env = MLEnvironmentFactory.get(id);
        DataSet<Integer> input = env.getExecutionEnvironment().fromElements(1, 2, 3);
        Table table = env.getBatchTableEnvironment().fromDataSet(input);

        FakeEstimator estimator = new FakeEstimator();
        estimator.setMLEnvironmentId(id);
        estimator.fit(env.getBatchTableEnvironment(), table);

        Assert.assertTrue(estimator.batchFitted);
        Assert.assertFalse(estimator.streamFitted);
    }

    @Test
    public void testFitStreamTable() {
        Long id = MLEnvironmentFactory.getNewMLEnvironmentId();
        MLEnvironment env = MLEnvironmentFactory.get(id);
        DataStream<Integer> input = env.getStreamExecutionEnvironment().fromElements(1, 2, 3);
        Table table = env.getStreamTableEnvironment().fromDataStream(input);

        FakeEstimator estimator = new FakeEstimator();
        estimator.setMLEnvironmentId(id);
        estimator.fit(env.getStreamTableEnvironment(), table);

        Assert.assertFalse(estimator.batchFitted);
        Assert.assertTrue(estimator.streamFitted);
    }
}
