/*
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */

package org.apache.flink.ml.pipeline;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.ml.api.core.Estimator;
import org.apache.flink.ml.api.core.Transformer;
import org.apache.flink.ml.common.MLEnvironment;
import org.apache.flink.ml.common.MLEnvironmentFactory;
import org.apache.flink.table.api.Table;

import org.junit.Test;

/**
 * The base class for testing the base implementation of pipeline stages, i.e. Estimators and Transformers.
 * This class is package private because we do not expect extension outside of the package.
 */
abstract class PipelineStageTestBase {

	@Test(expected = IllegalArgumentException.class)
	public void testMismatchTableEnvironment() {
		Long id = MLEnvironmentFactory.getNewMLEnvironmentId();
		MLEnvironment env = MLEnvironmentFactory.get(id);
		DataSet<Integer> input = env.getExecutionEnvironment().fromElements(1, 2, 3);
		Table t = env.getBatchTableEnvironment().fromDataSet(input);

		PipelineStageBase<?> pipelineStageBase = createPipelineStage();
		pipelineStageBase.setMLEnvironmentId(id);
		if (pipelineStageBase instanceof EstimatorBase) {
			((Estimator) pipelineStageBase).fit(MLEnvironmentFactory.getDefault().getBatchTableEnvironment(), t);
		} else {
			((Transformer) pipelineStageBase).transform(MLEnvironmentFactory.getDefault().getBatchTableEnvironment(), t);
		}
	}

	@Test(expected = IllegalArgumentException.class)
	public void testNullInputTable() {
		Long id = MLEnvironmentFactory.getNewMLEnvironmentId();
		MLEnvironment env = MLEnvironmentFactory.get(id);

		PipelineStageBase<?> pipelineStageBase = createPipelineStage();
		pipelineStageBase.setMLEnvironmentId(id);
		if (pipelineStageBase instanceof Estimator) {
			((Estimator) pipelineStageBase).fit(env.getBatchTableEnvironment(), null);
		} else {
			((Transformer) pipelineStageBase).transform(env.getBatchTableEnvironment(), null);
		}
	}

	protected abstract PipelineStageBase createPipelineStage();
}
