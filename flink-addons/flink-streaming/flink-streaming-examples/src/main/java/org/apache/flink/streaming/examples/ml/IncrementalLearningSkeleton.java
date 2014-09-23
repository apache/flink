/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.examples.ml;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.function.co.CoMapFunction;
import org.apache.flink.streaming.api.function.source.SourceFunction;
import org.apache.flink.util.Collector;

public class IncrementalLearningSkeleton {

	// Source for feeding new data for prediction
	public static class NewDataSource implements SourceFunction<Integer> {
		private static final long serialVersionUID = 1L;

		@Override
		public void invoke(Collector<Integer> collector) throws Exception {
			while (true) {
				collector.collect(getNewData());
			}
		}

		// Method for pulling new data for prediction
		private Integer getNewData() throws InterruptedException {
			Thread.sleep(1000);
			return 1;
		}
	}

	// Source for feeding new training data for partial model building
	public static class TrainingDataSource implements SourceFunction<Integer> {
		private static final long serialVersionUID = 1L;

		@Override
		public void invoke(Collector<Integer> collector) throws Exception {

			while (true) {
				collector.collect(getTrainingData());
			}

		}

		// Method for pulling new training data
		private Integer getTrainingData() throws InterruptedException {
			Thread.sleep(1000);
			return 1;

		}
	}

	// Task for building up-to-date partial models on new training data
	public static class PartialModelBuilder implements GroupReduceFunction<Integer, Double[]> {
		private static final long serialVersionUID = 1L;

		// Method for building partial model on the grouped training data
		protected Double[] buildPartialModel(Iterable<Integer> values) {
			return new Double[] { 1. };
		}

		@Override
		public void reduce(Iterable<Integer> values, Collector<Double[]> out) throws Exception {
			out.collect(buildPartialModel(values));
		}
	}

	// Task for performing prediction using the model produced in
	// batch-processing and the up-to-date partial model
	public static class Predictor implements CoMapFunction<Integer, Double[], Integer> {
		private static final long serialVersionUID = 1L;

		Double[] batchModel = null;
		Double[] partialModel = null;

		@Override
		public Integer map1(Integer value) {
			// Return prediction
			return predict(value);
		}

		@Override
		public Integer map2(Double[] value) {
			// Update model
			partialModel = value;
			batchModel = getBatchModel();
			return 1;
		}

		// Pulls model built with batch-job on the old training data
		protected Double[] getBatchModel() {
			return new Double[] { 0. };
		}

		// Performs prediction using the two models
		protected Integer predict(Integer inTuple) {
			return 0;
		}

	}

	private static final int PARALLELISM = 1;
	private static final int SOURCE_PARALLELISM = 1;

	public static void main(String[] args) throws Exception {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(
				PARALLELISM).setBufferTimeout(1000);

		// Build new model on every second of new data
		DataStream<Double[]> model = env.addSource(new TrainingDataSource(), SOURCE_PARALLELISM)
				.window(5000).reduceGroup(new PartialModelBuilder());

		// Use partial model for prediction
		DataStream<Integer> prediction = env.addSource(new NewDataSource(), SOURCE_PARALLELISM)
				.connect(model).map(new Predictor());

		prediction.print();

		env.execute();
	}



}
