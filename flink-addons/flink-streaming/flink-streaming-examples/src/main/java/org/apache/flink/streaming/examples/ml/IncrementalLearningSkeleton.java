/**
 *
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
 *
 */

package org.apache.flink.streaming.examples.ml;

import org.apache.flink.api.java.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.function.source.SourceFunction;
import org.apache.flink.util.Collector;

public class IncrementalLearningSkeleton {

	// Source for feeding new data for prediction
	public static class NewDataSource implements SourceFunction<Tuple1<Integer>> {
		private static final long serialVersionUID = 1L;

		@Override
		public void invoke(Collector<Tuple1<Integer>> collector) throws Exception {
			while (true) {
				collector.collect(getNewData());
			}
		}

		// Method for pulling new data for prediction
		private Tuple1<Integer> getNewData() throws InterruptedException {
			return new Tuple1<Integer>(1);
		}
	}

	// Source for feeding new training data for partial model building
	public static class TrainingDataSource implements SourceFunction<Tuple1<Integer>> {
		private static final long serialVersionUID = 1L;

		@Override
		public void invoke(Collector<Tuple1<Integer>> collector) throws Exception {

			while (true) {
				// Group the predefined number of records in a streamrecord then
				// emit for model building
				collector.collect(getTrainingData());
				;
			}

		}

		// Method for pulling new training data
		private Tuple1<Integer> getTrainingData() throws InterruptedException {
			return new Tuple1<Integer>(1);

		}
	}

	// Task for building up-to-date partial models on new training data
	public static class PartialModelBuilder extends RichMapFunction<Tuple1<Integer>, Tuple1<Integer>> {
		private static final long serialVersionUID = 1L;

		@Override
		public Tuple1<Integer> map(Tuple1<Integer> inTuple) throws Exception {
			return buildPartialModel(inTuple);
		}

		// Method for building partial model on the grouped training data
		protected Tuple1<Integer> buildPartialModel(Tuple1<Integer> inTuple) {
			return new Tuple1<Integer>(1);
		}

	}

	// Task for performing prediction using the model produced in
	// batch-processing and the up-to-date partial model
	public static class Predictor extends RichMapFunction<Tuple1<Integer>, Tuple1<Integer>> {
		private static final long serialVersionUID = 1L;

		Tuple1<Integer> batchModel = null;
		Tuple1<Integer> partialModel = null;

		@Override
		public Tuple1<Integer> map(Tuple1<Integer> inTuple) throws Exception {
			if (isModel(inTuple)) {
				partialModel = inTuple;
				batchModel = getBatchModel();
				return null; // TODO: fix
			} else {
				return predict(inTuple);
			}

		}

		// Pulls model built with batch-job on the old training data
		protected Tuple1<Integer> getBatchModel() {
			return new Tuple1<Integer>(1);
		}

		// Checks whether the record is a model or a new data
		protected boolean isModel(Tuple1<Integer> inTuple) {
			return true;
		}

		// Performs prediction using the two models
		protected Tuple1<Integer> predict(Tuple1<Integer> inTuple) {
			return new Tuple1<Integer>(0);
		}

	}

	private static final int PARALLELISM = 1;
	private static final int SOURCE_PARALLELISM = 1;

	public static void main(String[] args) {

		StreamExecutionEnvironment env = StreamExecutionEnvironment
				.createLocalEnvironment(PARALLELISM);

		DataStream<Tuple1<Integer>> model = env
				.addSource(new TrainingDataSource(), SOURCE_PARALLELISM)
				.map(new PartialModelBuilder()).broadcast();

		@SuppressWarnings("unchecked")
		DataStream<Tuple1<Integer>> prediction = env
				.addSource(new NewDataSource(), SOURCE_PARALLELISM).merge(model)
				.map(new Predictor());

		prediction.print();

		env.execute();
	}
}