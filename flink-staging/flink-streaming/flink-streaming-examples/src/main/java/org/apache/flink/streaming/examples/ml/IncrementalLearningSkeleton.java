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

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.WindowMapFunction;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.helper.Time;
import org.apache.flink.streaming.api.windowing.helper.Timestamp;
import org.apache.flink.util.Collector;

/**
 * Skeleton for incremental machine learning algorithm consisting of a
 * pre-computed model, which gets updated for the new inputs and new input data
 * for which the job provides predictions.
 * <p/>
 * <p>
 * This may serve as a base of a number of algorithms, e.g. updating an
 * incremental Alternating Least Squares model while also providing the
 * predictions.
 * </p>
 * <p/>
 * <p/>
 * This example shows how to use:
 * <ul>
 * <li>Connected streams
 * <li>CoFunctions
 * <li>Tuple data types
 * </ul>
 */
public class IncrementalLearningSkeleton {

	private static DataStream<Integer> trainingData = null;
	private static DataStream<Integer> newData = null;

	// *************************************************************************
	// PROGRAM
	// *************************************************************************

	public static void main(String[] args) throws Exception {

		if (!parseParameters(args)) {
			return;
		}

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		// env.setDegreeOfParallelism(1);
		createSourceStreams(env);

		// build new model on every second of new data
		DataStream<Double[]> model = trainingData.window(Time.of(5000, new LinearTimestamp()))
				.mapWindow(new PartialModelBuilder()).flatten();

		// use partial model for newData
		DataStream<Integer> prediction = newData.connect(model).map(new Predictor());

		// emit result
		if (fileOutput) {
			prediction.writeAsText(outputPath, 1);
		} else {
			prediction.print();
		}

		// execute program
		env.execute("Streaming Incremental Learning");
	}

	// *************************************************************************
	// USER FUNCTIONS
	// *************************************************************************

	/**
	 * Feeds new data for newData. By default it is implemented as constantly
	 * emitting the Integer 1 in a loop.
	 */
	public static class NewDataSource implements SourceFunction<Integer> {
		private static final long serialVersionUID = 1L;
		private static final int NEW_DATA_SLEEP_TIME = 1000;

		@Override
		public boolean reachedEnd() throws Exception {
			return false;
		}

		@Override
		public Integer next() throws Exception {
			Thread.sleep(NEW_DATA_SLEEP_TIME);
			return 1;
		}

	}

	/**
	 * Feeds new data for newData. By default it is implemented as constantly
	 * emitting the Integer 1 in a loop.
	 */
	public static class FiniteNewDataSource implements SourceFunction<Integer> {
		private static final long serialVersionUID = 1L;
		private int counter;

		private Integer getNewData() throws InterruptedException {
			Thread.sleep(5);
			counter++;
			return 1;
		}

		@Override
		public boolean reachedEnd() throws Exception {
			return counter >= 50;
		}

		@Override
		public Integer next() throws Exception {
			return getNewData();
		}

	}

	/**
	 * Feeds new training data for the partial model builder. By default it is
	 * implemented as constantly emitting the Integer 1 in a loop.
	 */
	public static class TrainingDataSource implements SourceFunction<Integer> {
		private static final long serialVersionUID = 1L;
		private static final int TRAINING_DATA_SLEEP_TIME = 10;

		@Override
		public boolean reachedEnd() throws Exception {
			return false;
		}

		@Override
		public Integer next() throws Exception {
			Thread.sleep(TRAINING_DATA_SLEEP_TIME);
			return 1;
		}

	}

	/**
	 * Feeds new training data for the partial model builder. By default it is
	 * implemented as constantly emitting the Integer 1 in a loop.
	 */
	public static class FiniteTrainingDataSource implements SourceFunction<Integer> {
		private static final long serialVersionUID = 1L;
		private int counter = 0;

		private Integer getTrainingData() throws InterruptedException {
			counter++;
			return 1;
		}

		@Override
		public boolean reachedEnd() throws Exception {
			return counter >= 8200;
		}

		@Override
		public Integer next() throws Exception {
			return getTrainingData();
		}

	}

	public static class LinearTimestamp implements Timestamp<Integer> {
		private static final long serialVersionUID = 1L;

		private long counter = 0L;

		@Override
		public long getTimestamp(Integer value) {
			return counter += 10L;
		}
	}

	/**
	 * Builds up-to-date partial models on new training data.
	 */
	public static class PartialModelBuilder implements WindowMapFunction<Integer, Double[]> {
		private static final long serialVersionUID = 1L;

		protected Double[] buildPartialModel(Iterable<Integer> values) {
			return new Double[]{1.};
		}

		@Override
		public void mapWindow(Iterable<Integer> values, Collector<Double[]> out) throws Exception {
			out.collect(buildPartialModel(values));
		}
	}

	/**
	 * Creates newData using the model produced in batch-processing and the
	 * up-to-date partial model.
	 * <p/>
	 * <p>
	 * By defaults emits the Integer 0 for every newData and the Integer 1
	 * for every model update.
	 * </p>
	 */
	public static class Predictor implements CoMapFunction<Integer, Double[], Integer> {
		private static final long serialVersionUID = 1L;

		Double[] batchModel = null;
		Double[] partialModel = null;

		@Override
		public Integer map1(Integer value) {
			// Return newData
			return predict(value);
		}

		@Override
		public Integer map2(Double[] value) {
			// Update model
			partialModel = value;
			batchModel = getBatchModel();
			return 1;
		}

		// pulls model built with batch-job on the old training data
		protected Double[] getBatchModel() {
			return new Double[]{0.};
		}

		// performs newData using the two models
		protected Integer predict(Integer inTuple) {
			return 0;
		}

	}

	// *************************************************************************
	// UTIL METHODS
	// *************************************************************************

	private static boolean fileOutput = false;
	private static String outputPath;

	private static boolean parseParameters(String[] args) {

		if (args.length > 0) {
			// parse input arguments
			fileOutput = true;
			if (args.length == 1) {
				outputPath = args[0];
			} else {
				System.err.println("Usage: IncrementalLearningSkeleton <result path>");
				return false;
			}
		} else {
			System.out.println("Executing IncrementalLearningSkeleton with generated data.");
			System.out.println("  Provide parameter to write to file.");
			System.out.println("  Usage: IncrementalLearningSkeleton <result path>");
		}
		return true;
	}

	public static void createSourceStreams(StreamExecutionEnvironment env) {
		if (fileOutput) {
			trainingData = env.addSource(new FiniteTrainingDataSource());
			newData = env.addSource(new FiniteNewDataSource());
		} else {
			trainingData = env.addSource(new TrainingDataSource());
			newData = env.addSource(new NewDataSource());
		}
	}
}
