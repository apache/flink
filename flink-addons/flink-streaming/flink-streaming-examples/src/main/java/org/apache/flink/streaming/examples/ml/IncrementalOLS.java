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

import java.util.Random;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.math.stat.regression.OLSMultipleLinearRegression;
import org.apache.flink.api.java.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.function.source.SourceFunction;
import org.apache.flink.util.Collector;

public class IncrementalOLS {

	public static class NewDataSource implements SourceFunction<Tuple2<Boolean, Double[]>> {

		private static final long serialVersionUID = 1L;
		Random rnd = new Random();

		@Override
		public void invoke(Collector<Tuple2<Boolean, Double[]>> collector) throws Exception {
			while (true) {
				// pull new record from data source
				collector.collect(getNewData());
			}

		}

		private Tuple2<Boolean, Double[]> getNewData() throws InterruptedException {

			return new Tuple2<Boolean, Double[]>(false, new Double[] { rnd.nextDouble() * 3,
					rnd.nextDouble() * 5 });
		}
	}

	public static class TrainingDataSource implements SourceFunction<Tuple2<Double, Double[]>> {
		private static final long serialVersionUID = 1L;

		Random rnd = new Random();

		@Override
		public void invoke(Collector<Tuple2<Double, Double[]>> out) throws Exception {

			while (true) {
				out.collect(getTrainingData());
			}

		}

		private Tuple2<Double, Double[]> getTrainingData() throws InterruptedException {

			return new Tuple2<Double, Double[]>(rnd.nextDouble() * 10, new Double[] {
					rnd.nextDouble() * 3, rnd.nextDouble() * 5 });

		}
	}

	public static class PartialModelBuilder extends
			RichMapFunction<Tuple2<Double, Double[]>, Tuple2<Boolean, Double[]>> {
		private static final long serialVersionUID = 1L;

		@Override
		public Tuple2<Boolean, Double[]> map(Tuple2<Double, Double[]> inTuple) throws Exception {
			return buildPartialModel(inTuple);
		}

		// TODO: deal with batchsize
		protected Tuple2<Boolean, Double[]> buildPartialModel(Tuple2<Double, Double[]> inTuple) {

			// Integer numOfTuples = record.getNumOfTuples();
			Integer numOfTuples = 1;
			Integer numOfFeatures = ((Double[]) inTuple.getField(1)).length;

			double[][] x = new double[numOfTuples][numOfFeatures];
			double[] y = new double[numOfTuples];

			for (int i = 0; i < numOfTuples; i++) {

				// Tuple t = record.getTuple(i);
				Tuple t = inTuple;
				Double[] x_i = (Double[]) t.getField(1);
				y[i] = (Double) t.getField(0);
				for (int j = 0; j < numOfFeatures; j++) {
					x[i][j] = x_i[j];
				}
			}

			OLSMultipleLinearRegression ols = new OLSMultipleLinearRegression();
			ols.newSampleData(y, x);

			return new Tuple2<Boolean, Double[]>(true, (Double[]) ArrayUtils.toObject(ols
					.estimateRegressionParameters()));
		}
	}

	// TODO: How do I know the x for which I have predicted y?
	public static class Predictor extends RichMapFunction<Tuple2<Boolean, Double[]>, Tuple1<Double>> {
		private static final long serialVersionUID = 1L;

		// StreamRecord batchModel = null;
		Double[] partialModel = new Double[] { 0.0, 0.0 };

		@Override
		public Tuple1<Double> map(Tuple2<Boolean, Double[]> inTuple) throws Exception {
			if (isModel(inTuple)) {
				partialModel = inTuple.f1;
				// batchModel = getBatchModel();
				return null; // TODO: fix
			} else {
				return predict(inTuple);
			}

		}

		protected boolean isModel(Tuple2<Boolean, Double[]> inTuple) {
			return inTuple.f0;
		}

		protected Tuple1<Double> predict(Tuple2<Boolean, Double[]> inTuple) {
			Double[] x = inTuple.f1;

			Double prediction = 0.0;
			for (int i = 0; i < x.length; i++) {
				prediction = prediction + x[i] * partialModel[i];
			}

			return new Tuple1<Double>(prediction);
		}

	}

	private static final int PARALLELISM = 1;
	private static final int SOURCE_PARALLELISM = 1;

	public static void main(String[] args) {

		StreamExecutionEnvironment env = StreamExecutionEnvironment
				.createLocalEnvironment(PARALLELISM);

		DataStream<Tuple2<Boolean, Double[]>> model = env
				.addSource(new TrainingDataSource(), SOURCE_PARALLELISM)
				.map(new PartialModelBuilder()).broadcast();

		@SuppressWarnings("unchecked")
		DataStream<Tuple1<Double>> prediction = env
				.addSource(new NewDataSource(), SOURCE_PARALLELISM).merge(model)
				.map(new Predictor());

		prediction.print();

		env.execute();
	}
}
