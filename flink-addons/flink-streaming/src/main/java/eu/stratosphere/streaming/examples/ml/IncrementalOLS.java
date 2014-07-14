/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2014 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/
package eu.stratosphere.streaming.examples.ml;

import java.net.InetSocketAddress;
import java.util.Random;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.math.stat.regression.OLSMultipleLinearRegression;
import org.apache.log4j.Level;

import eu.stratosphere.api.java.tuple.Tuple;
import eu.stratosphere.api.java.tuple.Tuple1;
import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.client.minicluster.NepheleMiniCluster;
import eu.stratosphere.client.program.Client;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.streaming.api.JobGraphBuilder;
import eu.stratosphere.streaming.api.invokable.UserSinkInvokable;
import eu.stratosphere.streaming.api.invokable.UserSourceInvokable;
import eu.stratosphere.streaming.api.invokable.UserTaskInvokable;
import eu.stratosphere.streaming.api.streamrecord.StreamRecord;
import eu.stratosphere.streaming.util.LogUtils;

public class IncrementalOLS {

	public static class NewDataSource extends UserSourceInvokable {

		StreamRecord record = new StreamRecord(2, 1);

		Random rnd = new Random();

		@Override
		public void invoke() throws Exception {
			record.initRecords();
			while(true) {
				// pull new record from data source
				record.setTuple(getNewData());
				emit(record);
			}

		}

		private Tuple getNewData() throws InterruptedException {

			return new Tuple2<Boolean, Double[]>(false, new Double[] { rnd.nextDouble() * 3,
					rnd.nextDouble() * 5 });
		}
	}

	public static class TrainingDataSource extends UserSourceInvokable {

		private final int BATCH_SIZE = 1000;

		StreamRecord record = new StreamRecord(2, BATCH_SIZE);

		Random rnd = new Random();

		@Override
		public void invoke() throws Exception {

			record.initRecords();

			while(true) {
				for (int i = 0; i < BATCH_SIZE; i++) {
					record.setTuple(i, getTrainingData());
				}
				emit(record);
			}

		}

		private Tuple getTrainingData() throws InterruptedException {

			return new Tuple2<Double, Double[]>(rnd.nextDouble() * 10, new Double[] {
					rnd.nextDouble() * 3, rnd.nextDouble() * 5 });

		}
	}

	public static class PartialModelBuilder extends UserTaskInvokable {

		@Override
		public void invoke(StreamRecord record) throws Exception {
			emit(buildPartialModel(record));
		}

		protected StreamRecord buildPartialModel(StreamRecord record) {

			Integer numOfTuples = record.getNumOfTuples();
			Integer numOfFeatures = ((Double[]) record.getField(1)).length;

			double[][] x = new double[numOfTuples][numOfFeatures];
			double[] y = new double[numOfTuples];

			for (int i = 0; i < numOfTuples; i++) {

				Tuple t = record.getTuple(i);
				Double[] x_i = (Double[]) t.getField(1);
				y[i] = (Double) t.getField(0);
				for (int j = 0; j < numOfFeatures; j++) {
					x[i][j] = x_i[j];
				}
			}

			OLSMultipleLinearRegression ols = new OLSMultipleLinearRegression();
			ols.newSampleData(y, x);

			return new StreamRecord(new Tuple2<Boolean, Double[]>(true,
					(Double[]) ArrayUtils.toObject(ols.estimateRegressionParameters())));
		}
	}

	public static class Predictor extends UserTaskInvokable {

		// StreamRecord batchModel = null;
		Double[] partialModel = new Double[] { 0.0, 0.0 };

		@Override
		public void invoke(StreamRecord record) throws Exception {
			if (isModel(record)) {
				partialModel = (Double[]) record.getField(1);
				// batchModel = getBatchModel();
			} else {
				emit(predict(record));
			}

		}

		// protected StreamRecord getBatchModel() {
		// return new StreamRecord(new Tuple1<Integer>(1));
		// }

		protected boolean isModel(StreamRecord record) {
			return record.getBoolean(0);
		}

		protected StreamRecord predict(StreamRecord record) {
			Double[] x = (Double[]) record.getField(1);

			Double prediction = 0.0;
			for (int i = 0; i < x.length; i++) {
				prediction = prediction + x[i] * partialModel[i];
			}

			return new StreamRecord(new Tuple1<Double>(prediction));
		}

	}

	public static class Sink extends UserSinkInvokable {

		@Override
		public void invoke(StreamRecord record) throws Exception {
		}
	}

	private static JobGraph getJobGraph() throws Exception {
		JobGraphBuilder graphBuilder = new JobGraphBuilder("IncrementalOLS");

		graphBuilder.setSource("NewData", NewDataSource.class, 1, 1);
		graphBuilder.setSource("TrainingData", TrainingDataSource.class, 1, 1);
		graphBuilder.setTask("PartialModelBuilder", PartialModelBuilder.class, 1, 1);
		graphBuilder.setTask("Predictor", Predictor.class, 1, 1);
		graphBuilder.setSink("Sink", Sink.class, 1, 1);

		graphBuilder.shuffleConnect("TrainingData", "PartialModelBuilder");
		graphBuilder.shuffleConnect("NewData", "Predictor");
		graphBuilder.broadcastConnect("PartialModelBuilder", "Predictor");
		graphBuilder.shuffleConnect("Predictor", "Sink");

		return graphBuilder.getJobGraph();
	}

	public static void main(String[] args) {

		// set logging parameters for local run
		LogUtils.initializeDefaultConsoleLogger(Level.INFO, Level.INFO);

		try {

			// generate JobGraph
			JobGraph jG = getJobGraph();
			Configuration configuration = jG.getJobConfiguration();

			if (args.length == 0 || args[0].equals("local")) {
				System.out.println("Running in Local mode");
				// start local cluster and submit JobGraph
				NepheleMiniCluster exec = new NepheleMiniCluster();
				exec.start();

				Client client = new Client(new InetSocketAddress("localhost", 6498), configuration);

				client.run(jG, true);

				exec.stop();
			} else if (args[0].equals("cluster")) {
				System.out.println("Running in Cluster mode");
				// submit JobGraph to the running cluster
				Client client = new Client(new InetSocketAddress("dell150", 6123), configuration);
				client.run(jG, true);
			}

		} catch (Exception e) {
			System.out.println(e);
		}
	}
}
