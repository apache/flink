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

import org.apache.log4j.Level;

import eu.stratosphere.api.java.tuple.Tuple;
import eu.stratosphere.api.java.tuple.Tuple1;
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

public class IncrementalLearning {

	public static class NewDataSource extends UserSourceInvokable {

		StreamRecord record = new StreamRecord(new Tuple1<Integer>(1));

		@Override
		public void invoke() throws Exception {

			while (true) {
				// pull new record from data source
				record.setTuple(getNewData());
				emit(record);
			}

		}

		private Tuple getNewData() throws InterruptedException {
			return new Tuple1<Integer>(1);
		}
	}

	public static class TrainingDataSource extends UserSourceInvokable {

		private final int BATCH_SIZE = 1000;

		StreamRecord record = new StreamRecord(1, BATCH_SIZE);

		@Override
		public void invoke() throws Exception {

			record.initRecords();

			for (int j = 0; j < 10; j++) {
				for (int i = 0; i < BATCH_SIZE; i++) {
					record.setTuple(i, getTrainingData());
				}
				emit(record);
			}

		}

		private Tuple getTrainingData() throws InterruptedException {
			return new Tuple1<Integer>(1);

		}
	}

	public static class PartialModelBuilder extends UserTaskInvokable {

		@Override
		public void invoke(StreamRecord record) throws Exception {
			emit(buildPartialModel(record));
		}

		protected StreamRecord buildPartialModel(StreamRecord record) {
			return new StreamRecord(new Tuple1<Integer>(1));
		}

	}

	public static class Predictor extends UserTaskInvokable {

		StreamRecord batchModel = null;
		StreamRecord partialModel = null;

		@Override
		public void invoke(StreamRecord record) throws Exception {
			if (isModel(record)) {
				partialModel = record;
				batchModel = getBatchModel();
			} else {
				emit(predict(record));
			}

		}

		protected StreamRecord getBatchModel() {
			return new StreamRecord(new Tuple1<Integer>(1));
		}

		protected boolean isModel(StreamRecord record) {
			return true;
		}

		protected StreamRecord predict(StreamRecord record) {
			return new StreamRecord(new Tuple1<Integer>(0));
		}

	}

	public static class Sink extends UserSinkInvokable {

		@Override
		public void invoke(StreamRecord record) throws Exception {
			// do nothing
		}
	}

	private static JobGraph getJobGraph() throws Exception {
		JobGraphBuilder graphBuilder = new JobGraphBuilder("IncrementalLearning");

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
