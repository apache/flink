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

package eu.stratosphere.streaming.api;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

import eu.stratosphere.api.java.functions.FlatMapFunction;
import eu.stratosphere.api.java.functions.GroupReduceFunction;
import eu.stratosphere.api.java.functions.MapFunction;
import eu.stratosphere.api.java.tuple.Tuple;
import eu.stratosphere.api.java.tuple.Tuple1;
import eu.stratosphere.streaming.api.invokable.UserSourceInvokable;
import eu.stratosphere.streaming.faulttolerance.FaultToleranceType;
import eu.stratosphere.streaming.util.ClusterUtil;
import eu.stratosphere.util.Collector;

//TODO: add file, elements, rmq source
//TODO: figure out generic dummysink
public class StreamExecutionEnvironment {
	JobGraphBuilder jobGraphBuilder;

	public StreamExecutionEnvironment(int batchSize) {
		if (batchSize < 1) {
			throw new IllegalArgumentException("Batch size must be positive.");
		}
		jobGraphBuilder = new JobGraphBuilder("jobGraph", FaultToleranceType.NONE, batchSize);
	}

	public StreamExecutionEnvironment() {
		this(1);
	}

	private static class DummySource extends UserSourceInvokable<Tuple1<String>> {
		private static final long serialVersionUID = 1L;

		public void invoke(Collector<Tuple1<String>> collector) {

			for (int i = 0; i < 10; i++) {
				collector.collect(new Tuple1<String>("source"));
			}
		}
	}
	
	public static enum ConnectionType {
		SHUFFLE, BROADCAST, FIELD
	}

	private <T extends Tuple> void connectGraph(DataStream<T> inputStream, String outputID) {

		for (int i = 0; i < inputStream.connectIDs.size(); i++) {
			ConnectionType type = inputStream.ctypes.get(i);
			String input = inputStream.connectIDs.get(i);
			int param = inputStream.cparams.get(i);

			switch (type) {
			case SHUFFLE:
				jobGraphBuilder.shuffleConnect(input, outputID);
				break;
			case BROADCAST:
				jobGraphBuilder.broadcastConnect(input, outputID);
				break;
			case FIELD:
				jobGraphBuilder.fieldsConnect(input, outputID, param);
				break;
			}

		}
	}

	public <T extends Tuple, R extends Tuple> DataStream<R> addFlatMapFunction(
			DataStream<T> inputStream, final FlatMapFunction<T, R> flatMapper) {
		DataStream<R> returnStream = new DataStream<R>(this);

		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		ObjectOutputStream oos;
		try {
			oos = new ObjectOutputStream(baos);
			oos.writeObject(flatMapper);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		jobGraphBuilder.setTask(returnStream.getId(), new FlatMapInvokable<T, R>(flatMapper),
				"flatMap", baos.toByteArray());

		connectGraph(inputStream, returnStream.getId());

		return returnStream;
	}

	public <T extends Tuple, R extends Tuple> DataStream<R> addMapFunction(
			DataStream<T> inputStream, final MapFunction<T, R> mapper) {
		DataStream<R> returnStream = new DataStream<R>(this);

		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		ObjectOutputStream oos;
		try {
			oos = new ObjectOutputStream(baos);
			oos.writeObject(mapper);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		jobGraphBuilder.setTask(returnStream.getId(), new MapInvokable<T, R>(mapper), "map",
				baos.toByteArray());

		connectGraph(inputStream, returnStream.getId());

		return returnStream;
	}

	public <T extends Tuple, R extends Tuple> DataStream<R> addBatchReduceFunction(
			DataStream<T> inputStream, final GroupReduceFunction<T, R> reducer) {
		DataStream<R> returnStream = new DataStream<R>(this);

		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		ObjectOutputStream oos;
		try {
			oos = new ObjectOutputStream(baos);
			oos.writeObject(reducer);
		} catch (IOException e) {
			e.printStackTrace();
		}

		jobGraphBuilder.setTask(returnStream.getId(), new BatchReduceInvokable<T, R>(reducer),
				"batchReduce", baos.toByteArray());

		connectGraph(inputStream, returnStream.getId());

		return returnStream;
	}

	public <T extends Tuple> DataStream<T> addSink(DataStream<T> inputStream,
			SinkFunction<T> sinkFunction) {
		DataStream<T> returnStream = new DataStream<T>(this);

		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		ObjectOutputStream oos;
		try {
			oos = new ObjectOutputStream(baos);
			oos.writeObject(sinkFunction);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		jobGraphBuilder.setSink("sink", new SinkInvokable<T>(sinkFunction), "sink",
				baos.toByteArray());

		connectGraph(inputStream, "sink");

		return returnStream;
	}

	public static final class DummySink extends SinkFunction<Tuple1<String>> {
		private static final long serialVersionUID = 1L;

		@Override
		public void invoke(Tuple1<String> tuple) {
			System.out.println(tuple);
		}

	}

	public <T extends Tuple> DataStream<T> addDummySink(DataStream<T> inputStream) {

		return addSink(inputStream, (SinkFunction<T>) new DummySink());
	}

	public void execute() {
		ClusterUtil.runOnMiniCluster(jobGraphBuilder.getJobGraph());
	}

	public <T extends Tuple> DataStream<T> addSource(SourceFunction<T> sourceFunction) {
		DataStream<T> returnStream = new DataStream<T>(this);

		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		ObjectOutputStream oos;
		try {
			oos = new ObjectOutputStream(baos);
			oos.writeObject(sourceFunction);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		jobGraphBuilder.setSource(returnStream.getId(), sourceFunction, "source",
				baos.toByteArray());

		return returnStream;
	}

	public DataStream<Tuple1<String>> addFileSource(String path) {
		return addSource(new FileSourceFunction(path));
	} 
	
	public DataStream<Tuple1<String>> addDummySource() {
		DataStream<Tuple1<String>> returnStream = new DataStream<Tuple1<String>>(this);

		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		ObjectOutputStream oos;
		try {
			oos = new ObjectOutputStream(baos);
			oos.writeObject(new DummySource());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		jobGraphBuilder.setSource(returnStream.getId(), new DummySource(), "source",
				baos.toByteArray());
		return returnStream;
	}

	public JobGraphBuilder jobGB() {
		return jobGraphBuilder;
	}
}
