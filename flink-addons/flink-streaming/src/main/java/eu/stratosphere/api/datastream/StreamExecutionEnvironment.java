package eu.stratosphere.api.datastream;

import eu.stratosphere.api.java.functions.FlatMapFunction;
import eu.stratosphere.api.java.tuple.Tuple;
import eu.stratosphere.api.java.tuple.Tuple1;
import eu.stratosphere.api.java.typeutils.TypeExtractor;
import eu.stratosphere.streaming.api.JobGraphBuilder;
import eu.stratosphere.streaming.api.StreamCollector;
import eu.stratosphere.streaming.api.invokable.UserSinkInvokable;
import eu.stratosphere.streaming.api.invokable.UserSourceInvokable;
import eu.stratosphere.streaming.api.invokable.UserTaskInvokable;
import eu.stratosphere.streaming.api.streamrecord.ArrayStreamRecord;
import eu.stratosphere.streaming.api.streamrecord.StreamRecord;
import eu.stratosphere.streaming.faulttolerance.FaultToleranceType;
import eu.stratosphere.streaming.util.ClusterUtil;
import eu.stratosphere.types.TypeInformation;

public class StreamExecutionEnvironment {
	JobGraphBuilder jobGraphBuilder;
	
	private static final int BATCH_SIZE = 1;
	
	public StreamExecutionEnvironment() {
		jobGraphBuilder = new JobGraphBuilder("jobGraph",FaultToleranceType.NONE);
	}
	
	private final class DummySource extends UserSourceInvokable {
		private static final long serialVersionUID = 1L;

		@Override
		public void invoke() throws Exception {
			StreamRecord outRecord = new ArrayStreamRecord(1);
			
			for (int i = 0; i < 10; i++) {
				outRecord.setTuple(0, new Tuple1<String>("++ message #" + i + " ++" ));
				emit(outRecord);
			}
		}
	}		
	
	public <T extends Tuple, R extends Tuple> DataStream<R> addFlatMapFunction(DataStream<T> inputStream, final FlatMapFunction<T, R> flatMapper, TypeInformation<R> returnType) {
		DataStream<R> returnStream = new DataStream<R>(this, returnType);
		
		jobGraphBuilder.setTask(inputStream.getId(), new UserTaskInvokable<T, R>() {
			private static final long serialVersionUID = 1L;

			@Override
			public void invoke(StreamRecord record, StreamCollector<R> collector) throws Exception {
				int batchSize = record.getBatchSize();
				for (int i = 0; i < batchSize; i++) {
					T tuple = (T) record.getTuple(i);
					flatMapper.flatMap(tuple, collector);
					// outRecord.setTuple(i, (Tuple) resultTuple);
				}
			}
		});
		
		jobGraphBuilder.shuffleConnect(inputStream.getId(), returnStream.getId());

		return returnStream;
	}
	
//	public <T, R> DataStream<R> addMapFunction(DataStream<T> inputStream, final MapFunction<T, R> mapper, TypeInformation<R> returnType) {
//		DataStream<R> returnStream = new DataStream<R>(this, returnType);
//		
//		jobGraphBuilder.setTask(inputStream.getId(), new UserTaskInvokable() {
//			private static final long serialVersionUID = 1L;
//			private StreamRecord outRecord = new ArrayStreamRecord(BATCH_SIZE);
//			
//			@Override
//			public void invoke(StreamRecord record) throws Exception {
//				int batchSize = record.getBatchSize();
//				for (int i = 0; i < batchSize; i++) {
//					T tuple = (T) record.getTuple(i);
//					R resultTuple = mapper.map(tuple);
//					outRecord.setTuple(i, (Tuple) resultTuple);
//				}
//			}
//		});
//		
//		jobGraphBuilder.shuffleConnect(inputStream.getId(), returnStream.getId());
//		return returnStream;
//	}

	
	public void execute(String idToSink) {
		jobGraphBuilder.setSink("sink", new UserSinkInvokable() {

			@Override
			public void invoke(StreamRecord record, StreamCollector collector) throws Exception {
				System.out.println("SINK: " + record);
			}
		});
		jobGraphBuilder.shuffleConnect(idToSink, "sink");
		ClusterUtil.runOnMiniCluster(jobGraphBuilder.getJobGraph());
	}

	public DataStream<Tuple1<String>> setDummySource() {
		Tuple1<String> tup = new Tuple1<String>("asd");
		DataStream<Tuple1<String>> returnStream = new DataStream<Tuple1<String>>(this, (TypeInformation<Tuple1<String>>) TypeExtractor.getForObject(tup));
		
		jobGraphBuilder.setSource(returnStream.getId(), DummySource.class);
		return returnStream;
	}
}
