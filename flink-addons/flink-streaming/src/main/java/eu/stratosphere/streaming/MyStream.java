package eu.stratosphere.streaming;

import eu.stratosphere.streaming.cellinfo.WorkerEngineExact;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Random;

import eu.stratosphere.core.io.InputSplit;
import eu.stratosphere.core.io.StringRecord;
import eu.stratosphere.nephele.io.RecordReader;
import eu.stratosphere.nephele.io.RecordWriter;
import eu.stratosphere.nephele.io.channels.ChannelType;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.nephele.jobgraph.JobGraphDefinitionException;
import eu.stratosphere.nephele.jobgraph.JobInputVertex;
import eu.stratosphere.nephele.jobgraph.JobOutputVertex;
import eu.stratosphere.nephele.jobgraph.JobTaskVertex;
import eu.stratosphere.nephele.template.AbstractInputTask;
import eu.stratosphere.nephele.template.AbstractOutputTask;
import eu.stratosphere.nephele.template.AbstractTask;
import eu.stratosphere.test.util.TestBase2;

public class MyStream extends TestBase2 {

	public static class RandIS implements InputSplit {

		@Override
		public void write(DataOutput out) throws IOException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void read(DataInput in) throws IOException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public int getSplitNumber() {
			// TODO Auto-generated method stub
			return 0;
		}
		
	}
	
	public static class InfoSource extends AbstractInputTask<RandIS> {

		private RecordWriter<StringRecord> output;
		
		@Override
		public RandIS[] computeInputSplits(int requestedMinNumber)
				throws Exception {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public Class<RandIS> getInputSplitType() {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public void registerInputOutput() {
			output = new RecordWriter<StringRecord>(this, StringRecord.class, new StreamPartitioner());
		}

		@Override
		public void invoke() throws Exception {
			
			Random rnd = new Random();
			
			for(int i=0; i<10; i++) {
				//output.emit(new StringRecord(rnd.nextInt(10)+" "+rnd.nextInt(1000)));
				output.emit(new StringRecord("5 500"));
				output.emit(new StringRecord("4 500"));

			}
			
		}

	}
	
	public static class QuerySource extends AbstractInputTask<RandIS> {

		private RecordWriter<StringRecord> output;
		
		@Override
		public RandIS[] computeInputSplits(int requestedMinNumber)
				throws Exception {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public Class<RandIS> getInputSplitType() {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public void registerInputOutput() {
			output = new RecordWriter<StringRecord>(this, StringRecord.class, new StreamPartitioner());
		}

		@Override
		public void invoke() throws Exception {
			Random rnd = new Random();

			for(int i=0; i<5; i++) {
				//output.emit(new StringRecord(rnd.nextInt(10)+" "+rnd.nextInt(1000)+" 500"));
				output.emit(new StringRecord("5 510 100"));
				output.emit(new StringRecord("4 510 100"));
			}
			
		}

	}
	
	public static class MySink extends AbstractOutputTask {

		private RecordReader<StringRecord> input = null;
		
		@Override
		public void registerInputOutput() {
			this.input = new RecordReader<StringRecord>(this, StringRecord.class);
		}

		@Override
		public void invoke() throws Exception {
			
			while(input.hasNext()) {
				System.out.println(input.next().toString());
			}
			
		}


	}
	
	public static class MyStreamMap extends AbstractTask {

		private RecordReader<StringRecord> inputInfo = null;
		private RecordReader<StringRecord> inputQuery = null;

		private RecordWriter<StringRecord> output = null;
		
		private WorkerEngineExact engine = new WorkerEngineExact(10,1000,0);
		
		
		@Override
		public void invoke() throws Exception {
			while (this.inputInfo.hasNext() && this.inputQuery.hasNext()) {
				String[] info = inputInfo.next().toString().split(" ");
				String[] query = inputQuery.next().toString().split(" ");
				
				engine.put(Integer.parseInt(info[0]),Long.parseLong(info[1]));
				
				
				
				this.output.emit(new StringRecord(String.valueOf(
							engine.get(Long.parseLong(query[1]),Long.parseLong(query[2]),Integer.parseInt(query[0])))
							));
			}
			while (this.inputInfo.hasNext()) {

				StringRecord info = inputInfo.next();
	
				this.output.emit(info);
			}
			while (this.inputQuery.hasNext()) {

				StringRecord query = inputQuery.next();
	
				this.output.emit(query);
			}
		}

		@Override
		public void registerInputOutput() {
			this.inputInfo = new RecordReader<StringRecord>(this, StringRecord.class);
			this.inputQuery = new RecordReader<StringRecord>(this, StringRecord.class);
			this.output = new RecordWriter<StringRecord>(this, StringRecord.class);
		}


	}
	
	
	@Override
	public JobGraph getJobGraph() {
		
		final JobGraph myJG = new JobGraph("MyStream");
		
		// SOURCE
		
		final JobInputVertex infoSource = new JobInputVertex("MyInfoSource", myJG);
		// final TaskConfig config = new TaskConfig(source.getConfiguration());
		infoSource.setInputClass(InfoSource.class);
		
		final JobInputVertex querySource = new JobInputVertex("MyQuerySource", myJG);
		// final TaskConfig config = new TaskConfig(source.getConfiguration());
		querySource.setInputClass(QuerySource.class);
		
		// TASK
		final JobTaskVertex task1 = new JobTaskVertex("MyTask1", myJG);
		task1.setTaskClass(MyStreamMap.class);
		task1.setNumberOfSubtasks(2);
		
		// SINK
		final JobOutputVertex sink = new JobOutputVertex("MySink", myJG);
		// final TaskConfig config = new TaskConfig(sink.getConfiguration());
		sink.setOutputClass(MySink.class);
		
		try {
			infoSource.connectTo(task1, ChannelType.INMEMORY);
			querySource.connectTo(task1, ChannelType.INMEMORY);
			task1.connectTo(sink, ChannelType.INMEMORY);
			

		} catch (JobGraphDefinitionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		JobGraphBuilder graphBuilder = new JobGraphBuilder("testGraph");
		graphBuilder.setSource("infoSource", InfoSource.class);
		graphBuilder.setSource("querySource", QuerySource.class);
		graphBuilder.setTask("cellTask", MyStreamMap.class, 2);
		graphBuilder.setSink("sink", MySink.class);
		
		graphBuilder.connectSource("infoSource", "cellTask", ChannelType.INMEMORY);
		graphBuilder.connectSource("querySource", "cellTask", ChannelType.INMEMORY);
		graphBuilder.connectSink("cellTask", "sink", ChannelType.INMEMORY);
		
		return graphBuilder.getJobGraph();
		
	}
	
}
