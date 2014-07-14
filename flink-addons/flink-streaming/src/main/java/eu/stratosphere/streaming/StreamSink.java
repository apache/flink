package eu.stratosphere.streaming;

import eu.stratosphere.nephele.io.ChannelSelector;
import eu.stratosphere.nephele.io.RecordReader;
import eu.stratosphere.nephele.template.AbstractOutputTask;
import eu.stratosphere.streaming.partitioner.DefaultPartitioner;
import eu.stratosphere.types.Record;

public class StreamSink extends AbstractOutputTask{
	
	//TODO: Refactor names
	private RecordReader<Record> input = null;
	private Class<? extends UserSinkInvokable> UserFunction;
	private UserSinkInvokable userFunction;
	
	public StreamSink(){
		//TODO: Make configuration file visible and call setClassInputs() here
		UserFunction = null;
		userFunction = null;
	}
	
	//TODO:Refactor key names,
	//TODO:Add output/input number to config and store class instances in list
	//TODO:Change default classes when done with JobGraphBuilder
	//TODO:Change partitioning from component level to connection level -> output_1_partitioner, output_2_partitioner etc.
	private void setClassInputs() {
		UserFunction = getTaskConfiguration().getClass("userfunction",
						TestSinkInvokable.class, UserSinkInvokable.class);
		}
	
	@Override
	public void registerInputOutput() {
		this.input = new RecordReader<Record>(this, Record.class);
	}

	@Override
	public void invoke() throws Exception {
		// TODO Auto-generated method stub
		
	}

}
