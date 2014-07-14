package eu.stratosphere.streaming.api;

import eu.stratosphere.nephele.io.RecordReader;
import eu.stratosphere.nephele.template.AbstractOutputTask;
import eu.stratosphere.streaming.api.invokable.UserSinkInvokable;
import eu.stratosphere.streaming.test.TestSinkInvokable;
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
		setClassInputs();
		this.input = new RecordReader<Record>(this, Record.class);
	}

	@Override
	public void invoke() throws Exception {
		try {
		    userFunction = UserFunction.newInstance();
		} catch (Exception e) {

		}
		
		boolean hasInput = true;
		while (hasInput){
		hasInput = false;
			if (input.hasNext()){
				hasInput = true;
				userFunction.invoke(
						input.next(), 
						input);
			}
		}
	}

}
