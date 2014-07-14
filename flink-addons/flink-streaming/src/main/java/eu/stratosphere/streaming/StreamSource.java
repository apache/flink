package eu.stratosphere.streaming;

import eu.stratosphere.nephele.io.ChannelSelector;
import eu.stratosphere.nephele.io.RecordWriter;
import eu.stratosphere.nephele.template.AbstractInputTask;
import eu.stratosphere.streaming.partitioner.DefaultPartitioner;
import eu.stratosphere.types.Record;

public class StreamSource extends AbstractInputTask<RandIS> {

  //TODO: Refactor names
	private RecordWriter<Record> output;
	private Class<? extends ChannelSelector<Record>> Partitioner;
	ChannelSelector<Record> partitioner;
	private Class<? extends UserSourceInvokable> UserFunction;
	private UserSourceInvokable userFunction;

	public StreamSource() {
	  //TODO: Make configuration file visible and call setClassInputs() here
		Partitioner = null;
		UserFunction = null;
		partitioner = null;
		userFunction = null;
	}

	//TODO: Learn relevance of InputSplits
	@Override
	public RandIS[] computeInputSplits(int requestedMinNumber) throws Exception {
		return null;
	}

	@Override
	public Class<RandIS> getInputSplitType() {
		return null;
	}

	//TODO:Refactor key names,
	//TODO:Add output/input number to config and store class instances in list
	//TODO:Change default classes when done with JobGraphBuilder
	//TODO:Change partitioning from component level to connection level -> output_1_partitioner, output_2_partitioner etc.
	private void setClassInputs() {
		Partitioner = getTaskConfiguration().getClass("partitioner",
				DefaultPartitioner.class, ChannelSelector.class);
		try {
			partitioner = Partitioner.newInstance();
		} catch (Exception e) {

		}
		UserFunction = getTaskConfiguration().getClass("userfunction",
				TestSourceInvokable.class, UserSourceInvokable.class);
		
	}
	//TODO: Store outputs in List
	@Override
	public void registerInputOutput() {
		setClassInputs();
		output = new RecordWriter<Record>(this, Record.class, this.partitioner);
	}

	//TODO: call userFunction.invoke for all output channels
	@Override
	public void invoke() throws Exception {
	  try {
	    userFunction = UserFunction.newInstance();
	  } catch (Exception e) {

	  }
	  	userFunction.invoke(output);
	}

}
