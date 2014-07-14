package eu.stratosphere.streaming;

import java.util.ArrayList;
import java.util.List;

import eu.stratosphere.nephele.io.ChannelSelector;
import eu.stratosphere.nephele.io.RecordReader;
import eu.stratosphere.nephele.io.RecordWriter;
import eu.stratosphere.nephele.template.AbstractTask;
import eu.stratosphere.types.Record;

public class StreamTask extends AbstractTask {

  //TODO: Refactor names
	private RecordWriter<Record> output;
	private Class<? extends ChannelSelector<Record>> Partitioner;
	ChannelSelector<Record> partitioner;
	private Class<? extends UserTaskInvokable> UserFunction;
	private UserTaskInvokable userFunction;

	private RecordReader<Record> inputInfo = null;
	private RecordReader<Record> inputQuery = null;

	public StreamTask() {
	   //TODO: Make configuration file visible and call setClassInputs() here
		Partitioner = null;
		UserFunction = null;
		partitioner = null;
		userFunction = null;
	}

	//TODO:Refactor key names,
  //TODO:Add output/input number to config and store class instances in list
  //TODO:Change default classes when done with JobGraphBuilder
	private void setClassInputs() {
		Partitioner = getTaskConfiguration().getClass("partitioner",
				DefaultPartitioner.class, ChannelSelector.class);
		try {
			partitioner = Partitioner.newInstance();
		} catch (Exception e) {

		}
		UserFunction = getTaskConfiguration().getClass("userfunction",
				TestTaskInvokable.class, UserTaskInvokable.class);
		try {
			userFunction = UserFunction.newInstance();
		} catch (Exception e) {

		}

	}

	//TODO: Store inputs and outputs in List
	@Override
	public void registerInputOutput() {
		setClassInputs();
		this.inputInfo = new RecordReader<Record>(this, Record.class);
		this.inputQuery = new RecordReader<Record>(this, Record.class);
		output = new RecordWriter<Record>(this, Record.class, this.partitioner);

	}

	@Override
	public void invoke() throws Exception {
		List<RecordReader<Record>> inputs = new ArrayList<RecordReader<Record>>();
		inputs.add(inputInfo);
		inputs.add(inputQuery);

		boolean hasInput = true;
		while (hasInput) {
			hasInput = false;
			for (RecordReader<Record> input : inputs) {
				if (input.hasNext()) {
					hasInput = true;
					userFunction.invoke(input.next(), output);
				}
			}
		}

	}

}
