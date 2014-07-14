package eu.stratosphere.streaming.api;

import java.util.LinkedList;
import java.util.List;

import eu.stratosphere.nephele.io.ChannelSelector;
import eu.stratosphere.nephele.io.RecordReader;
import eu.stratosphere.nephele.io.RecordWriter;
import eu.stratosphere.nephele.template.AbstractTask;
import eu.stratosphere.streaming.api.invokable.UserTaskInvokable;
import eu.stratosphere.streaming.partitioner.DefaultPartitioner;
import eu.stratosphere.streaming.test.TestTaskInvokable;
import eu.stratosphere.types.Record;

public class StreamTask extends AbstractTask {

	// TODO: Refactor names
	private RecordWriter<Record> output;
	private Class<? extends ChannelSelector<Record>> Partitioner;
	private List<RecordReader<Record>> inputs;
	ChannelSelector<Record> partitioner;
	private Class<? extends UserTaskInvokable> UserFunction;
	private UserTaskInvokable userFunction;

	public StreamTask() {
		// TODO: Make configuration file visible and call setClassInputs() here
		inputs = new LinkedList<RecordReader<Record>>();
		Partitioner = null;
		UserFunction = null;
		partitioner = null;
		userFunction = null;
	}

	// TODO:Refactor key names,
	// TODO:Add output/input number to config and store class instances in list
	// TODO:Change default classes when done with JobGraphBuilder
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

	@Override
	public void registerInputOutput() {
		setClassInputs();
		
		int numberOfInputs = getTaskConfiguration().getInteger("numberOfInputs", 0);
		for (int i = 0; i < numberOfInputs; i++) {
			inputs.add(new RecordReader<Record>(this, Record.class));		
		}
		output = new RecordWriter<Record>(this, Record.class, this.partitioner);
	}

	@Override
	public void invoke() throws Exception {
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
