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
  private Class<? extends ChannelSelector<Record>> Partitioner;
  private List<RecordReader<Record>> inputs;
  private List<RecordWriter<Record>> outputs;
  private List<ChannelSelector<Record>> partitioners;
  private Class<? extends UserTaskInvokable> UserFunction;
  private UserTaskInvokable userFunction;

  private int numberOfInputs;
  private int numberOfOutputs;

  public StreamTask() {
    // TODO: Make configuration file visible and call setClassInputs() here
    inputs = new LinkedList<RecordReader<Record>>();
    outputs = new LinkedList<RecordWriter<Record>>();
    Partitioner = null;
    UserFunction = null;
    partitioners = new LinkedList<ChannelSelector<Record>>();
    userFunction = null;
    numberOfInputs = 0;
    numberOfOutputs = 0;
  }

  // TODO:Refactor key names,
  // TODO:Change default classes when done with JobGraphBuilder
  private void setConfigInputs() {

    numberOfInputs = getTaskConfiguration().getInteger("numberOfInputs", 0);
    for (int i = 0; i < numberOfInputs; i++) {
      inputs.add(new RecordReader<Record>(this, Record.class));
    }

    numberOfOutputs = getTaskConfiguration().getInteger("numberOfOutputs", 0);

    for (int i = 1; i <= numberOfOutputs; i++) {
      Partitioner = getTaskConfiguration().getClass("partitioner_" + i,
          DefaultPartitioner.class, ChannelSelector.class);
      try {
        partitioners.add(Partitioner.newInstance());
      } catch (Exception e) {

      }
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
    setConfigInputs();
    for (ChannelSelector<Record> partitioner : partitioners) {
      outputs.add(new RecordWriter<Record>(this, Record.class, partitioner));
    }
  }

  // TODO: Performance with multiple outputs
  @Override
  public void invoke() throws Exception {
    boolean hasInput = true;
    while (hasInput) {
      hasInput = false;
      for (RecordReader<Record> input : inputs) {
        if (input.hasNext()) {
          hasInput = true;
          for (RecordWriter<Record> output : outputs) {
            userFunction.invoke(input.next(), output);
          }
        }
      }
    }
  }

}
