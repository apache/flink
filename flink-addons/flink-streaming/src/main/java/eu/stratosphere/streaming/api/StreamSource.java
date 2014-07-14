package eu.stratosphere.streaming.api;

import java.util.LinkedList;
import java.util.List;

import eu.stratosphere.nephele.io.ChannelSelector;
import eu.stratosphere.nephele.io.RecordWriter;
import eu.stratosphere.nephele.template.AbstractInputTask;
import eu.stratosphere.streaming.api.invokable.UserSourceInvokable;
import eu.stratosphere.streaming.partitioner.DefaultPartitioner;
import eu.stratosphere.streaming.test.RandIS;
import eu.stratosphere.streaming.test.TestSourceInvokable;
import eu.stratosphere.types.Record;

public class StreamSource extends AbstractInputTask<RandIS> {

  // TODO: Refactor names
  private List<RecordWriter<Record>> outputs;
  private Class<? extends ChannelSelector<Record>> Partitioner;
  private List<ChannelSelector<Record>> partitioners;
  private Class<? extends UserSourceInvokable> UserFunction;
  private UserSourceInvokable userFunction;

  private int numberOfOutputs;

  public StreamSource() {
    // TODO: Make configuration file visible and call setClassInputs() here
    outputs = new LinkedList<RecordWriter<Record>>();
    partitioners = new LinkedList<ChannelSelector<Record>>();
    Partitioner = null;
    UserFunction = null;
    userFunction = null;
    numberOfOutputs = 0;

  }

  @Override
  public RandIS[] computeInputSplits(int requestedMinNumber) throws Exception {
    return null;
  }

  @Override
  public Class<RandIS> getInputSplitType() {
    return null;
  }

  // TODO:Refactor key names,
  // TODO:Change default classes when done with JobGraphBuilder
  private void setConfigInputs() {

    UserFunction = getTaskConfiguration().getClass("userfunction",
        TestSourceInvokable.class, UserSourceInvokable.class);

    numberOfOutputs = getTaskConfiguration().getInteger("numberOfOutputs", 0);

    for (int i = 1; i <= numberOfOutputs; i++) {
      Partitioner = getTaskConfiguration().getClass("partitioner_" + i,
          DefaultPartitioner.class, ChannelSelector.class);

      try {
        partitioners.add(Partitioner.newInstance());
        // System.out.println("partitioner added");
      } catch (Exception e) {
        // System.out.println("partitioner error" + " " + "partitioner_" + i);
      }
    }

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

  @Override
  public void invoke() throws Exception {

    for (RecordWriter<Record> output : outputs) {
      userFunction.invoke(output);
    }
  }

}
