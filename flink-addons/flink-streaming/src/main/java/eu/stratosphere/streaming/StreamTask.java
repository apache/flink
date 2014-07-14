package eu.stratosphere.streaming;

import java.util.ArrayList;
import java.util.List;

import eu.stratosphere.core.io.IOReadableWritable;
import eu.stratosphere.core.io.StringRecord;
import eu.stratosphere.nephele.io.ChannelSelector;
import eu.stratosphere.nephele.io.RecordReader;
import eu.stratosphere.nephele.io.RecordWriter;
import eu.stratosphere.nephele.template.AbstractInputTask;
import eu.stratosphere.nephele.template.AbstractTask;
import eu.stratosphere.streaming.cellinfo.WorkerEngineExact;

public class StreamTask extends AbstractTask {

  private RecordWriter<IOReadableWritable> output;
  private Class<? extends ChannelSelector<IOReadableWritable>> Partitioner;
  ChannelSelector<IOReadableWritable> partitioner;
  private Class<? extends UserTaskInvokable> UserFunction;
  private UserTaskInvokable userFunction;
  

  private RecordReader<IOReadableWritable> inputInfo = null;
  private RecordReader<IOReadableWritable> inputQuery = null;


  
  public StreamTask() {
    Partitioner = null;
    UserFunction = null;
    partitioner = null;
    userFunction = null;
  }

  
  private void setClassInputs() {
    Partitioner = getTaskConfiguration().getClass("partitioner", DefaultPartitioner.class,ChannelSelector.class);
    try {
      partitioner = Partitioner.newInstance();
    } catch (Exception e) {

    }
    UserFunction = getTaskConfiguration().getClass("userfunction", TestTaskInvokable.class,UserTaskInvokable.class);
    try
    {
      userFunction = UserFunction.newInstance();
    } catch (Exception e)
    {
      
    }
  
  }

  @Override
  public void registerInputOutput() {
    setClassInputs();
    this.inputInfo = new RecordReader<IOReadableWritable>(this, IOReadableWritable.class);
    this.inputQuery = new RecordReader<IOReadableWritable>(this, IOReadableWritable.class);
    output = new RecordWriter<IOReadableWritable>(this, IOReadableWritable.class, this.partitioner);
    
  }

  @Override
  public void invoke() throws Exception {
    List< RecordReader<IOReadableWritable>> inputs = new ArrayList< RecordReader<IOReadableWritable>>();
    inputs.add(inputInfo);
    inputs.add(inputQuery);

    userFunction.invoke(inputs,output);

  }

}
