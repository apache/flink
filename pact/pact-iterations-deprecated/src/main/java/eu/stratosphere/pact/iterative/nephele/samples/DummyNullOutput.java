package eu.stratosphere.pact.iterative.nephele.samples;

import eu.stratosphere.nephele.io.RecordReader;
import eu.stratosphere.nephele.template.AbstractOutputTask;
import eu.stratosphere.nephele.types.StringRecord;


public class DummyNullOutput extends AbstractOutputTask {

  @Override
  public void registerInputOutput() {
     new RecordReader<StringRecord>(this, StringRecord.class);
  }

  @Override
  public void invoke() throws Exception {}

}
