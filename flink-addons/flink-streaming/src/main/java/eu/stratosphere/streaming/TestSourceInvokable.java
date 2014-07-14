package eu.stratosphere.streaming;


import eu.stratosphere.core.io.IOReadableWritable;
import eu.stratosphere.core.io.StringRecord;
import eu.stratosphere.nephele.io.RecordWriter;

public class TestSourceInvokable implements UserSourceInvokable {

  @Override
  public void invoke(RecordWriter<IOReadableWritable> output) throws Exception {
    for (int i = 0; i < 10; i++) {
      // output.emit(new StringRecord(rnd.nextInt(10)+" "+rnd.nextInt(1000)));
      output.emit(new StringRecord("5 500"));
      output.emit(new StringRecord("4 500"));

    }
  }

}
