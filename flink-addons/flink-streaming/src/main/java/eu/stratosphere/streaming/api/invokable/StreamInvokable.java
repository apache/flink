package eu.stratosphere.streaming.api.invokable;

import java.util.List;

import eu.stratosphere.nephele.io.RecordWriter;
import eu.stratosphere.streaming.api.StreamRecordProvider;
import eu.stratosphere.types.Record;

public abstract class StreamInvokable {

  private List<RecordWriter<Record>> outputs;

  public final void declareOutputs(List<RecordWriter<Record>> outputs) {
    this.outputs = outputs;
  }

  public final void emit(Record record) {
    for (RecordWriter<Record> output : outputs) {
      try {
        output.emit(StreamRecordProvider.addUUID(record));
      } catch (Exception e) {
        System.out.println("Emit error");
      }
    }
  }
}