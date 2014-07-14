package eu.stratosphere.streaming.test;

import eu.stratosphere.streaming.api.invokable.UserTaskInvokable;
import eu.stratosphere.types.Record;
import eu.stratosphere.types.StringValue;

public class WordCountTask extends UserTaskInvokable {

  @Override
  public void invoke(Record record) throws Exception {
    StringValue sentence = new StringValue("");
    record.getFieldInto(0, sentence);
    String[] words = sentence.getValue().split(" ");
    for (CharSequence word : words) {
      emit(new Record(new StringValue(word)));
    }
  }
}