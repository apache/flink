package eu.stratosphere.streaming.test;

import eu.stratosphere.streaming.api.invokable.UserSourceInvokable;
import eu.stratosphere.types.Record;
import eu.stratosphere.types.StringValue;

public class WordCountSource extends UserSourceInvokable {

  private String motto = "Stratosphere Big Data looks tiny from here";

  @Override
  public void invoke() throws Exception {
    for (int i=0;i<10;i++) {
      emit(new Record(new StringValue(motto)));
    }
  }

}