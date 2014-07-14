package eu.stratosphere.streaming.test;

import java.util.HashMap;
import java.util.Map;

import eu.stratosphere.streaming.api.invokable.UserSinkInvokable;
import eu.stratosphere.types.Record;
import eu.stratosphere.types.StringValue;

public class WordCountSink implements UserSinkInvokable {

  Map<String, Integer> wordCounts = new HashMap<String,Integer>();
  
  @Override
  public void invoke(Record record) throws Exception {
    StringValue word = new StringValue("");
    record.getFieldInto(0, word);
    
    if(wordCounts.containsKey(word.getValue())) {
      wordCounts.put(word.getValue(), wordCounts.get(word.getValue())+1);
      System.out.println(word.getValue()+" "+wordCounts.get(word.getValue()));
    } else {
      wordCounts.put(word.getValue(), 1);
      System.out.println(word.getValue()+" "+wordCounts.get(word.getValue()));
      
    }
  }

}