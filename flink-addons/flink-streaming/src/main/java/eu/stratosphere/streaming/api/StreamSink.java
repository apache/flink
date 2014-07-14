/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2014 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.streaming.api;

import java.util.LinkedList;
import java.util.List;

import eu.stratosphere.nephele.io.RecordReader;
import eu.stratosphere.nephele.template.AbstractOutputTask;
import eu.stratosphere.streaming.api.invokable.DefaultSinkInvokable;
import eu.stratosphere.streaming.api.invokable.UserSinkInvokable;
import eu.stratosphere.types.Record;

public class StreamSink extends AbstractOutputTask {

  private List<RecordReader<Record>> inputs;
  private UserSinkInvokable userFunction;

  private int numberOfInputs;

  public StreamSink() {
    // TODO: Make configuration file visible and call setClassInputs() here
    inputs = new LinkedList<RecordReader<Record>>();
    userFunction = null;
    numberOfInputs = 0;
  }

  private void setConfigInputs() {

    numberOfInputs = getTaskConfiguration().getInteger("numberOfInputs", 0);
    for (int i = 0; i < numberOfInputs; i++) {
      inputs.add(new RecordReader<Record>(this, Record.class));
    }

    Class<? extends UserSinkInvokable> userFunctionClass;
    userFunctionClass = getTaskConfiguration().getClass("userfunction",
        DefaultSinkInvokable.class, UserSinkInvokable.class);
    try {
      userFunction = userFunctionClass.newInstance();
    } catch (Exception e) {

    }
  }

  @Override
  public void registerInputOutput() {
    setConfigInputs();
  }

  @Override
  public void invoke() throws Exception {
    boolean hasInput = true;
    while (hasInput) {
      hasInput = false;
      for (RecordReader<Record> input : inputs) {
        if (input.hasNext()) {
          hasInput = true;
          Record rec = input.next();
          rec.removeField(rec.getNumFields()-1);
          userFunction.invoke(rec);
        }
      }
    }
  }

}
