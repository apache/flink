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

package eu.stratosphere.streaming.test;

import eu.stratosphere.streaming.api.FlatStreamRecord;
import eu.stratosphere.streaming.api.invokable.UserTaskInvokable;
import eu.stratosphere.streaming.test.cellinfo.WorkerEngineExact;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.LongValue;
import eu.stratosphere.types.Record;
import eu.stratosphere.types.StringValue;

public class TestTaskInvokable extends UserTaskInvokable {

  private WorkerEngineExact engine = new WorkerEngineExact(10, 1000, 0);

  @Override
  public void invoke(FlatStreamRecord record) throws Exception {
    IntValue value1 = new IntValue(0);
    record.getFieldInto(0, value1);
    LongValue value2 = new LongValue(0);
    record.getFieldInto(1, value2);

    // INFO
    if (record.getNumFields() == 2) {
      engine.put(value1.getValue(), value2.getValue());
      emit(new FlatStreamRecord(new Record(new StringValue(value1 + " " + value2))));
    }
    // QUERY
    else if (record.getNumFields() == 3) {
      LongValue value3 = new LongValue(0);
      record.getFieldInto(2, value3);
      emit(new FlatStreamRecord(new Record(new StringValue(String.valueOf(engine.get(
          value2.getValue(), value3.getValue(), value1.getValue()))))));
    }
  }
}
