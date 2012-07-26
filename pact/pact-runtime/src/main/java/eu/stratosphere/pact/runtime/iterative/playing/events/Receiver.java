/***********************************************************************************************************************
 *
 * Copyright (C) 2012 by the Stratosphere project (http://stratosphere.eu)
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

package eu.stratosphere.pact.runtime.iterative.playing.events;

import eu.stratosphere.nephele.io.RecordReader;
import eu.stratosphere.nephele.io.RecordWriter;
import eu.stratosphere.nephele.template.AbstractTask;
import eu.stratosphere.nephele.types.StringRecord;

public class Receiver extends AbstractTask {

  private RecordReader<StringRecord> input = null;
  private RecordWriter<StringRecord> output = null;

  @Override
  public void registerInputOutput() {
    input = new RecordReader<StringRecord>(this, StringRecord.class);
    output = new RecordWriter<StringRecord>(this, StringRecord.class);
    input.subscribeToEvent(new SimpleListener("Receiver"), SenderEvent.class);
  }

  @Override
  public void invoke() throws Exception {

    int i = 1;
    while (input.hasNext()) {
      i++;
      StringRecord s = input.next();
      if (i % 2 == 0) {
        input.publishEvent(new ReceiverEvent());
      }
      output.emit(s);
    }

  }

}
