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

package eu.stratosphere.pact.runtime.iterative.event;

import eu.stratosphere.nephele.event.task.AbstractTaskEvent;
import eu.stratosphere.pact.common.type.Value;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class WorkerDoneEvent extends AbstractTaskEvent {

  private int workerIndex;
  //TODO generalize
  //private long aggregate;
  private Value aggregate;

  public WorkerDoneEvent() {}

  public WorkerDoneEvent(int workerIndex, Value aggregate) {
    this.workerIndex = workerIndex;
    this.aggregate = aggregate;
  }

  public int workerIndex() {
    return workerIndex;
  }

  public Value aggregate() {
    return aggregate;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(workerIndex);
    out.writeUTF(aggregate.getClass().getName());
    aggregate.write(out);
    //out.writeLong(aggregate);
  }

  @Override
  public void read(DataInput in) throws IOException {
    workerIndex = in.readInt();
    String classname = in.readUTF();
    try {
      aggregate = Class.forName(classname).asSubclass(Value.class).newInstance();
    } catch (Exception e) {
      throw new IOException(e);
    }
    aggregate.read(in);
    //aggregate = in.readLong();
  }
}
