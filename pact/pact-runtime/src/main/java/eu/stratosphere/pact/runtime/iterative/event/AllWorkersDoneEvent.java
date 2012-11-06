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

/** signals that all iteration heads are done with one particular superstep */
//TODO merge serialization code with WorkerDoneEvent
public class AllWorkersDoneEvent extends AbstractTaskEvent {

  private Value aggregate;

  public AllWorkersDoneEvent() {}

  public AllWorkersDoneEvent(Value aggregate) {
    this.aggregate = aggregate;
  }

  public Value aggregate() {
    return aggregate;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    boolean hasAggregate = aggregate != null;
    out.writeBoolean(hasAggregate);
    if (hasAggregate) {
      out.writeUTF(aggregate.getClass().getName());
      aggregate.write(out);
    }
  }

  @Override
  public void read(DataInput in) throws IOException {
    boolean hasAggregate = in.readBoolean();
    if (hasAggregate) {
      String classname = in.readUTF();
      try {
        aggregate = Class.forName(classname).asSubclass(Value.class).newInstance();
      } catch (Exception e) {
        throw new IOException(e);
      }
      aggregate.read(in);
    } else {
      aggregate = null;
    }
  }
}
