/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
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

package eu.stratosphere.pact.runtime.iterative;

import eu.stratosphere.nephele.jobgraph.JobID;

/** simple identifier class for subtasks */
public class Identifier {

  private final String id;

  public Identifier(JobID jobID, int subTaskID) {
    id = jobID.toString() + "#" + subTaskID;
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof Identifier) {
      return id.equals(((Identifier) o).id);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return id.hashCode();
  }

  @Override
  public String toString() {
    return id;
  }
}
