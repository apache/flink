/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.test.iterative.nephele.danglingpagerank;

import eu.stratosphere.core.memory.DataInputView;
import eu.stratosphere.core.memory.DataOutputView;
import eu.stratosphere.types.Value;

import java.io.IOException;

public class BooleanValue implements Value {
  private static final long serialVersionUID = 1L;

  private boolean value;

  public BooleanValue(boolean value) {
    this.value = value;
  }

  public BooleanValue() {
  }

  public boolean get() {
    return value;
  }

  public void set(boolean value) {
    this.value = value;
  }

  @Override
  public void write(DataOutputView out) throws IOException {
    out.writeBoolean(value);
  }

  @Override
  public void read(DataInputView in) throws IOException {
    value = in.readBoolean();
  }
}
