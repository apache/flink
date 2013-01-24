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

package eu.stratosphere.pact.runtime.iterative.playing.iterativemapreduce;

import com.google.common.base.Charsets;
import eu.stratosphere.pact.common.io.FileOutputFormat;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactString;

import java.io.IOException;

public class AppendTokenOutFormat extends FileOutputFormat {

  private final StringBuilder buffer = new StringBuilder();

  @Override
  public void writeRecord(PactRecord record) throws IOException {
    buffer.setLength(0);
    buffer.append(record.getField(0, PactInteger.class).getValue());
    buffer.append('\t');
    buffer.append(record.getField(1, PactInteger.class).getValue());
    buffer.append('\n');

    byte[] bytes = buffer.toString().getBytes(Charsets.UTF_8);
    stream.write(bytes);
  }
}
