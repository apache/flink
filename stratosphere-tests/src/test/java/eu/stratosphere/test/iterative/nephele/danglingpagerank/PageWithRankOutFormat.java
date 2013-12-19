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

import com.google.common.base.Charsets;

import eu.stratosphere.api.record.io.FileOutputFormat;
import eu.stratosphere.types.DoubleValue;
import eu.stratosphere.types.LongValue;
import eu.stratosphere.types.Record;

import java.io.IOException;

public class PageWithRankOutFormat extends FileOutputFormat {
  private static final long serialVersionUID = 1L;

  private final StringBuilder buffer = new StringBuilder();

  @Override
  public void writeRecord(Record record) throws IOException {
    buffer.setLength(0);
    buffer.append(record.getField(0, LongValue.class).toString());
    buffer.append('\t');
    buffer.append(record.getField(1, DoubleValue.class).toString());
    buffer.append('\n');

    byte[] bytes = buffer.toString().getBytes(Charsets.UTF_8);
    stream.write(bytes);
  }
}
