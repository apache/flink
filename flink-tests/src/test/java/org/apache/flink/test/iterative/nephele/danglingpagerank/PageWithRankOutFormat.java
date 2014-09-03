/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package org.apache.flink.test.iterative.nephele.danglingpagerank;

import com.google.common.base.Charsets;

import java.io.IOException;

import org.apache.flink.api.java.record.io.FileOutputFormat;
import org.apache.flink.types.DoubleValue;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.Record;

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
