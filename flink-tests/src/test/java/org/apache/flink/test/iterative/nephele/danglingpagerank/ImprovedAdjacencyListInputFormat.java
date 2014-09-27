/*
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

import org.apache.flink.api.java.record.io.DelimitedInputFormat;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.Record;

public class ImprovedAdjacencyListInputFormat extends DelimitedInputFormat {
  private static final long serialVersionUID = 1L;

  private final LongValue vertexID = new LongValue();
  private final AsciiLongArrayView arrayView = new AsciiLongArrayView();
  private final LongArrayView adjacentVertices = new LongArrayView();

  @Override
  public Record readRecord(Record target, byte[] bytes, int offset, int numBytes) {

    if (numBytes == 0) {
      return null;
    }

    arrayView.set(bytes, offset, numBytes);

    int numElements = arrayView.numElements();
    adjacentVertices.allocate(numElements - 1);

    try {

      int pos = 0;
      while (arrayView.next()) {

        if (pos == 0) {
          vertexID.setValue(arrayView.element());
        } else {
          adjacentVertices.setQuick(pos - 1, arrayView.element());
        }

        pos++;
      }

      //sanity check
      if (pos != numElements) {
        throw new IllegalStateException("Should have gotten " + numElements + " elements, but saw " + pos);
      }

    } catch (RuntimeException e) {
      throw new RuntimeException("Error parsing: " + arrayView.toString(), e);
    }

    target.clear();
    target.addField(vertexID);
    target.addField(adjacentVertices);
    return target;
  }
}

