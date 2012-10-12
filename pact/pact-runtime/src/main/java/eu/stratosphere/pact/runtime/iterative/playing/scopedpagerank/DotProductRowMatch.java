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

package eu.stratosphere.pact.runtime.iterative.playing.scopedpagerank;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MatchStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactLong;

public class DotProductRowMatch extends MatchStub {

  private PactRecord record;
  private PactLong vertexID;
  private PactDouble partialRank;

  @Override
  public void open(Configuration parameters) throws Exception {
    record = new PactRecord();
    vertexID = new PactLong();
    partialRank = new PactDouble();
  }

  @Override
  public void match(PactRecord pageWithRank, PactRecord transitionMatrixRow, Collector<PactRecord> collector)
      throws Exception {

//    System.out.println("fields ###### " + pageWithRank.getNumFields() + " " + transitionMatrixEntry.getNumFields());
//    System.out.println("field0 ###### " + pageWithRank.getField(0, PactLong.class).getValue() + " " + transitionMatrixEntry.getField(0, PactLong.class).getValue());
//    System.out.println("field1 ###### " + pageWithRank.getField(1, PactDouble.class).getValue() + " " + transitionMatrixEntry.getField(1, PactDouble.class).getValue());

//    long source = transitionMatrixEntry.getField(0, PactLong.class).getValue();
//    long target = transitionMatrixEntry.getField(1, PactLong.class).getValue();
//    long vertexID = pageWithRank.getField(0, PactLong.class).getValue();

    double rank = pageWithRank.getField(1, PactDouble.class).getValue();

    SequentialAccessSparseRowVector row = transitionMatrixRow.getField(1, SequentialAccessSparseRowVector.class);
    long[] indexes = row.indexes();
    double[] values = row.values();

    for (int n = 0; n < indexes.length; n++) {
      vertexID.setValue(indexes[n]);
      partialRank.setValue(rank * values[n]);

      record.setField(0, vertexID);
      record.setField(1, partialRank);
      collector.collect(record);
    }

//    System.out.println("Joining (" + vertexID + "," + rank + ") with (" + source + "," + target + "," + transitionProbability + ")");
//    System.out.println(">>>>>>>>>>>> Emitting: " + target + "," + (rank * transitionProbability));
  }

  @Override
  public void close() throws Exception {
    record = null;
  }
}
