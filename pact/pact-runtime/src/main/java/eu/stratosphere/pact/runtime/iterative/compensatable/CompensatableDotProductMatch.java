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

package eu.stratosphere.pact.runtime.iterative.compensatable;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MatchStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactLong;
import eu.stratosphere.pact.runtime.iterative.concurrent.IterationContext;

import java.util.Random;

public class CompensatableDotProductMatch extends MatchStub {

  private PactRecord record;

  private int workerIndex;
  private int currentIteration;
  private Random random;

  private int failingIteration;
  private int failingWorker;
  private double messageLoss;

  @Override
  public void open(Configuration parameters) throws Exception {
    record = new PactRecord();
    workerIndex = parameters.getInteger("pact.parallel.task.id", -1);
    if (workerIndex == -1) {
      throw new IllegalStateException("Invalid workerIndex " + workerIndex);
    }

    failingIteration = parameters.getInteger("compensation.failingIteration", -1);
    if (failingIteration == -1) {
      throw new IllegalStateException();
    }
    failingWorker = parameters.getInteger("compensation.failingWorker", -1);
    if (failingWorker == -1) {
      throw new IllegalStateException();
    }
    messageLoss = Double.parseDouble(parameters.getString("compensation.messageLoss", "-1"));
    if (messageLoss == -1) {
      throw new IllegalStateException();
    }
    currentIteration = parameters.getInteger("pact.iterations.currentIteration", -1);
    if (currentIteration == -1) {
      throw new IllegalStateException();
    }

    random = new Random();
  }

  @Override
  public void match(PactRecord pageWithRank, PactRecord transitionMatrixEntry, Collector<PactRecord> collector)
      throws Exception {

//    System.out.println("fields ###### " + pageWithRank.getNumFields() + " " + transitionMatrixEntry.getNumFields());
//    System.out.println("field0 ###### " + pageWithRank.getField(0, PactLong.class).getValue() + " " + transitionMatrixEntry.getField(0, PactLong.class).getValue());
//    System.out.println("field1 ###### " + pageWithRank.getField(1, PactDouble.class).getValue() + " " + transitionMatrixEntry.getField(1, PactDouble.class).getValue());

//    long source = transitionMatrixEntry.getField(0, PactLong.class).getValue();
//    long target = transitionMatrixEntry.getField(1, PactLong.class).getValue();
//    long vertexID = pageWithRank.getField(0, PactLong.class).getValue();

    double rank = pageWithRank.getField(1, PactDouble.class).getValue();
    double transitionProbability = transitionMatrixEntry.getField(2, PactDouble.class).getValue();


    record.setField(0, transitionMatrixEntry.getField(1, PactLong.class));
    record.setField(1, new PactDouble(rank * transitionProbability));

//    System.out.println("Joining (" + vertexID + "," + rank + ") with (" + source + "," + target + "," + transitionProbability + ")");
//    System.out.println(">>>>>>>>>>>> Emitting: " + target + "," + (rank * transitionProbability));

    if (currentIteration == failingIteration && workerIndex == failingWorker) {
      if (random.nextDouble() >= messageLoss) {
        collector.collect(record);
      }
    } else {
      collector.collect(record);
    }
  }

  @Override
  public void close() throws Exception {
    record = null;
  }
}
