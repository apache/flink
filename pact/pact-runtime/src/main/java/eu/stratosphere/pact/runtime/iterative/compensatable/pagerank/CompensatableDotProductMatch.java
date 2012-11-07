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

package eu.stratosphere.pact.runtime.iterative.compensatable.pagerank;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MatchStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactLong;
import eu.stratosphere.pact.runtime.iterative.compensatable.ConfigUtils;
import eu.stratosphere.pact.runtime.iterative.concurrent.IterationContext;

import java.util.Random;

public class CompensatableDotProductMatch extends MatchStub {

  private PactRecord record;

  private int workerIndex;
  private int currentIteration;
  private int failingIteration;

  private int failingWorker;
  private double messageLoss;

  private Random random;

  @Override
  public void open(Configuration parameters) throws Exception {
    record = new PactRecord();

    workerIndex = ConfigUtils.asInteger("pact.parallel.task.id", parameters);
    currentIteration = ConfigUtils.asInteger("pact.iterations.currentIteration", parameters);
    failingIteration = ConfigUtils.asInteger("compensation.failingIteration", parameters);
    failingWorker = ConfigUtils.asInteger("compensation.failingWorker", parameters);
    messageLoss = ConfigUtils.asDouble("compensation.messageLoss", parameters);

    random = new Random();
  }

  @Override
  public void match(PactRecord pageWithRank, PactRecord transitionMatrixEntry, Collector<PactRecord> collector)
      throws Exception {

    double rank = pageWithRank.getField(1, PactDouble.class).getValue();
    double transitionProbability = transitionMatrixEntry.getField(2, PactDouble.class).getValue();

    record.setField(0, transitionMatrixEntry.getField(1, PactLong.class));
    record.setField(1, new PactDouble(rank * transitionProbability));

    if (currentIteration == failingIteration && workerIndex == failingWorker) {
      if (random.nextDouble() >= messageLoss) {
        collector.collect(record);
      }
    } else {
      collector.collect(record);
    }
  }

}
