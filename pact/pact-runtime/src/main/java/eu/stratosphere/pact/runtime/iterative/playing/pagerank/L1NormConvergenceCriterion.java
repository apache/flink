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

package eu.stratosphere.pact.runtime.iterative.playing.pagerank;

import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.runtime.iterative.aggregate.Aggregator;
import eu.stratosphere.pact.runtime.iterative.aggregate.SumDoubleAggregator;
import eu.stratosphere.pact.runtime.iterative.convergence.ConvergenceCriterion;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class L1NormConvergenceCriterion implements ConvergenceCriterion<PactDouble> {

  private static final double EPSILON = 0.0001;

  private static final Log log = LogFactory.getLog(L1NormConvergenceCriterion.class);

  @Override
  public Aggregator<PactDouble> createAggregator() {
    return new SumDoubleAggregator();
  }

  @Override
  public boolean isConverged(int iteration, PactDouble value) {

    double diff = value.getValue();

    if (log.isInfoEnabled()) {
      log.info("L1 norm of the vector difference is [" + diff + "] in iteration [" + iteration + "]");
    }

    return diff < EPSILON;
  }
}
