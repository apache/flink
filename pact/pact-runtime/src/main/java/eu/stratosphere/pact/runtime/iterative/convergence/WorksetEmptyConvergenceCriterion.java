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

package eu.stratosphere.pact.runtime.iterative.convergence;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/** A workset iteration is by definition converged if no records have been inserted into the workset */
public class WorksetEmptyConvergenceCriterion implements ConvergenceCriterion<Long> {

  private long updatesInSolutionset;
  private int iteration = 0;

  private static final Log log = LogFactory.getLog(WorksetEmptyConvergenceCriterion.class);

  @Override
  public void prepareForNextIteration() {
    updatesInSolutionset = 0;
    iteration++;
  }

  @Override
  public void analyze(int workerIndex, Long updates) {

    if (log.isInfoEnabled()) {
      log.info("solutionset of worker [" + workerIndex + "] had [" + updates + "] updates in iteration " +
          "[" + iteration + "]");
    }

    this.updatesInSolutionset += updates.longValue();
  }

  @Override
  public boolean isConverged() {

    if (log.isInfoEnabled()) {
      log.info("[" + updatesInSolutionset + "] solutionset updates in iteration [" + iteration + "]");
    }

    return updatesInSolutionset == 0;
  }
}
