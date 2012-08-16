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

/** A workset iteration is by definition converged if no records have been inserted into the workset */
public class WorksetEmptyConvergenceCriterion<T> implements ConvergenceCriterion<T> {

  private boolean worksetEmpty;

  @Override
  public void prepareForNextIteration() {
    worksetEmpty = true;
  }

  @Override
  public void analyze(T record) {
    worksetEmpty = false;
  }

  @Override
  public boolean isConverged() {
    return worksetEmpty;
  }
}
