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

/** Used to check for convergence */
public interface ConvergenceCriterion<T> {

  /** Prepare for the start of an iteration */
  void prepareForNextIteration();

  /** Called for each aggregate sent to the synchronization task */
  void analyze(int workerIndex, T aggregate);

  /** Decide whether the iterative algorithm has converged */
  boolean isConverged();
}
