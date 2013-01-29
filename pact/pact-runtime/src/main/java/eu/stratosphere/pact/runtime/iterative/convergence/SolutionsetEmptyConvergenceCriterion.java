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

import eu.stratosphere.pact.common.type.base.PactLong;
import eu.stratosphere.pact.runtime.iterative.aggregate.Aggregator;
import eu.stratosphere.pact.runtime.iterative.aggregate.SumLongAggregator;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/** A workset iteration is by definition converged if no records have been updated in the solutionset */
public class SolutionsetEmptyConvergenceCriterion implements ConvergenceCriterion<PactLong> {

	private static final Log log = LogFactory.getLog(SolutionsetEmptyConvergenceCriterion.class);

	@Override
	public Aggregator<PactLong> createAggregator() {
		return new SumLongAggregator();
	}

	@Override
	public boolean isConverged(int iteration, PactLong value) {

		long updatedElements = value.getValue();

		if (log.isInfoEnabled()) {
			log.info("[" + updatedElements + "] elements updated in the solutionset in iteration [" + iteration + "]");
		}

		return updatedElements == 0;
	}

}
