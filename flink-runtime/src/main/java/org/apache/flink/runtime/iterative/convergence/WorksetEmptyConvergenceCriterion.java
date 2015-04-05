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


package org.apache.flink.runtime.iterative.convergence;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flink.api.common.accumulators.ConvergenceCriterion;

/**
 * A workset iteration is by definition converged if no records have been updated in the solutionset
 */
public class WorksetEmptyConvergenceCriterion implements ConvergenceCriterion<Long> {

	private static final long serialVersionUID = 1L;

	private static final Logger log = LoggerFactory.getLogger(WorksetEmptyConvergenceCriterion.class);
	
	public static final String ACCUMULATOR_NAME = "pact.runtime.workset-empty-accumulator";

	@Override
	public boolean isConverged(int iteration, Long value) {

		long updatedElements = value.longValue();

		if (log.isInfoEnabled()) {
			log.info("[" + updatedElements + "] elements updated in the solutionset in iteration [" + iteration + "]");
		}

		return updatedElements == 0;
	}
}
