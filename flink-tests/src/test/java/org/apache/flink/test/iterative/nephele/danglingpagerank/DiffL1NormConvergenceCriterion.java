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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flink.api.common.aggregators.ConvergenceCriterion;

@SuppressWarnings("serial")
public class DiffL1NormConvergenceCriterion implements ConvergenceCriterion<PageRankStats> {

	private static final double EPSILON = 0.00005;

	private static final Logger log = LoggerFactory.getLogger(DiffL1NormConvergenceCriterion.class);

	@Override
	public boolean isConverged(int iteration, PageRankStats pageRankStats) {
		double diff = pageRankStats.diff();

		if (log.isInfoEnabled()) {
			log.info("Stats in iteration [" + iteration + "]: " + pageRankStats);
			log.info("L1 norm of the vector difference is [" + diff + "] in iteration [" + iteration + "]");
		}

		return diff < EPSILON;
	}
}
