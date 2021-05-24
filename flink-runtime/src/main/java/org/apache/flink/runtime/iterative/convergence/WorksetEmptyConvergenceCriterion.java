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

import org.apache.flink.api.common.aggregators.ConvergenceCriterion;
import org.apache.flink.types.LongValue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A workset iteration is by definition converged if no records have been updated in the
 * solutionset.
 */
public class WorksetEmptyConvergenceCriterion implements ConvergenceCriterion<LongValue> {

    private static final long serialVersionUID = 1L;

    private static final Logger log =
            LoggerFactory.getLogger(WorksetEmptyConvergenceCriterion.class);

    public static final String AGGREGATOR_NAME = "pact.runtime.workset-empty-aggregator";

    @Override
    public boolean isConverged(int iteration, LongValue value) {

        long updatedElements = value.getValue();

        if (log.isInfoEnabled()) {
            log.info(
                    "["
                            + updatedElements
                            + "] elements updated in the solutionset in iteration ["
                            + iteration
                            + "]");
        }

        return updatedElements == 0;
    }
}
