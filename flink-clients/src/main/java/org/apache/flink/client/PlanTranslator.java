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
 *
 */

package org.apache.flink.client;

import org.apache.flink.api.common.Plan;
import org.apache.flink.api.dag.Pipeline;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.optimizer.DataStatistics;
import org.apache.flink.optimizer.Optimizer;
import org.apache.flink.optimizer.costs.DefaultCostEstimator;
import org.apache.flink.optimizer.plan.OptimizedPlan;
import org.apache.flink.optimizer.plandump.PlanJSONDumpGenerator;
import org.apache.flink.optimizer.plantranslate.JobGraphGenerator;
import org.apache.flink.runtime.jobgraph.JobGraph;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.flink.util.Preconditions.checkArgument;

/** {@link FlinkPipelineTranslator} for DataSet API {@link Plan Plans}. */
public class PlanTranslator implements FlinkPipelineTranslator {

    private static final Logger LOG = LoggerFactory.getLogger(PlanTranslator.class);

    @Override
    public JobGraph translateToJobGraph(
            Pipeline pipeline, Configuration optimizerConfiguration, int defaultParallelism) {
        checkArgument(pipeline instanceof Plan, "Given pipeline is not a DataSet Plan.");

        Plan plan = (Plan) pipeline;
        setDefaultParallelism(plan, defaultParallelism);
        return compilePlan(plan, optimizerConfiguration);
    }

    private void setDefaultParallelism(Plan plan, int defaultParallelism) {
        if (defaultParallelism > 0 && plan.getDefaultParallelism() <= 0) {
            LOG.debug(
                    "Changing plan default parallelism from {} to {}",
                    plan.getDefaultParallelism(),
                    defaultParallelism);
            plan.setDefaultParallelism(defaultParallelism);
        }

        LOG.debug(
                "Set parallelism {}, plan default parallelism {}",
                defaultParallelism,
                plan.getDefaultParallelism());
    }

    @Override
    public String translateToJSONExecutionPlan(Pipeline pipeline) {
        checkArgument(pipeline instanceof Plan, "Given pipeline is not a DataSet Plan.");

        Plan plan = (Plan) pipeline;

        Optimizer opt =
                new Optimizer(
                        new DataStatistics(), new DefaultCostEstimator(), new Configuration());
        OptimizedPlan optPlan = opt.compile(plan);

        return new PlanJSONDumpGenerator().getOptimizerPlanAsJSON(optPlan);
    }

    private JobGraph compilePlan(Plan plan, Configuration optimizerConfiguration) {
        Optimizer optimizer = new Optimizer(new DataStatistics(), optimizerConfiguration);
        OptimizedPlan optimizedPlan = optimizer.compile(plan);

        JobGraphGenerator jobGraphGenerator = new JobGraphGenerator(optimizerConfiguration);
        return jobGraphGenerator.compileJobGraph(optimizedPlan, plan.getJobId());
    }

    @Override
    public boolean canTranslate(Pipeline pipeline) {
        return pipeline instanceof Plan;
    }
}
