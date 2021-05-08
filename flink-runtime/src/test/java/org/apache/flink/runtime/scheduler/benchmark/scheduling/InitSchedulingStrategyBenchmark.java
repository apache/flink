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
 * limitations under the License
 */

package org.apache.flink.runtime.scheduler.benchmark.scheduling;

import org.apache.flink.runtime.scheduler.benchmark.JobConfiguration;
import org.apache.flink.runtime.scheduler.strategy.PipelinedRegionSchedulingStrategy;

/**
 * The benchmark of initializing {@link PipelinedRegionSchedulingStrategy} in a STREAMING/BATCH job.
 */
public class InitSchedulingStrategyBenchmark extends SchedulingBenchmarkBase {

    public InitSchedulingStrategyBenchmark() {}

    public void setup(JobConfiguration jobConfiguration) throws Exception {
        initSchedulingTopology(jobConfiguration);
    }

    public PipelinedRegionSchedulingStrategy initSchedulingStrategy() {
        return new PipelinedRegionSchedulingStrategy(schedulerOperations, schedulingTopology);
    }
}
