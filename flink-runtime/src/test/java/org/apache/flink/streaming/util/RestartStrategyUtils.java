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

package org.apache.flink.streaming.util;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

/** Utility class for configuring restart strategies. */
public class RestartStrategyUtils {

    /**
     * Disables the restart strategy for the given StreamExecutionEnvironment.
     *
     * @param env the StreamExecutionEnvironment to configure
     */
    public static void configureNoRestartStrategy(StreamExecutionEnvironment env) {
        env.configure(new Configuration().set(RestartStrategyOptions.RESTART_STRATEGY, "none"));
    }

    /**
     * Disables the restart strategy for the given JobGraph.
     *
     * @param jobGraph the JobGraph to configure
     */
    public static void configureNoRestartStrategy(JobGraph jobGraph) {
        jobGraph.getJobConfiguration().set(RestartStrategyOptions.RESTART_STRATEGY, "none");
    }

    /**
     * Sets a fixed-delay restart strategy for the given StreamExecutionEnvironment.
     *
     * @param env the StreamExecutionEnvironment to configure
     * @param restartAttempts the number of restart attempts
     * @param delayBetweenAttempts the delay between restart attempts in milliseconds
     */
    public static void configureFixedDelayRestartStrategy(
            StreamExecutionEnvironment env, int restartAttempts, long delayBetweenAttempts) {
        configureFixedDelayRestartStrategy(
                env, restartAttempts, Duration.ofMillis(delayBetweenAttempts));
    }

    /**
     * Sets a fixed-delay restart strategy for the given StreamExecutionEnvironment.
     *
     * @param env the StreamExecutionEnvironment to configure
     * @param restartAttempts the number of restart attempts
     * @param delayBetweenAttempts the delay between restart attempts
     */
    public static void configureFixedDelayRestartStrategy(
            StreamExecutionEnvironment env, int restartAttempts, Duration delayBetweenAttempts) {
        Configuration configuration = new Configuration();
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");
        configuration.set(
                RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, restartAttempts);
        configuration.set(
                RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY, delayBetweenAttempts);

        env.configure(configuration);
    }

    /**
     * Sets a fixed-delay restart strategy for the given JobGraph.
     *
     * @param jobGraph the JobGraph to configure
     * @param restartAttempts the number of restart attempts
     * @param delayBetweenAttempts the delay between restart attempts in milliseconds
     */
    public static void configureFixedDelayRestartStrategy(
            JobGraph jobGraph, int restartAttempts, long delayBetweenAttempts) {
        configureFixedDelayRestartStrategy(
                jobGraph, restartAttempts, Duration.ofMillis(delayBetweenAttempts));
    }

    /**
     * Sets a fixed-delay restart strategy for the given JobGraph.
     *
     * @param jobGraph the JobGraph to configure
     * @param restartAttempts the number of restart attempts
     * @param delayBetweenAttempts the delay between restart attempts
     */
    public static void configureFixedDelayRestartStrategy(
            JobGraph jobGraph, int restartAttempts, Duration delayBetweenAttempts) {
        Configuration configuration = jobGraph.getJobConfiguration();
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");
        configuration.set(
                RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, restartAttempts);
        configuration.set(
                RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY, delayBetweenAttempts);
    }
}
