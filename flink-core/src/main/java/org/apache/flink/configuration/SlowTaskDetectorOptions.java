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

package org.apache.flink.configuration;

import org.apache.flink.annotation.docs.Documentation;

import java.time.Duration;

import static org.apache.flink.configuration.ConfigOptions.key;

/** Configuration options to detect slow tasks. */
public class SlowTaskDetectorOptions {

    @Documentation.Section(Documentation.Sections.EXPERT_SCHEDULING)
    public static final ConfigOption<Duration> CHECK_INTERVAL =
            key("slow-task-detector.check-interval")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(1))
                    .withDescription("The interval to check slow tasks.");

    @Documentation.Section(Documentation.Sections.EXPERT_SCHEDULING)
    public static final ConfigOption<Duration> EXECUTION_TIME_BASELINE_LOWER_BOUND =
            key("slow-task-detector.execution-time.baseline-lower-bound")
                    .durationType()
                    .defaultValue(Duration.ofMinutes(1))
                    .withDescription("The lower bound of slow task detection baseline.");

    @Documentation.Section(Documentation.Sections.EXPERT_SCHEDULING)
    public static final ConfigOption<Double> EXECUTION_TIME_BASELINE_RATIO =
            key("slow-task-detector.execution-time.baseline-ratio")
                    .doubleType()
                    .defaultValue(0.75)
                    .withDescription(
                            "The finished execution ratio threshold to calculate the slow tasks "
                                    + "detection baseline. Given that the parallelism is N and the "
                                    + "ratio is R, define T as the median of the first N*R finished "
                                    + "tasks' execution time. The baseline will be T*M, where M is "
                                    + "the multiplier of the baseline. Note that the execution time "
                                    + "will be weighted with the task's input bytes to ensure the "
                                    + "accuracy of the detection if data skew occurs.");

    @Documentation.Section(Documentation.Sections.EXPERT_SCHEDULING)
    public static final ConfigOption<Double> EXECUTION_TIME_BASELINE_MULTIPLIER =
            key("slow-task-detector.execution-time.baseline-multiplier")
                    .doubleType()
                    .defaultValue(1.5)
                    .withDescription(
                            "The multiplier to calculate the slow tasks detection baseline. Given "
                                    + "that the parallelism is N and the ratio is R, define T as "
                                    + "the median of the first N*R finished tasks' execution time. "
                                    + "The baseline will be T*M, where M is the multiplier of the "
                                    + "baseline. Note that the execution time will be weighted with "
                                    + "the task's input bytes to ensure the accuracy of the "
                                    + "detection if data skew occurs.");

    private SlowTaskDetectorOptions() {
        throw new IllegalAccessError();
    }
}
