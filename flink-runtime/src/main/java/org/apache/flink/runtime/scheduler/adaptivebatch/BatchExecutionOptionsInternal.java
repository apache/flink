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

package org.apache.flink.runtime.scheduler.adaptivebatch;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.MemorySize;

import static org.apache.flink.configuration.ConfigOptions.key;

/** Internal configuration options for the batch job execution. */
@Internal
public class BatchExecutionOptionsInternal {
    public static final ConfigOption<MemorySize> ADAPTIVE_SKEWED_OPTIMIZATION_SKEWED_THRESHOLD =
            key("$internal.execution.batch.adaptive.skewed-optimization.skewed-threshold")
                    .memoryType()
                    .defaultValue(MemorySize.ofMebiBytes(256))
                    .withDescription(
                            "Flink will automatically reduce the ratio of the maximum to median concurrent task "
                                    + "processing data volume to below the skewed-factor and will also achieve "
                                    + "a more balanced data distribution, unless the maximum value is below the "
                                    + "skewed-threshold.");

    public static final ConfigOption<Double> ADAPTIVE_SKEWED_OPTIMIZATION_SKEWED_FACTOR =
            key("$internal.execution.batch.adaptive.skewed-optimization.skewed-factor")
                    .doubleType()
                    .defaultValue(4.0)
                    .withDescription(
                            "When the maximum data volume processed by a concurrent task is greater than the "
                                    + "skewed-threshold, Flink can automatically reduce the ratio of the maximum "
                                    + "data volume processed by a concurrent task to the median to less than the "
                                    + "skewed-factor and will also achieve a more balanced data distribution.");
}
