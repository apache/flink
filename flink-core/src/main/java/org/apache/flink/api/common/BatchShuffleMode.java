/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.api.common;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.DescribedEnum;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.configuration.description.InlineElement;

import static org.apache.flink.configuration.description.TextElement.text;

/**
 * Defines how data is exchanged between tasks in batch {@link ExecutionOptions#RUNTIME_MODE} if the
 * shuffling behavior has not been set explicitly for an individual exchange.
 *
 * <p>With pipelined exchanges, upstream and downstream tasks run simultaneously. In order to
 * achieve lower latency, a result record is immediately sent to and processed by the downstream
 * task. Thus, the receiver back-pressures the sender. The streaming mode always uses this exchange.
 *
 * <p>With blocking exchanges, upstream and downstream tasks run in stages. Records are persisted to
 * some storage between stages. Downstream tasks then fetch these records after the upstream tasks
 * finished. Such an exchange reduces the resources required to execute the job as it does not need
 * to run upstream and downstream tasks simultaneously.
 */
@PublicEvolving
public enum BatchShuffleMode implements DescribedEnum {

    /**
     * Upstream and downstream tasks run simultaneously.
     *
     * <p>This leads to lower latency and more evenly distributed (but higher) resource usage across
     * tasks.
     */
    ALL_EXCHANGES_PIPELINED(
            text(
                    "Upstream and downstream tasks run simultaneously. This leads to lower latency "
                            + "and more evenly distributed (but higher) resource usage across tasks.")),

    /**
     * Upstream and downstream tasks run subsequently.
     *
     * <p>This reduces the resource usage as downstream tasks are started after upstream tasks
     * finished.
     */
    ALL_EXCHANGES_BLOCKING(
            text(
                    "Upstream and downstream tasks run subsequently. This reduces the resource usage "
                            + "as downstream tasks are started after upstream tasks finished."));

    private final InlineElement description;

    BatchShuffleMode(InlineElement description) {
        this.description = description;
    }

    @Override
    public InlineElement getDescription() {
        return description;
    }
}
